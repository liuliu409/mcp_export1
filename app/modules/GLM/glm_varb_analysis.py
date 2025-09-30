import pandas as pd
import numpy as np
from fastapi import HTTPException


########################## HELPER FUNCTIONS ##########################
def generate_amount_labels(bins, unit='m', billion_threshold=1000000000):
    """
    Generate labels for value ranges based on bin values
    
    Args:
        bins: List of bin values
        unit: Unit suffix ('m' for million, 'bn' for billion)
        billion_threshold: Value to switch from millions to billions
    
    Returns:
        List of formatted labels
    """
    labels = []
    for i in range(len(bins) - 1):
        current = bins[i]
        next_val = bins[i + 1]
        
        # Convert values to millions or billions for display
        if current >= billion_threshold:
            current_display = f"{current/billion_threshold:.2f}bn"
        else:
            current_display = f"{current/1000000:.0f}m"
            
        if next_val >= billion_threshold:
            next_display = f"{next_val/billion_threshold:.2f}bn"
        else:
            next_display = f"{next_val/1000000:.0f}m"
        
        # Format label based on position
        if i == 0:
            label = f"From 0 to {next_display}"
        elif next_val == float('inf'):
            label = f"Above {current_display}"
        else:
            label = f"Above {current_display} to {next_display}"
            
        # Add zero-padding for sorting
        label = f"{i+1:02d}-{label}"
        labels.append(label)
    
    return [bins,labels]

def generate_single_unit_labels(bins, unit):
    """
    Args:
        bins: List of bin values
        unit: Unit suffix (default: 'years')
    
    Returns:
        List of formatted labels and modified bins where the upper limit is reduced by 1
    """
    labels = []
    bins_mod = [bins[0]]  # Start with the first bin edge
    
    for i in range(len(bins) - 1):
        current = int(bins[i])
        next_val = bins[i + 1]
        
        # Format label based on position
        if i == 0:
            label = f"Under {int(next_val)} {unit}"
        elif next_val == float('inf'):
            label = f"From {current} {unit} and above"
        else:
            # For age ranges, we subtract 1 from next_val for proper range display
            label = f"From {current} to {int(next_val)-1} {unit}"
        
        # Add the next bin edge to bins_mod (adjusted by -1, except for inf)
        if i < len(bins) - 2:  # Only add if not the last bin
            if bins[i+1] == float('inf'):
                bins_mod.append(float('inf'))
            else:
                bins_mod.append(int(bins[i+1]) - 1)
        
        # Add zero-padding for sorting
        label = f"{i+1:02d}-{label}"
        labels.append(label)
    
    # Manually add the last bin edge (inf) if it exists in the original bins
    if bins[-1] == float('inf') and bins_mod[-1] != float('inf'):
        bins_mod.append(float('inf'))
    
    return [bins_mod, labels]

def categorize_car(df:pd.DataFrame ,var_name: str ,bins: list ,unit: str) -> pd.DataFrame:

    df[var_name] = pd.to_numeric(df[var_name], errors='coerce')
    if var_name == 'VEHICLE_VALUE_GROUP':
        bin_mod, labels = generate_amount_labels(bins,unit="m") # hardcode for đồng or VND
    elif var_name == 'VEHICLE_AGE_GROUP':
        bin_mod, labels = generate_single_unit_labels(bins,unit)
    elif var_name == 'VEHICLE_SEATS_GROUP':
        bin_mod, labels = generate_single_unit_labels(bins,unit)

    if df.empty:
        raise HTTPException(status_code=409, detail="Lỗi thiết lập file chưa đúng.")

    if bins and labels:
        df[var_name] = pd.cut(df[var_name], bins=bin_mod, labels=labels, include_lowest=True)
        if df[var_name].isna().sum() > 0:
            df[var_name] = df[var_name].cat.add_categories('NaN').fillna('NaN')
    return df

def categorize_health(df:pd.DataFrame ,var_name: str ,bins: list ,unit: str) -> pd.DataFrame:

    df[var_name] = pd.to_numeric(df[var_name], errors='coerce')
    if var_name == 'SUM_ASSURED_GROUP':
        bin_mod, labels = generate_amount_labels(bins,unit="m") # hardcode for đồng or VND
    elif var_name == 'CERT_AGE_GROUP':
        bin_mod, labels = generate_single_unit_labels(bins,unit)
    elif var_name == 'BENEFIT_CODE_GROUP':
        bin_mod, labels = generate_single_unit_labels(bins,unit)

    if df.empty:
        raise HTTPException(status_code=409, detail="Lỗi thiết lập file chưa đúng.")

    if bins and labels:
        df[var_name] = pd.cut(df[var_name], bins=bin_mod, labels=labels, include_lowest=True)
        if df[var_name].isna().sum() > 0:
            df[var_name] = df[var_name].cat.add_categories('NaN').fillna('NaN')
    return df

def setup_analysis_params(productName: str, additional_apply: bool, additional_codes: str):
    """
    Helper function để setup các parameters cho analysis
    """
    if productName == 'CAR':
        COUNT_NAME = "POLICY_ID"
        LABEL_NAME = "NUM_POLS"
        SUM_ASSURED = "VEHICLE_VALUE"
    elif productName == 'HEALTH':
        COUNT_NAME = "CERTIFICATE_ID"
        LABEL_NAME = "NUM_POLS"
        SUM_ASSURED = "SUM_ASSURED"
    
    # Xử lý additional apply (ưu tiên additional_apply over add_clause cho compatibility)
    if additional_apply and additional_codes:
        NUM_CLAIMS = "NUM_CLAIMS_" + additional_codes
        CLAIM_PMT = "CLAIM_PMT_" + additional_codes
    else:
        NUM_CLAIMS = 'NUM_CLAIMS'
        CLAIM_PMT = 'CLAIM_PMT'
    
    return COUNT_NAME, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED

def calculate_pivot_tables(df_table: pd.DataFrame, byvar_list: list, COUNT_NAME: str, NUM_CLAIMS: str, CLAIM_PMT: str, SUM_ASSURED: str):
    """
    Helper function để tính toán pivot tables
    """
    # Loại bỏ trùng lặp cho POLICY_ID để tính đúng EXPOSURE_YEAR và EXPOSURE_PREM
    df_unique = df_table.drop_duplicates(subset=[COUNT_NAME])

    # Tính toán riêng EXPOSURE_YEAR và EXPOSURE_PREM
    exposure_pivot = df_unique.pivot_table(
        index=byvar_list,
        values=['EXPOSURE_YEAR', 'EXPOSURE_PREM', SUM_ASSURED],
        aggfunc='sum'
    )
    
    # Tính toán các cột khác (không loại bỏ trùng lặp)
    other_pivot = df_table.pivot_table(
        index=byvar_list,
        values=[COUNT_NAME, NUM_CLAIMS, CLAIM_PMT],
        aggfunc={
            COUNT_NAME: 'nunique',
            NUM_CLAIMS: 'sum',
            CLAIM_PMT: 'sum'
        }
    )
    
    # Kết hợp hai bảng lại
    df = pd.concat([other_pivot, exposure_pivot], axis=1).reset_index()

    return df

def setup_categorical_columns(df: pd.DataFrame, byvar_list: list):
    """
    Helper function để setup categorical columns
    """
    for byvar in byvar_list:
        if df[byvar].dtype.name != 'category':
            df[byvar] = df[byvar].astype('category')
    return df

def calculate_metrics(df: pd.DataFrame, COUNT_NAME: str, NUM_CLAIMS: str, CLAIM_PMT: str, SUM_ASSURED: str):
    """
    Helper function để tính các metrics
    """
    total_row = df[[COUNT_NAME, NUM_CLAIMS,'EXPOSURE_YEAR',SUM_ASSURED,'EXPOSURE_PREM',CLAIM_PMT]].sum() 
    total_row = pd.DataFrame(total_row).transpose()
    
    df['FREQUENCY'] = df[NUM_CLAIMS]/df['EXPOSURE_YEAR']
    df['SEVERITY'] = df[CLAIM_PMT]/df[NUM_CLAIMS]
    df['AVG_PREMIUM'] = df['EXPOSURE_PREM']/df['EXPOSURE_YEAR']
    df['SUM_ASSURED'] = df[SUM_ASSURED]
    df['AVG_SUM_ASSURED'] = df['SUM_ASSURED']/df['EXPOSURE_YEAR']
    df['PURE_PREMIUM'] = df['FREQUENCY'] * df['SEVERITY']
    df['LOSS_RATIO'] = df[CLAIM_PMT]/df['EXPOSURE_PREM']
    df['GWP_%'] = df['EXPOSURE_PREM']/total_row['EXPOSURE_PREM'].sum()
    
    return df

def format_final_dataframe(df: pd.DataFrame, LABEL_NAME: str, NUM_CLAIMS: str, CLAIM_PMT: str, new_order_col: list):
    """
    Helper function để format final dataframe
    """
    df[LABEL_NAME] = df[LABEL_NAME].round().astype(int)
    df['NUM_CLAIMS'] = df[NUM_CLAIMS].round().astype(int)
    df['EXPOSURE_YEAR'] = df['EXPOSURE_YEAR'].round(6)
    df['EXPOSURE_PREM'] = df['EXPOSURE_PREM'].round(6)
    df['CLAIM_PMT'] = df[CLAIM_PMT].round()
    df['FREQUENCY'] = df['FREQUENCY'].round(6)
    df['SEVERITY'] = df['SEVERITY'].round(6)
    df['AVG_PREMIUM'] = df['AVG_PREMIUM'].round(6)
    df['PURE_PREMIUM'] = df['PURE_PREMIUM'].round(6)
    df['LOSS_RATIO'] = df['LOSS_RATIO'].round(6)
    df['GWP_%'] = df['GWP_%'].round(6)

    # Fill NA values for numeric columns
    var_col = [col for col in new_order_col if not col.startswith('VAR_')]
    for col in var_col:
        df[col] = df[col].fillna(0)

    return df[new_order_col]

######################## ANALYSIS FUNCTIONS ########################
# Updated ONE WAY ANALYSIS
def OWA_func(pol_year_ind: int
             , df_table: pd.DataFrame
             , byvar: str, var_code: str, productName: str
             , additional_apply: bool
             , additional_codes: str
             , additional_descriptions: str) -> pd.DataFrame:

    df_table['CAL_YEAR'] = df_table['CAL_YEAR'].astype(int)
    if pol_year_ind != 0:
        df_table = df_table.loc[df_table['CAL_YEAR'] == pol_year_ind]

    # Setup parameters using helper function
    COUNT_NAME, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED = setup_analysis_params(
        productName, additional_apply, additional_codes
    )

    # Calculate pivot tables using helper function
    df = calculate_pivot_tables(df_table, [byvar], COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)
    
    # Setup categorical columns using helper function
    df = setup_categorical_columns(df, [byvar])
    
    # check dataframe is empty
    if df.empty:
        raise HTTPException(status_code=409, detail="Lỗi thiết lập file chưa đúng.")
    
    # Calculate metrics using helper function
    df = calculate_metrics(df, COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)

    # Setup result columns
    df['VAR_YEAR'] = pol_year_ind
    df['VAR_NAME_CODE'] = var_code + "_" + byvar + (": " + str(additional_codes) if additional_apply == True else "")
    df['VAR_NAME'] = byvar + (": " + str(additional_descriptions) if additional_apply == True else "")
    df['VAR_DETAIL'] = df[byvar].astype(str) + (": " + str(additional_descriptions) if additional_apply == True else "")

    df = df.drop(columns=[byvar])
    df.rename(columns={COUNT_NAME:LABEL_NAME},inplace=True)
    
    new_order_col = ['VAR_YEAR','VAR_NAME_CODE','VAR_NAME','VAR_DETAIL',LABEL_NAME,'NUM_CLAIMS','EXPOSURE_YEAR','EXPOSURE_PREM',
                'CLAIM_PMT','FREQUENCY','SEVERITY','AVG_PREMIUM','PURE_PREMIUM','LOSS_RATIO','GWP_%','SUM_ASSURED','AVG_SUM_ASSURED']

    # Format final dataframe using helper function
    return format_final_dataframe(df, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, new_order_col)

# Updated TWO WAY ANALYSIS
def TWA_func(pol_year_ind: int, df_table: pd.DataFrame, byvar1: str, byvar2: str, 
             var_code: str, productName: str, 
             additional_apply: bool = False, additional_codes: str = "", 
             additional_descriptions: str = "") -> pd.DataFrame:

    df_table['CAL_YEAR'] = df_table['CAL_YEAR'].astype(int)
    if pol_year_ind != 0:
        df_table = df_table.loc[df_table['CAL_YEAR'] == pol_year_ind]

    # Setup parameters
    COUNT_NAME, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED = setup_analysis_params(
        productName, additional_apply, additional_codes
    )

    # Calculate pivot tables
    df = calculate_pivot_tables(df_table, [byvar1, byvar2], COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)
    
    # Setup categorical columns
    df = setup_categorical_columns(df, [byvar1, byvar2])

    if df.empty:
        raise HTTPException(status_code=409, detail="Lỗi thiết lập file chưa đúng.")

    # Calculate metrics
    df = calculate_metrics(df, COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)

    # Setup result columns
    df['VAR_YEAR'] = pol_year_ind
    df['VAR_NAME_CODE'] = var_code + ": " + byvar1 + " & " + byvar2 + (": " + str(additional_codes) if additional_apply else "")
    df['VAR_NAME1'] = byvar1 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_NAME2'] = byvar2 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL1'] = df[byvar1].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL2'] = df[byvar2].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")

    df = df.drop(columns=[byvar1, byvar2])
    df.rename(columns={COUNT_NAME: LABEL_NAME}, inplace=True)
    
    new_order_col = ['VAR_YEAR','VAR_NAME_CODE','VAR_NAME1','VAR_NAME2','VAR_DETAIL1','VAR_DETAIL2',
                     LABEL_NAME,'NUM_CLAIMS','EXPOSURE_YEAR','EXPOSURE_PREM',
                     'CLAIM_PMT','FREQUENCY','SEVERITY','AVG_PREMIUM','PURE_PREMIUM','LOSS_RATIO','GWP_%', 'SUM_ASSURED','AVG_SUM_ASSURED']

    return format_final_dataframe(df, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, new_order_col)

# Updated THREE WAY ANALYSIS
def threeway_func(pol_year_ind: int, df_table: pd.DataFrame, byvar1: str, byvar2: str, byvar3: str,
                  var_code: str, productName: str, 
                  additional_apply: bool = False, additional_codes: str = "", 
                  additional_descriptions: str = "") -> pd.DataFrame:

    df_table['CAL_YEAR'] = df_table['CAL_YEAR'].astype(int)
    if pol_year_ind != 0:
        df_table = df_table.loc[df_table['CAL_YEAR'] == pol_year_ind]

    # Setup parameters
    COUNT_NAME, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED = setup_analysis_params(
        productName, additional_apply, additional_codes
    )

    # Calculate pivot tables
    df = calculate_pivot_tables(df_table, [byvar1, byvar2, byvar3], COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)
    
    # Setup categorical columns
    df = setup_categorical_columns(df, [byvar1, byvar2, byvar3])

    if df.empty:
        raise HTTPException(status_code=409, detail="Lỗi thiết lập file chưa đúng.")

    # Calculate metrics
    df = calculate_metrics(df, COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)

    # Setup result columns
    df['VAR_YEAR'] = pol_year_ind
    df['VAR_NAME_CODE'] = var_code + ": " + byvar1 + " & " + byvar2 + " & " + byvar3 + (": " + str(additional_codes) if additional_apply else "")
    df['VAR_NAME1'] = byvar1 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_NAME2'] = byvar2 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_NAME3'] = byvar3 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL1'] = df[byvar1].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL2'] = df[byvar2].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL3'] = df[byvar3].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")

    df = df.drop(columns=[byvar1, byvar2, byvar3])
    df.rename(columns={COUNT_NAME: LABEL_NAME}, inplace=True)
    
    new_order_col = ['VAR_YEAR','VAR_NAME_CODE','VAR_NAME1','VAR_NAME2','VAR_NAME3',
                     'VAR_DETAIL1','VAR_DETAIL2','VAR_DETAIL3',
                     LABEL_NAME,'NUM_CLAIMS','EXPOSURE_YEAR','EXPOSURE_PREM',
                     'CLAIM_PMT','FREQUENCY','SEVERITY','AVG_PREMIUM','PURE_PREMIUM','LOSS_RATIO','GWP_%', 'SUM_ASSURED','AVG_SUM_ASSURED']

    return format_final_dataframe(df, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, new_order_col)

# Updated FOUR WAY ANALYSIS
def fourway_func(pol_year_ind: int, df_table: pd.DataFrame, byvar1: str, byvar2: str, byvar3: str, byvar4: str,
                 var_code: str, productName: str, 
                 additional_apply: bool = False, additional_codes: str = "", 
                 additional_descriptions: str = "") -> pd.DataFrame:

    df_table['CAL_YEAR'] = df_table['CAL_YEAR'].astype(int)
    if pol_year_ind != 0:
        df_table = df_table.loc[df_table['CAL_YEAR'] == pol_year_ind]

    # Setup parameters
    COUNT_NAME, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED = setup_analysis_params(
        productName, additional_apply, additional_codes
    )

    # Calculate pivot tables
    df = calculate_pivot_tables(df_table, [byvar1, byvar2, byvar3, byvar4], COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)
    
    # Setup categorical columns
    df = setup_categorical_columns(df, [byvar1, byvar2, byvar3, byvar4])

    if df.empty:
        raise HTTPException(status_code=409, detail="Lỗi thiết lập file chưa đúng.")

    # Calculate metrics
    df = calculate_metrics(df, COUNT_NAME, NUM_CLAIMS, CLAIM_PMT, SUM_ASSURED)

    # Setup result columns
    df['VAR_YEAR'] = pol_year_ind
    df['VAR_NAME_CODE'] = var_code + ": " + byvar1 + " & " + byvar2 + " & " + byvar3 + " & " + byvar4 + (": " + str(additional_codes) if additional_apply else "")
    df['VAR_NAME1'] = byvar1 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_NAME2'] = byvar2 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_NAME3'] = byvar3 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_NAME4'] = byvar4 + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL1'] = df[byvar1].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL2'] = df[byvar2].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL3'] = df[byvar3].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")
    df['VAR_DETAIL4'] = df[byvar4].astype(str) + (": " + str(additional_descriptions) if additional_apply else "")

    df = df.drop(columns=[byvar1, byvar2, byvar3, byvar4])
    df.rename(columns={COUNT_NAME: LABEL_NAME}, inplace=True)
    
    new_order_col = ['VAR_YEAR','VAR_NAME_CODE','VAR_NAME1','VAR_NAME2','VAR_NAME3','VAR_NAME4',
                     'VAR_DETAIL1','VAR_DETAIL2','VAR_DETAIL3','VAR_DETAIL4',
                     LABEL_NAME,'NUM_CLAIMS','EXPOSURE_YEAR','EXPOSURE_PREM',
                     'CLAIM_PMT','FREQUENCY','SEVERITY','AVG_PREMIUM','PURE_PREMIUM','LOSS_RATIO','GWP_%', 'SUM_ASSURED','AVG_SUM_ASSURED']

    return format_final_dataframe(df, LABEL_NAME, NUM_CLAIMS, CLAIM_PMT, new_order_col)