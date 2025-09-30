import pandas as pd
from fastapi import HTTPException
from functools import reduce

def generate_age_car_vn(bins, unit):
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
            label = f"Dưới {int(next_val)} {unit}"
        elif next_val == float('inf'):
            label = f"Từ {current} {unit} trở lên"
        else:
            # For age ranges, we subtract 1 from next_val for proper range display
            label = f"Từ {current} đến {int(next_val)-1} {unit}"
        
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


def apply_mapping(df, var_single_settings, var_cate_settings):
    coverage_mapping = None
    type_vehicle_mapping = None
    age_category_mapping = None

    # Chuyển đổi list mapping thành dict mapping
    for setting in var_single_settings:
        if "COVERAGE_ID" in setting:
            # Tạo dict: key là 'cols', value là dict thông tin còn lại
            coverage_mapping = {item["cols"]: item for item in setting["COVERAGE_ID"]}
        if "TYPE_VEHICLE" in setting:
            type_vehicle_mapping = {item["cols"]: item for item in setting["TYPE_VEHICLE"]}

    if coverage_mapping is None:
        raise HTTPException(status_code=409, detail="Thiếu cấu hình mapping cho COVERAGE_ID")
    if type_vehicle_mapping is None:
        raise HTTPException(status_code=409, detail="Thiếu cấu hình mapping cho TYPE_VEHICLE")
    
    for setting in var_cate_settings:
        if "VEHICLE_AGE_GROUP" in setting:
            age_category_mapping = setting["VEHICLE_AGE_GROUP"]
    
    if age_category_mapping is None:
        raise HTTPException(status_code=409, detail="Thiếu cấu hình mapping cho VEHICLE_AGE_GROUP")

    # Mapping các trường single
    df["PROD_MOF_CODE"] = df["COVERAGE_ID"].apply(lambda x: coverage_mapping.get(str(x), {}).get("PROD_MOF_CODE", ""))
    df["PROD_MOF_NAME"] = df["COVERAGE_ID"].apply(lambda x: coverage_mapping.get(str(x), {}).get("PROD_MOF_NAME", ""))
    df["PNT_11_CODE"] = df["TYPE_VEHICLE"].apply(lambda x: type_vehicle_mapping.get(str(x), {}).get("PNT_11_CODE", ""))
    df["PNT_11_NAME"] = df["TYPE_VEHICLE"].apply(lambda x: type_vehicle_mapping.get(str(x), {}).get("PNT_11_NAME", ""))
    df["SUB_PNT_11_CODE"] = df["TYPE_VEHICLE"].apply(lambda x: type_vehicle_mapping.get(str(x), {}).get("SUB_PNT_11_CODE", ""))
    df["SUB_PNT_11_NAME"] = df["TYPE_VEHICLE"].apply(lambda x: type_vehicle_mapping.get(str(x), {}).get("SUB_PNT_11_NAME", ""))

    # Mapping VEHICLE_AGE_GROUP
    bins = [float("inf") if x == "Infinity" else float(x) for x in age_category_mapping["bin"]]
    unit = age_category_mapping["unit"]
    bins_mod, labels = generate_age_car_vn(bins, unit)

    # Remove the prefix from labels (01-, 02-, etc.)
    clean_labels = [label.split('-', 1)[1] if '-' in label else label for label in labels]
    age_labels = [label.split('-', 1)[0] if '-' in label else label for label in labels]

    df["VEHICLE_AGE_GROUP"] = pd.cut(
        df["VEHICLE_AGE"], bins=bins_mod, labels=clean_labels, include_lowest=True
    )
    if df["VEHICLE_AGE_GROUP"].isna().sum() > 0:
        df["VEHICLE_AGE_GROUP"] = df["VEHICLE_AGE_GROUP"].cat.add_categories('NaN').fillna('NaN')

    # Use zfill(2) for AGE_PNT_11_CODE to get 01, 02, etc.
    # age_labels = [str(i+1).zfill(2) for i in range(len(bins)-1)]
    df["AGE_PNT_11_CODE"] = pd.cut(
        df["VEHICLE_AGE"], bins=bins_mod, labels=age_labels, include_lowest=True
    )
    if df["AGE_PNT_11_CODE"].isna().sum() > 0:
        df["AGE_PNT_11_CODE"] = df["AGE_PNT_11_CODE"].cat.add_categories('NaN').fillna('NaN')

    # Convert to string to ensure consistent data type for grouping
    df["AGE_PNT_11_CODE"] = df["AGE_PNT_11_CODE"].astype(str)
    df["VEHICLE_AGE_GROUP"] = df["VEHICLE_AGE_GROUP"].astype(str)

    return df

LIST_HARD_COLS = [
    "PROD_MOF_CODE", "PROD_MOF_NAME", "PNT_11_CODE", "PNT_11_NAME",
    "SUB_PNT_11_CODE", "SUB_PNT_11_NAME", "VEHICLE_AGE_GROUP", "AGE_PNT_11_CODE"
]

def summary_claim(df):

    df_summary = df.pivot_table(
        index=LIST_HARD_COLS,
        values=["CLAIM_ID", "CLAIM_PMT"],
        aggfunc={"CLAIM_ID": "nunique", "CLAIM_PMT": "sum"}
    ).reset_index()

    # Rename columns for clarity
    df_summary.rename(columns={
        "CLAIM_ID": "NUM_CLAIMS",
        "CLAIM_PMT": "CLAIM_PMT"
    }, inplace=True)

    return df_summary

def summary_gwp(df):

    df_summary = df.pivot_table(
        index=LIST_HARD_COLS,
        values=["REG_NO", "VEHICLE_VALUE", "GWP"],
        aggfunc={
            "REG_NO": "nunique",
            "VEHICLE_VALUE": "sum",
            "GWP": "sum"
        }
    ).reset_index()

    # Rename columns for clarity
    df_summary.rename(columns={
        "REG_NO": "NUM_CARS",
        "VEHICLE_VALUE": "SUM_ASSURED",
        "GWP": "GROSS_PREMIUM"
    }, inplace=True)

    return df_summary

def summary_reserve(df, template_name):

    if template_name != "RES_PNT_11_02":
        cols_summary = ["UPR_END", "OSC_END", "LARC_RES_END"]
    else:
        cols_summary = ["UPR_END", "OSC_END", "LARC_RES_END", "UPR_BEG", "OSC_BEG", "LARC_RES_BEG"]

    df_summary = df.pivot_table(
        index=LIST_HARD_COLS,
        values=cols_summary,
        aggfunc="sum"
    ).reset_index()

    return df_summary

def summary_begining_report(df):

    df_summary = df.pivot_table(
        index=LIST_HARD_COLS,
        values=["UPR_END", "OSC_END", "LARC_RES_END"],
        aggfunc="sum"
    ).reset_index()

    df_summary.rename(columns={
        "UPR_END": "UPR_BEG",
        "OSC_END": "OSC_BEG",
        "LARC_RES_END": "LARC_RES_BEG"
    }, inplace=True)

    return df_summary

def combine_summaries(df_claim, df_gwp, df_reserve, df_begining):

    # Danh sách các DataFrame cần 
    if df_begining is not None:
        data_frames = [df_claim, df_gwp, df_reserve, df_begining]
    else:
        data_frames = [df_claim, df_gwp, df_reserve]
    
    # Dùng reduce để merge tất cả các dataframe trong list
    df_combined = reduce(lambda left, right: pd.merge(left, right, on=LIST_HARD_COLS, how='inner'), data_frames)

    # Fill NaN values with 0 for numeric columns12
    numeric_cols = [
        "NUM_CARS", "SUM_ASSURED", "GROSS_PREMIUM",
        "NUM_CLAIMS", "CLAIM_PMT",
        "UPR_END", "OSC_END", "LARC_RES_END","UPR_BEG", "OSC_BEG", "LARC_RES_BEG"
    ]
    
    for col in numeric_cols:
        if col in df_combined.columns:
            df_combined[col] = df_combined[col].fillna(0)

    # Group by the identifying columns and sum the numeric columns to consolidate rows
    df_combined = df_combined.groupby(LIST_HARD_COLS, as_index=False)[numeric_cols].sum()

    df_combined["UPR_PERIOD"] = df_combined["UPR_END"] - df_combined["UPR_BEG"]
    df_combined["OSC_PERIOD"] = df_combined["OSC_END"] - df_combined["OSC_BEG"]
    df_combined["LARC_RES_PERIOD"] = df_combined["LARC_RES_END"] - df_combined["LARC_RES_BEG"]
    df_combined["RES_END"] = df_combined["UPR_END"] + df_combined["OSC_END"] + df_combined["LARC_RES_END"]
    df_combined["RES_BEG"] = df_combined["UPR_BEG"] + df_combined["OSC_BEG"] + df_combined["LARC_RES_BEG"]
    df_combined["RES_PERIOD"] = df_combined["RES_END"] - df_combined["RES_BEG"]

    return df_combined