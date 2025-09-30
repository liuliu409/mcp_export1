import pandas as pd

def process_claim_data(df: pd.DataFrame, calYear: int):
    log = ''
    df['OCCURENCE_DATE'] = pd.to_datetime(df['OCCURENCE_DATE'], format='%m-%d-%Y', errors='coerce')
    df = df[df["OCCURENCE_DATE"].dt.year == calYear]
    df["LD_YEAR"] = df["OCCURENCE_DATE"].dt.year


    # Convert numerical columns to numeric, coercing errors to NaN or replacing with a default value
    df['CLAIM_COST'] = pd.to_numeric(df['CLAIM_COST'], errors='coerce')

    # Ensure all text columns are treated as strings (if necessary)
    df['CLAIM_DESCRIPTION'] = df['CLAIM_DESCRIPTION'].astype(str)
    df['TYPE_CAUSE'] = df['TYPE_CAUSE'].astype(str)

    # Clean any numeric fields that shouldn't be numeric (if needed)
    df['POLICY_ID'] = df['POLICY_ID'].astype(str)
    df['CERTIFICATE_ID'] = df['CERTIFICATE_ID'].astype(str)

    df['INIT_DATE'] = pd.to_datetime(df['INIT_DATE'], format='%m-%d-%Y', errors='coerce')
    df['OCCURENCE_DATE'] = pd.to_datetime(df['OCCURENCE_DATE'], format='%m-%d-%Y', errors='coerce')
    df['REPORT_DATE'] = pd.to_datetime(df['REPORT_DATE'], format='%m-%d-%Y', errors='coerce')
    df['PAYMENT_DATE'] = pd.to_datetime(df['PAYMENT_DATE'], format='%m-%d-%Y', errors='coerce')
    
    df['REG_NO'] = pd.to_numeric(df['REG_NO'], errors='coerce')  # This will convert invalid values to NaN

    
    return [df, log]