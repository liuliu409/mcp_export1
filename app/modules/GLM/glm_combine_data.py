import pandas as pd
import time
def determine_claim_pmt_seq(row):
    claim_pmt_seq = {}
    claim_pmt_not_seq = {}
    num_claims_seq = 0
    num_claims_not_seq = 0
    if isinstance(row['CLAIM_OCCURENCE'], dict):
        for claim_id, details in row['CLAIM_OCCURENCE'].items():
            if row['START_SEQ'] <= details['OCCURENCE_DATE'] <= row['END_SEQ']:
                claim_pmt_seq[claim_id] = details['CLAIM_PMT']
                num_claims_seq += 1
            else:
                claim_pmt_not_seq[claim_id] = details['CLAIM_PMT']
                num_claims_not_seq += 1
    return pd.Series([claim_pmt_seq, claim_pmt_not_seq, num_claims_seq, num_claims_not_seq])


def combine_data(df_claim: pd.DataFrame, df_gwp: pd.DataFrame, cl_year: int):
    start_time = time.time()

    log = ''
    clmdf2 = df_claim[df_claim['CLAIM_PMT'] >=0]
    clmdf_neg = df_claim[df_claim['CLAIM_PMT'] <0]
    log += "Claim amount is negative {:,.2f}".format(clmdf_neg['CLAIM_PMT'].sum())
    clmdf2.loc[:,'LD_YEAR'] = clmdf2['OCCURENCE_DATE'].dt.year
    log += "\nDup GWP: " + str(df_gwp.duplicated(subset=['POLICY_ID','CERTIFICATE_ID','CL_YEAR']).sum())
    log += "\nDup CLAIM: " + str(clmdf2.duplicated(subset=['POLICY_ID','CERTIFICATE_ID','LD_YEAR']).sum())
    
    # Group by and aggregate
    clmdf_grouped = clmdf2.groupby(['POLICY_ID', 'CERTIFICATE_ID', 'LD_YEAR']).agg({
        'CLAIM_ID': 'count',
        'CLAIM_PMT': 'sum'
    }).reset_index()

    # Create a dictionary with CLAIM_ID as key and {OCCURENCE_DATE, CLAIM_PMT} as value
    claim_occurrence = clmdf2.groupby(['POLICY_ID', 'CERTIFICATE_ID', 'LD_YEAR']).apply(
        lambda x: {claim_id: {'OCCURENCE_DATE': occ_date, 'CLAIM_PMT': claim_pmt}
                for claim_id, occ_date, claim_pmt in zip(x['CLAIM_ID'], x['OCCURENCE_DATE'], x['CLAIM_PMT'])}
    , include_groups=False).reset_index(name='CLAIM_OCCURENCE')

    # Merge the aggregated data with the claim occurrence data
    clmdf_grouped = pd.merge(clmdf_grouped, claim_occurrence, on=['POLICY_ID', 'CERTIFICATE_ID', 'LD_YEAR'])

    # Rename columns
    clmdf_grouped.rename(columns={'CLAIM_ID': 'NUM_CLAIMS'}, inplace=True)

    log += "\nClaim amount of clm grouped: " + str(clmdf_grouped['CLAIM_PMT'].sum()) + " rows: " + str(clmdf_grouped.shape[0])
    log += "\nClaim amount of clm before group: " + str(clmdf2['CLAIM_PMT'].sum()) + " rows: " + str(clmdf2.shape[0])
    log += "\nDifference: " + str(clmdf2['CLAIM_PMT'].sum() - clmdf_grouped['CLAIM_PMT'].sum())
    df_gwp['CL_YEAR'] = df_gwp['CL_YEAR'].astype(int)
    clmdf_grouped['LD_YEAR'] = clmdf_grouped['LD_YEAR'].astype(int)
    
    combdf = pd.merge(df_gwp, clmdf_grouped, left_on=['POLICY_ID','CERTIFICATE_ID','CL_YEAR']
                  , right_on= ['POLICY_ID','CERTIFICATE_ID','LD_YEAR'], how='left', suffixes=('_GWP', '_CLM'))
    
    log += "\nPremium of comdf: " + str(combdf['EXPOSURE_PREM'].sum()) + " rows " + str(combdf.shape[0])
    log += "\nPremium before merge: " + str(df_gwp['EXPOSURE_PREM'].sum()) + " rows " + str(df_gwp.shape[0])
    log += "\nDifference in premium: " + str(df_gwp['EXPOSURE_PREM'].sum() - combdf['EXPOSURE_PREM'].sum())
    log += "\nClaim amount of comdf: " + str(combdf['CLAIM_PMT'].sum()) + " rows " + str(combdf['NUM_CLAIMS'].value_counts().sum())
    log += "\nClaim amount before merge: " + str(clmdf_grouped['CLAIM_PMT'].sum()) + " rows " + str(clmdf_grouped.shape[0])
    log += "\nDifference in claim amount: " + str(clmdf_grouped['CLAIM_PMT'].sum() - combdf['CLAIM_PMT'].sum())
    
    clm_notin_combine = clmdf_grouped[~clmdf_grouped['POLICY_ID'].isin(combdf['POLICY_ID'])]
    log += "\nClaim amount of clm not in combine: " + str(clm_notin_combine['CLAIM_PMT'].sum()) + " rows: " + str(clm_notin_combine.shape[0])

    # Apply the function to create the new columns
    combdf[['CLAIM_PMT_SEQ', 'CLAIM_PMT_NOT_SEQ', 'NUM_CLAIMS_SEQ', 'NUM_CLAIMS_NOT_SEQ']] = combdf.apply(determine_claim_pmt_seq, axis=1)

    log += "\nClaim amount of claim_pmt_seq: " + str(combdf['CLAIM_PMT_SEQ'].apply(lambda x: sum(x.values())).sum())
    log += "\nClaim amount of claim_pmt_not_seq: " + str(combdf['CLAIM_PMT_NOT_SEQ'].apply(lambda x: sum(x.values())).sum())
    log += "\nTotal Claim amount: " + str(combdf['CLAIM_PMT'].sum())
    log += "\nDifference: " + str(combdf['CLAIM_PMT'].sum() - combdf['CLAIM_PMT_SEQ'].apply(lambda x: sum(x.values())).sum() - combdf['CLAIM_PMT_NOT_SEQ'].apply(lambda x: sum(x.values())).sum())

    log += "\nNumber of claims in claim_pmt_seq: " + str(combdf['NUM_CLAIMS_SEQ'].sum())
    log += "\nNumber of claims in claim_pmt_not_seq: " + str(combdf['NUM_CLAIMS_NOT_SEQ'].sum())
    log += "\nTotal number of claims: " + str(combdf['NUM_CLAIMS'].sum())
    log += "\nDifference: " + str(combdf['NUM_CLAIMS'].sum() - combdf['NUM_CLAIMS_SEQ'].sum() - combdf['NUM_CLAIMS_NOT_SEQ'].sum())

    obs_range = (combdf['CL_YEAR'] == cl_year)
    combdf2 = combdf.loc[obs_range]
    combdf_notin_obs = combdf.loc[~obs_range]
    log += "\nExposure premium of combdf2: " + str(combdf2['EXPOSURE_PREM'].sum())
    log += "\nExposure premium of combdf_notin_obs: " + str(combdf_notin_obs['EXPOSURE_PREM'].sum())
    log += "\nTotal exposure premium: " + str(combdf['EXPOSURE_PREM'].sum())
    log += "\nDifference: " + str(combdf['EXPOSURE_PREM'].sum() - combdf2['EXPOSURE_PREM'].sum() - combdf_notin_obs['EXPOSURE_PREM'].sum())

    end_time = time.time()
    elapsed_time = end_time - start_time
    log += '\nElapsed time: ' + str(elapsed_time) + ' seconds'
    return [clm_notin_combine, combdf, log]

