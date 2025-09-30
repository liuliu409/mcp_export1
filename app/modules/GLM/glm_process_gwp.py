import pandas as pd
from dateutil import parser
import json
import warnings
import numpy as np
import sys
from IPython.display import display
import time

def year_range(st_date, ed_date):
    date_rgn = pd.date_range(st_date, ed_date, freq='YE').year
    end_year = pd.to_datetime(ed_date).year
    if end_year not in date_rgn:
        date_rgn = date_rgn.insert(len(date_rgn), end_year)
    return date_rgn

def split_date(cl_year, effdate, enddate):
    if cl_year == effdate.year:
        Start_date = pd.Timestamp(cl_year,effdate.month,effdate.day)
        end_date = pd.Timestamp(cl_year,12,31)
        exp_day = end_date - Start_date + pd.Timedelta(days=1)
    elif cl_year > effdate.year and cl_year < enddate.year:
        Start_date = pd.Timestamp(cl_year,1,1)
        end_date = pd.Timestamp(cl_year,12,31)
        exp_day = end_date - Start_date + pd.Timedelta(days=1)
    elif cl_year == enddate.year:
        Start_date = pd.Timestamp(cl_year,1,1)
        end_date = pd.Timestamp(cl_year,enddate.month,enddate.day)
        exp_day = end_date - Start_date
    return [Start_date,end_date,exp_day]

def cal_exposure(exp_day, nb_year, cl_year, effdate, enddate):
    days_in_year = pd.Timestamp(str(cl_year)+'-12-31').dayofyear
    leap_year = pd.Timestamp(str(enddate.year)+'-12-31').is_leap_year
    leap_date = pd.Timestamp(str(enddate.year)+'-02-28')

    if nb_year == 1 and leap_year == True:
        if enddate > leap_date:
            exp_year = exp_day.days/366
        else:
            exp_year = exp_day.days/365
    else:
        exp_year = exp_day.days/days_in_year

    return [exp_year,days_in_year]


def process_gwp_data(df: pd.DataFrame, calYear: int):
    start_time = time.time()

    log = ''

    df['START_DATE'] = pd.to_datetime(df['START_DATE'], format='%Y-%m-%d 00:00:00', errors='coerce')
    df['EXPIRY_DATE'] = pd.to_datetime(df['EXPIRY_DATE'], format='%Y-%m-%d 00:00:00', errors='coerce')
    df['INIT_DATE'] = pd.to_datetime(df['INIT_DATE'], format='%Y-%m-%d 00:00:00', errors='coerce')
    df.loc[df['START_DATE'].isnull(), 'START_DATE'] = df['INIT_DATE']
    df['EXPIRY_DATE'] = df['EXPIRY_DATE'].fillna(df['START_DATE'] + pd.DateOffset(years=1))
    df['POL_KEY'] = df['POLICY_ID'].astype(str) + df['CERTIFICATE_ID'].astype(str)
    df.loc[:,'NB_YEAR'] = df.loc[:,'EXPIRY_DATE'].dt.year - df.loc[:,'START_DATE'].dt.year

    # Generate the year ranges for each row
    df['YEAR_RANGE'] = df.apply(lambda row: year_range(row['START_DATE'], row['EXPIRY_DATE']), axis=1)
    # Explode the DataFrame to have one row per year
    df_exploded = df.explode('YEAR_RANGE')
    # Rename the columns to match the desired output
    df_exploded = df_exploded.rename(columns={'YEAR_RANGE': 'CL_YEAR'})
    # Select only the necessary columns
    df_years2 = df_exploded[['CL_YEAR', 'POL_KEY']]
    log = log + "\nCheck dup ['POL_KEY','CL_YEAR']: " + str(df_years2.duplicated(['POL_KEY','CL_YEAR']).sum())

    total_row_af_splityear = sum((index+1) * value for index, value in df['NB_YEAR'].value_counts().items())
    log = log + '\nCheck total row after split year: ' + str(df_years2.shape[0]-total_row_af_splityear)

    df2 = pd.merge(df,df_years2, on='POL_KEY', how='right')
    log = log + "\nCheck rows after merge: " + str(df2.shape[0] - df_years2.shape[0])

    df2['START_SEQ'] = df2.apply(lambda x: split_date(x['CL_YEAR'], x['START_DATE'], x['EXPIRY_DATE'])[0], axis=1)
    df2['END_SEQ'] = df2.apply(lambda x: split_date(x['CL_YEAR'], x['START_DATE'], x['EXPIRY_DATE'])[1], axis=1)
    df2['EXPOSURE_DAY'] = df2.apply(lambda x: split_date(x['CL_YEAR'], x['START_DATE'], x['EXPIRY_DATE'])[2], axis=1)

    df2['DAYS_OF_YEAR'] = df2.apply(lambda x: cal_exposure(x['EXPOSURE_DAY'], x['NB_YEAR'], x['CL_YEAR'],  x['START_DATE'], x['EXPIRY_DATE'])[1], axis=1)
    df2['EXPOSURE_YEAR'] = df2.apply(lambda x: cal_exposure(x['EXPOSURE_DAY'], x['NB_YEAR'], x['CL_YEAR'],  x['START_DATE'], x['EXPIRY_DATE'])[0], axis=1)
    tot_expo = df2.groupby('POL_KEY').agg({'EXPOSURE_YEAR':'sum'}).reset_index()
    tot_expo.rename(columns={'EXPOSURE_YEAR':'TOTAL_EXPOSURE'}, inplace=True)
    df3 = pd.merge(df2,tot_expo, on='POL_KEY', how='left')
    df3['EXPOSURE_PREM'] = df3['GWP']*(df3['EXPOSURE_YEAR']/df3['TOTAL_EXPOSURE'])
    log = log + "\nPremium after exposure: " + str(df3['EXPOSURE_PREM'].sum() + df3.shape[0])
    log = log + "\nPremium of original: " + str(df['GWP'].sum())
    log = log + "\nDifference: " + str(df['GWP'].sum() - df3['EXPOSURE_PREM'].sum())

    df3_pos = df3[df3['EXPOSURE_PREM'] > 0]
    df3_neg = df3[df3['EXPOSURE_PREM'] <= 0]

    log += '\nPremium of positive: ' + str(df3_pos['EXPOSURE_PREM'].sum()) + " rows: " + str(df3_pos.shape[0])
    log += '\nPremium of negative: ' + str(df3_neg['EXPOSURE_PREM'].sum()) + " rows: " + str(df3_neg.shape[0])
    log += "\nDifference: " + str(df3_pos['EXPOSURE_PREM'].sum() + df3_neg['EXPOSURE_PREM'].sum() - df3['EXPOSURE_PREM'].sum())
    
    log += "\nCheck rows after split: " + str(df3_pos.shape[0] + df3_neg.shape[0] - df3.shape[0])
    df_final = df3_pos[df3_pos["CL_YEAR"].isin([calYear])]
    df_final.loc[:, 'YEAR_RANGE'] = df_final['YEAR_RANGE'].apply(lambda x: list(x) if isinstance(x, pd.Index) else x)


    end_time = time.time()
    elapsed_time = end_time - start_time
    log += '\nElapsed time: ' + str(elapsed_time) + ' seconds'

    return [df_final, log]
