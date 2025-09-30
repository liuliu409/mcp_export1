import pandas as pd
from fastapi import HTTPException
from sqlalchemy import text, Table, MetaData, create_engine
import os
from datetime import date, datetime
from modules.database import (
    engine  # Sử dụng engine thay vì get_db_session
)

# Tạo metadata object
metadata = MetaData()
DB_SCHEMA = "frrs"
# ============ 1. DATABASE FUNCTIONS (Query Tables) ============

def get_mof_gl_template():
    """
    Lấy template General Ledger từ bảng GL
    """
    try:
        # Reflect the GL table from the database
        table = Table(
            'mof_gl', metadata, autoload_with=engine, schema=DB_SCHEMA
        )
        
        # Create query with ordering
        query = table.select().order_by(table.c.code)
        
        # Execute query and return DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return df
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying GL: {str(e)}")

def get_balance_sheet_template(type_company):
    """
    Lấy template Balance Sheet từ bảng BALANCE_SHEET
    """
    try:
        # Reflect the BALANCE_SHEET table from the database
        table = Table(
            'mof_balance_sheet', metadata, autoload_with=engine, schema=DB_SCHEMA
        )
        
        # Create query with ordering
        query = table.select().order_by(table.c.lineCode).where(table.c.type == type_company)
        
        # Execute query and return DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return df
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying BALANCE_SHEET: {str(e)}")

def get_pl01_template(type_company):
    """
    Lấy template PL01 từ bảng PL01
    """
    try:
        # Reflect the PL01 table from the database
        table = Table(
            'mof_pl_01', metadata, autoload_with=engine, schema=DB_SCHEMA
        )
        
        # Create query with ordering
        query = table.select().order_by(table.c.lineCode).where(table.c.type == type_company)
        
        # Execute query and return DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return df
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying PL01: {str(e)}")

def get_pl02_template(type_company):
    """
    Lấy template PL02 từ bảng PL02
    """
    try:
        # Reflect the PL02 table from the database
        table = Table(
            'mof_pl_02', metadata, autoload_with=engine, schema=DB_SCHEMA
        )
        
        # Create query with ordering
        query = table.select().order_by(table.c.lineCode).where(table.c.type == type_company)

        # Execute query and return DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return df
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying PL02: {str(e)}")

def get_cf01_template(type_company):
    """
    Lấy template CF01 từ bảng CF01
    """
    try:
        # Reflect the CF01 table from the database
        table = Table(
            'mof_cf_01', metadata, autoload_with=engine, schema=DB_SCHEMA
        )

        # Create query with ordering
        query = table.select().order_by(table.c.code).where(table.c.type == type_company)

        # Execute query and return DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying CF01: {str(e)}")

def get_cf02_template(type_company):
    """
    Lấy template CF02 từ bảng CF02
    """
    try:
        # Reflect the CF02 table from the database
        table = Table(
            'mof_cf_02', metadata, autoload_with=engine, schema=DB_SCHEMA
        )

        # Create query with ordering
        query = table.select().order_by(table.c.code).where(table.c.type == type_company)

        # Execute query and return DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying CF02: {str(e)}")

def get_mof_gl_company(user_id):
    """
    Lấy danh sách các công ty từ bảng mof_gl_companies
    """
    try:
        # Reflect the mof_gl_companies table from the database
        table = Table(
            'mof_gl_companies', metadata, autoload_with=engine, schema=DB_SCHEMA
        )

        # Create query with ordering
        query = table.select().order_by(table.c.code).where(table.c.createdBy == user_id)

        # Execute query and return DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying mof_gl_companies: {str(e)}")

def create_report_period_dates(report_year, report_period_code, report_period_value, is_accumulated=False):
    """
    Tạo ngày bắt đầu và kết thúc kỳ báo cáo
    
    Args:
        report_year (int): Năm báo cáo (ví dụ: 2024)
        report_period_code (str): Mã kỳ báo cáo ('MONTHLY', 'QUARTERLY', 'YEARLY')
        report_period_value (int): Giá trị kỳ báo cáo (1-12 cho tháng, 1-4 cho quý, 1 cho năm)
        is_accumulated (bool): Có phải báo cáo lũy kế từ đầu năm không
    
    Returns:
        tuple: (start_date, end_date) - Ngày bắt đầu và kết thúc kỳ báo cáo (kiểu date)
    """
    try:
        if report_period_code.upper() == 'MONTHLY':
            # Kỳ báo cáo theo tháng
            if not (1 <= report_period_value <= 12):
                raise ValueError("Tháng phải từ 1 đến 12")
            
            start_date = date(report_year, report_period_value, 1)
            
            # Tính ngày cuối tháng
            if report_period_value == 12:
                end_date = date(report_year, 12, 31)
            else:
                # Tính ngày đầu tháng tiếp theo rồi trừ 1 ngày
                if report_period_value == 12:
                    next_month_start = date(report_year + 1, 1, 1)
                else:
                    next_month_start = date(report_year, report_period_value + 1, 1)
                
                # Sử dụng timedelta thay vì pd.Timedelta
                from datetime import timedelta
                end_date = next_month_start - timedelta(days=1)
        
        elif report_period_code.upper() == 'QUARTERLY':
            # Kỳ báo cáo theo quý
            if not (1 <= report_period_value <= 4):
                raise ValueError("Quý phải từ 1 đến 4")
            
            quarter_months = {
                1: (1, 3),   # Q1: Jan-Mar
                2: (4, 6),   # Q2: Apr-Jun
                3: (7, 9),   # Q3: Jul-Sep
                4: (10, 12)  # Q4: Oct-Dec
            }
            
            start_month, end_month = quarter_months[report_period_value]
            start_date = date(report_year, start_month, 1)
            
            # Tính ngày cuối quý
            if end_month == 12:
                end_date = date(report_year, 12, 31)
            else:
                # Tính ngày đầu tháng tiếp theo rồi trừ 1 ngày
                next_month_start = date(report_year, end_month + 1, 1)
                from datetime import timedelta
                end_date = next_month_start - timedelta(days=1)
        
        elif report_period_code.upper() == 'YEARLY':
            # Kỳ báo cáo theo năm
            if report_period_value != 1:
                raise ValueError("Năm chỉ có giá trị 1")
            
            start_date = date(report_year, 1, 1)
            end_date = date(report_year, 12, 31)
        
        else:
            raise ValueError(f"Mã kỳ báo cáo không hợp lệ: {report_period_code}")
        
        # Nếu là báo cáo lũy kế, start_date luôn là đầu năm
        if is_accumulated:
            start_date = date(report_year, 1, 1)

        return start_date, end_date
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Lỗi tạo ngày kỳ báo cáo: {str(e)}")

def create_gl_mapping(gl_data, user_id, report_year, report_period_code, report_period_value):
    """
    Tạo mapping cho GL data với cả debit và credit accounts
    """
    df_company = get_mof_gl_company(user_id)

    # Tạo ngày bắt đầu và kết thúc kỳ báo cáo
    start_date, end_date = create_report_period_dates(report_year, report_period_code, report_period_value, is_accumulated=True)
    
    # Convert INVOICE_DATE to date if it's string
    if 'INVOICE_DATE' in gl_data.columns:
        gl_data['INVOICE_DATE'] = pd.to_datetime(gl_data['INVOICE_DATE']).dt.date

    # Kiểm tra INVOICE_DATE trong gl_data có đúng năm và kỳ báo cáo không
    invalid_dates = gl_data[
        (gl_data['INVOICE_DATE'] > end_date)
        | (gl_data['INVOICE_DATE'] < start_date)
    ]
    
    if not invalid_dates.empty:
        raise HTTPException(
            status_code=409, 
            detail=f"INVOICE_DATE trong GL data không thuộc kỳ báo cáo ({start_date} đến {end_date}). "
                   f"Có {len(invalid_dates)} bản ghi không hợp lệ."
        )

    # Map với DEBIT_ACC
    df_mapping = pd.merge(
        gl_data,
        df_company[['code', 'accountSM', 'accountBS', 'accountPL']],
        left_on='DEBIT_ACC',
        right_on='code',
        how='left',
        suffixes=('', '_DEBIT')
    ).rename(columns={
        'accountSM': 'DEBIT_SM',
        'accountBS': 'DEBIT_BS', 
        'accountPL': 'DEBIT_PL'
    }).drop(columns=['code'])
    
    # Map với CREDIT_ACC
    df_mapping = pd.merge(
        df_mapping,
        df_company[['code', 'accountSM', 'accountBS', 'accountPL']],
        left_on='CREDIT_ACC',
        right_on='code',
        how='left',
        suffixes=('', '_CREDIT')
    ).rename(columns={
        'accountSM': 'CREDIT_SM',
        'accountBS': 'CREDIT_BS',
        'accountPL': 'CREDIT_PL'
    }).drop(columns=['code'])

    # Kiểm tra tài khoản kết chuyển 911 có trong dữ liệu không
    if '911' not in df_mapping['DEBIT_ACC'].values and '911' not in df_mapping['CREDIT_ACC'].values:
        raise HTTPException(
            status_code=409,
            detail="Tài khoản kết chuyển 911 không có trong dữ liệu GL."
        )

    return df_mapping

def create_trial_balance(gl_data_mapping, opening_balance=None):
    """
    Tạo bảng cân đối thử từ dữ liệu GL
    """
    try:

        # Remove any unnamed or irrelevant columns
        gl_df_cleaned = gl_data_mapping[['DEBIT_ACC', 'CREDIT_ACC'
                                , 'DEBIT_AMT', 'CREDIT_AMT'
                                , 'DEBIT_SM', 'DEBIT_BS', 'DEBIT_PL'
                                , 'CREDIT_SM', 'CREDIT_BS', 'CREDIT_PL'
        ]].copy()

        # Ensure numeric columns are handled correctly (in case of text, commas etc.)
        gl_df_cleaned['DEBIT_AMT'] = pd.to_numeric(gl_df_cleaned['DEBIT_AMT'], errors='coerce').fillna(0)
        gl_df_cleaned['CREDIT_AMT'] = pd.to_numeric(gl_df_cleaned['CREDIT_AMT'], errors='coerce').fillna(0)       

        # Tổng hợp số dư tài khoản
        debit_summary = gl_df_cleaned.groupby(
            by=['DEBIT_ACC', 'DEBIT_SM', 'DEBIT_BS', 'DEBIT_PL']
        )['DEBIT_AMT'].sum().reset_index().rename(
            columns={
                'DEBIT_ACC': 'ACCOUNT',
                'DEBIT_SM': 'AG_CODE', 
                'DEBIT_BS': 'BS_CODE',
                'DEBIT_PL': 'PL_CODE',
                'DEBIT_AMT': 'PERIOD_DEBIT_AMOUNT'
            }
        )

        credit_summary = gl_df_cleaned.groupby(
            by=['DEBIT_ACC', 'DEBIT_SM', 'DEBIT_BS', 'DEBIT_PL']
        )['CREDIT_AMT'].sum().reset_index().rename(
            columns={
                'DEBIT_ACC': 'ACCOUNT',
                'DEBIT_SM': 'AG_CODE', 
                'DEBIT_BS': 'BS_CODE',
                'DEBIT_PL': 'PL_CODE',
                'CREDIT_AMT': 'PERIOD_CREDIT_AMOUNT'
            }
        )

        # Gộp debit và credit
        trial_balance = pd.merge(
            debit_summary,
            credit_summary,
            on=['ACCOUNT', 'AG_CODE', 'BS_CODE', 'PL_CODE'],
            how='outer'
        ).fillna(0)

        # Tính số dư cuối kỳ
        trial_balance['PERIOD_NET_AMOUNT'] = trial_balance['PERIOD_DEBIT_AMOUNT'] - trial_balance['PERIOD_CREDIT_AMOUNT']

        trial_balance['OPENING_DEBIT_AMOUNT'] = 0
        trial_balance['OPENING_CREDIT_AMOUNT'] = 0
        trial_balance['CLOSING_DEBIT_AMOUNT'] = 0
        trial_balance['CLOSING_CREDIT_AMOUNT'] = 0

        if opening_balance is not None:
            tb = pd.merge(
                trial_balance,
                opening_balance,
                on=['ACCOUNT', 'AG_CODE', 'BS_CODE', 'PL_CODE'],
                how='outer',
                suffixes=('', '_OPENING')
            )
            tb['OPENING_DEBIT_AMOUNT'] = tb['CLOSING_DEBIT_AMOUNT_OPENING'].fillna(0)
            tb['OPENING_CREDIT_AMOUNT'] = tb['CLOSING_CREDIT_AMOUNT_OPENING'].fillna(0)
        else:
            tb = trial_balance.copy()
            tb['OPENING_DEBIT_AMOUNT'] = 0
            tb['OPENING_CREDIT_AMOUNT'] = 0

        def calc_closing(opening_debit, opening_credit, period_debit, period_credit):
            """
            Tính số dư cuối kỳ dựa trên số dư đầu kỳ và các khoản phát sinh trong kỳ
            """
            closing_debit = max((opening_debit + period_debit) - (opening_credit + period_credit), 0)
            closing_credit = max((opening_credit + period_credit) - (opening_debit + period_debit), 0)
            
            return closing_debit, closing_credit

        # Tính số dư cuối kỳ
        tb['CLOSING_DEBIT_AMOUNT'] = tb.apply(
            lambda row: calc_closing(
                row['OPENING_DEBIT_AMOUNT'],
                row['OPENING_CREDIT_AMOUNT'],
                row['PERIOD_DEBIT_AMOUNT'],
                row['PERIOD_CREDIT_AMOUNT']
            )[0],
            axis=1
        )

        tb['CLOSING_CREDIT_AMOUNT'] = tb.apply(
            lambda row: calc_closing(
                row['OPENING_DEBIT_AMOUNT'],
                row['OPENING_CREDIT_AMOUNT'],
                row['PERIOD_DEBIT_AMOUNT'],
                row['PERIOD_CREDIT_AMOUNT']
            )[1],
            axis=1
        )

        # Số dư cuối kỳ
        tb['CLOSING_NET_AMOUNT'] = tb['CLOSING_DEBIT_AMOUNT'] - tb['CLOSING_CREDIT_AMOUNT']

        # format columns
        tb_final = tb[['ACCOUNT', 'AG_CODE', 'BS_CODE', 'PL_CODE'
                    , 'OPENING_DEBIT_AMOUNT', 'OPENING_CREDIT_AMOUNT'
                    , 'PERIOD_DEBIT_AMOUNT', 'PERIOD_CREDIT_AMOUNT'
                    , 'CLOSING_DEBIT_AMOUNT', 'CLOSING_CREDIT_AMOUNT'
                    , 'CLOSING_NET_AMOUNT']].copy()

        # Kiểm tra tổng số dư cuối kỳ == 0, nếu sai raise cảnh báo
        total_closing = tb_final['CLOSING_NET_AMOUNT'].sum()
        if abs(total_closing) > 1e-6:
                raise ValueError(f"Tổng số dư cuối kỳ không bằng 0! Tổng = {total_closing}")

        return tb_final

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating trial balance: {str(e)}")
    
def calc_detail_bs_amount(row, trial_balance, col_name):
    """
    Tính số tiền cho Balance Sheet detail từ trial_balance
    """
    bs_code = str(row[col_name])
    
    # Lấy tổng CLOSING_NET_AMOUNT từ trial_balance có BS_CODE = bs_code
    amount = trial_balance.loc[
        trial_balance['BS_CODE'] == bs_code, 'CLOSING_NET_AMOUNT'
    ].sum()
    
    # Convert code to number để xác định tài sản hay nợ phải trả
    code_num = pd.to_numeric(bs_code, errors='coerce')
    
    if pd.isna(code_num):
        return amount
    elif code_num < 300:  # Mã tài sản (100-299)
        return amount
    else:  # Mã nợ phải trả và vốn chủ sở hữu (300+)
        return -amount  # Đảo dấu cho nợ phải trả

def calc_balance_sheet_amount(row, trial_balance, col_name):
    """
    Tính số tiền cho Balance Sheet theo công thức DUNO và DUCO
    DUNO(code) = CLOSING_DEBIT_AMOUNT của accountBS = code
    DUCO(code) = -CLOSING_CREDIT_AMOUNT của accountBS = code
    """
    formula = str(row.get('calculationFormula', '')).replace(' ', '')
    if not formula:
        return 0
    
    import re
    result = 0
    
    # Tìm các pattern DUNO(xxx) và DUCO(xxx)
    duno_pattern = r'([+\-]?)DUNO\((\d+)\)'
    duco_pattern = r'([+\-]?)DUCO\((\d+)\)'
    
    # Xử lý DUNO patterns
    duno_matches = re.findall(duno_pattern, formula)
    for sign, code in duno_matches:
        multiplier = -1 if sign == '-' else 1
        # Lấy tổng CLOSING_DEBIT_AMOUNT của accountBS = code
        amount = trial_balance.loc[
            trial_balance['BS_CODE'] == code, 'CLOSING_DEBIT_AMOUNT'
        ].sum()
        result += multiplier * amount
    
    # Xử lý DUCO patterns
    duco_matches = re.findall(duco_pattern, formula)
    for sign, code in duco_matches:
        multiplier = -1 if sign == '-' else 1
        # Lấy số âm của CLOSING_CREDIT_AMOUNT của accountBS = code
        amount = trial_balance.loc[
            trial_balance['BS_CODE'] == code, 'CLOSING_CREDIT_AMOUNT'
        ].sum()
        result += multiplier * (-amount)  # Âm của credit amount
    
    return result

def calc_cashflow(row, gl_data_mapping, balance_sheet=None, trial_balance=None, pl01=None, col_name=None):
    """
    Tính số tiền cho Cash Flow theo công thức PhatSinhNO và PhatSinhCO
    PhatSinhNO(11/code) = DEBIT_SM các đầu tài khoản có 2 ký tự đầu là 11 (111,112,113,..) và CREDIT_SM = code, lấy MAX(DEBIT_AMT - CREDIT_AMT,0) 
    PhatSinhCO(11/code) = DEBIT_SM các đầu tài khoản có 2 ký tự đầu là 11 (111,112,113,..) và CREDIT_SM = code, lấy -MAX(CREDIT_AMT - DEBIT_AMT,0)
    PL(50) lấy từ PL01 theo mã số 50
    Ở đây code là các đầu tài khoản trong file gl_data_mapping
    """
    formula = str(row.get('calculationFormula', '')).replace(' ', '')
    if not formula:
        return 0
    
    import re
    result = 0
    
    # Tìm các pattern PL(xx)
    pl_pattern = r'([+\-]?)PL\((\d+)\)'
    pl_matches = re.findall(pl_pattern, formula)
    for sign, pl_code in pl_matches:
        multiplier = -1 if sign == '-' else 1
        if pl01 is not None:
            # Lấy giá trị từ pl01 theo mã số pl_code
            pl_amount = pl01.loc[pl01['code'] == pl_code, 'CURRENT_AMOUNT'].sum()
            result += multiplier * pl_amount

    # Tìm các pattern PhatSinhNO(11/xxx)
    phatsinh_no_pattern = r'([+\-]?)PhatSinhNO\(11/(\d+)\)'
    no_matches = re.findall(phatsinh_no_pattern, formula)
    
    for sign, target_code in no_matches:
        multiplier = -1 if sign == '-' else 1
        
        # PhatSinhNO(11/code): DEBIT_SM có 2 ký tự đầu là 11 (111,112,113,..) và CREDIT_SM = target_code
        # Lấy MAX(DEBIT_AMT - CREDIT_AMT, 0)
        case_filter = (
            (gl_data_mapping['DEBIT_SM'].astype(str).str[:2] == '11') &  # DEBIT_SM bắt đầu bằng 11
            (gl_data_mapping['CREDIT_SM'] == target_code)  # CREDIT_SM = target_code
        )
        
        filtered_data = gl_data_mapping.loc[case_filter]
        if not filtered_data.empty:
            # Tính MAX(DEBIT_AMT - CREDIT_AMT, 0) cho từng dòng rồi sum lại
            net_amounts = (filtered_data['DEBIT_AMT'] - filtered_data['CREDIT_AMT']).apply(lambda x: max(x, 0))
            case_amount = net_amounts.sum()
        else:
            case_amount = 0
            
        result += multiplier * case_amount
    
    # Tìm các pattern PhatSinhCO(11/xxx)
    phatsinh_co_pattern = r'([+\-]?)PhatSinhCO\(11/(\d+)\)'
    co_matches = re.findall(phatsinh_co_pattern, formula)
    
    for sign, target_code in co_matches:
        multiplier = -1 if sign == '-' else 1
        
        # PhatSinhCO(11/code): DEBIT_SM có 2 ký tự đầu là 11 (111,112,113,..) và CREDIT_SM = target_code
        # Lấy -MAX(CREDIT_AMT - DEBIT_AMT, 0)
        case_filter = (
            (gl_data_mapping['DEBIT_SM'].astype(str).str[:2] == '11') &  # DEBIT_SM bắt đầu bằng 11
            (gl_data_mapping['CREDIT_SM'] == target_code)  # CREDIT_SM = target_code
        )
        
        filtered_data = gl_data_mapping.loc[case_filter]
        if not filtered_data.empty:
            # Tính -MAX(CREDIT_AMT - DEBIT_AMT, 0) cho từng dòng rồi sum lại
            net_amounts = (filtered_data['CREDIT_AMT'] - filtered_data['DEBIT_AMT']).apply(lambda x: -max(x, 0))
            case_amount = net_amounts.sum()
        else:
            case_amount = 0
            
        result += multiplier * case_amount
    
    return result

def calc_pl_amount(row, gl_data_mapping, col_name):
    """
    Tính số tiền cho Profit/Loss theo công thức PhatSinhCO và PhatSinhNO
    PhatSinhCO: CREDIT_PL = mã số và DEBIT_SM = 911 và số tiền DEBIT_AMT - CREDIT_AMT
    PhatSinhNO: DEBIT_PL = mã số và CREDIT_SM = 911 và số tiền DEBIT_AMT - CREDIT_AMT
    """
    formula = str(row.get('calculationFormula', '')).replace(' ', '')
    if not formula:
        return 0
    
    import re
    result = 0
    pl_code = str(row[col_name])
    
    # Tìm các pattern PhatSinhCO(xxx)
    # phatsinh_co_pattern = r'([+\-]?)PhatSinhCO\((\d+)\)'
    phatsinh_co_pattern = r'([+\-]?)PhatSinhCO'
    co_matches = re.findall(phatsinh_co_pattern, formula)
    
    for sign in co_matches:
        multiplier = -1 if sign == '-' else 1

        # PhatSinhCO: CREDIT_ACC = acc và accountPL = pl_code, lấy CREDIT_AMT - DEBIT_AMT
        case_filter = (
            (gl_data_mapping['CREDIT_PL'] == pl_code) &
            (gl_data_mapping['DEBIT_SM'] == "911")
        )
        filtered_data = gl_data_mapping.loc[case_filter]
        case_amount = (filtered_data['DEBIT_AMT'] - filtered_data['CREDIT_AMT']).sum()
        result += multiplier * case_amount
    
    # Tìm các pattern PhatSinhNO(xxx)
    # phatsinh_no_pattern = r'([+\-]?)PhatSinhNO\((\d+)\)'
    phatsinh_no_pattern = r'([+\-]?)PhatSinhNO'
    no_matches = re.findall(phatsinh_no_pattern, formula)
    
    for sign in no_matches:
        multiplier = -1 if sign == '-' else 1

        # PhatSinhNO: CREDIT_ACC = acc và accountPL = pl_code, lấy DEBIT_AMT - CREDIT_AMT
        case_filter = (
            (gl_data_mapping['DEBIT_PL'] == pl_code) &
            (gl_data_mapping['CREDIT_SM'] == "911")
        )
        filtered_data = gl_data_mapping.loc[case_filter]
        case_amount = (filtered_data['DEBIT_AMT'] - filtered_data['CREDIT_AMT']).sum()
        result += multiplier * case_amount
    
    return result

def calc_total_amount(row, df, col_name, amt_name):
    formula = str(row.get('calculationFormula', '')).replace(' ', '')
    if formula:
        import re
        result = 0

        # Chuẩn hóa các dấu trừ khác nhau về dấu trừ ASCII
        formula = formula.replace('–', '-').replace('—', '-').replace('−', '-')

        # Pattern mới: [1], [2], [1.1], [1.2], etc. - bao gồm cả số đơn và số thập phân
        square_tokens = re.findall(r'([+\-]?)\[([^\]]+)\]', formula)
        for sign, code in square_tokens:
            multiplier = -1 if sign == '-' else 1
            amt = df.loc[df[col_name] == code, amt_name].sum()
            result += multiplier * amt
            # Debug logging
            # print(f"Square pattern: {sign}[{code}] = {multiplier} * {amt} = {multiplier * amt}")
        
        # Pattern cũ: (1), (2), (1.1), (1.2), etc. - để backward compatibility
        round_tokens = re.findall(r'([+\-]?)\(([^\)]+)\)', formula)
        for sign, code in round_tokens:
            multiplier = -1 if sign == '-' else 1
            amt = df.loc[df[col_name] == code, amt_name].sum()
            result += multiplier * amt
            # Debug logging
            # print(f"Round pattern: {sign}({code}) = {multiplier} * {amt} = {multiplier * amt}")
            
        # print(f"Formula: {formula}, Final result: {result}")
        return result
    return 0

def create_balance_sheet(trial_balance, type_company):
    """
    Tạo bảng cân đối kế toán từ bảng cân đối kế thử
    """
    try:
        bs_template = get_balance_sheet_template(type_company)
        
        # Sort BS by lineCode as numeric trước khi tính toán
        bs_template['lineCode_num'] = pd.to_numeric(bs_template['lineCode'], errors='coerce')
        bs_template = bs_template.sort_values('lineCode_num').reset_index(drop=True)
        
        # Khởi tạo CURRENT_AMOUNT
        bs_template['CURRENT_AMOUNT'] = 0
        
        # Tính cho dòng chi tiết trước - sử dụng calc_detail_bs_amount
        mask_detail = bs_template['isTotalLine'].astype(str).str.upper() == 'FALSE'
        for idx in bs_template[mask_detail].index:
            row = bs_template.loc[idx]
            calculated_amount = calc_detail_bs_amount(row, trial_balance, 'code')
            bs_template.loc[idx, 'CURRENT_AMOUNT'] = calculated_amount

        
        # Tính cho dòng tổng - Định nghĩa thứ tự ưu tiên rõ ràng
        mask_total = bs_template['isTotalLine'].astype(str).str.upper() == 'TRUE'
        total_rows = bs_template[mask_total].copy()
        
        # Định nghĩa thứ tự ưu tiên: số càng nhỏ = ưu tiên càng thấp (tính sau)
        def get_priority(code):
            code_str = str(code)
            if code_str in ['270', '440']:  # Các dòng tổng cuối cùng
                return 3
            elif code_str in ['100', '200', '300', '400']:  # Các dòng tổng chính
                return 2
            else:  # Các dòng tổng phụ khác
                return 1
        
        total_rows['priority'] = total_rows['code'].apply(get_priority)
        total_rows['code_num'] = pd.to_numeric(total_rows['code'], errors='coerce')
        
        # Sort theo priority tăng dần, sau đó theo code giảm dần trong cùng priority
        total_rows = total_rows.sort_values(['priority', 'code_num'], ascending=[True, False])
        
        for idx in total_rows.index:
            row = bs_template.loc[idx]
            calculated_amount = calc_total_amount(row, bs_template, 'code', 'CURRENT_AMOUNT')
            bs_template.loc[idx, 'CURRENT_AMOUNT'] = calculated_amount
        
        bs_template['noteRef'] = bs_template['noteRef'].astype(str).fillna('')
        bs_final = bs_template[['lineCode','lineName','code','noteRef','CURRENT_AMOUNT']].copy()
        
        # Drop the temporary columns
        bs_final = bs_final.drop(columns=['lineCode_num'], errors='ignore')

        # kiểm tra Tài sản = Nguồn Vốn
        total_asset = bs_final.loc[bs_final['code'] == '270', 'CURRENT_AMOUNT'].sum()
        total_equity = bs_final.loc[bs_final['code'] == '440', 'CURRENT_AMOUNT'].sum()
        
        if total_asset != total_equity:
            raise HTTPException(status_code=409, detail="Tài sản không bằng Nguồn Vốn")

        return bs_final
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting balance sheet template: {str(e)}")

def create_profitloss_01(gl_data_mapping, type_company, report_year, report_period_code, report_period_value):
    """
    Tạo báo cáo PL01 từ dữ liệu GL mapping
    """
    try:
        pl01_template = get_pl01_template(type_company)
        
        # Sort PL01 by lineCode as numeric trước khi tính toán
        pl01_template['lineCode_num'] = pd.to_numeric(pl01_template['lineCode'], errors='coerce')
        pl01_template = pl01_template.sort_values('lineCode_num').reset_index(drop=True)
        
        # Tạo gl current and accumulated amounts
        gl_current_amount = gl_data_mapping[gl_data_mapping['INVOICE_DATE'].between(
            *create_report_period_dates(report_year, report_period_code, report_period_value, is_accumulated=False)
        )]
        gl_accumulated_amount = gl_data_mapping[gl_data_mapping['INVOICE_DATE'].between(
            *create_report_period_dates(report_year, report_period_code, report_period_value, is_accumulated=True)
        )]

        # Tính cho dòng chi tiết trước
        pl01_template['CURRENT_AMOUNT'] = 0
        pl01_template['ACCUMULATED_AMOUNT'] = 0
        
        # Sửa lỗi logic: isTotalLine == FALSE là dòng chi tiết
        mask_detail = pl01_template['isTotalLine'].astype(str).str.upper() == 'FALSE'
        pl01_template.loc[mask_detail, 'CURRENT_AMOUNT'] = pl01_template.loc[mask_detail].apply(
            lambda row: calc_pl_amount(row, gl_current_amount, 'code'), axis=1
        )
        pl01_template.loc[mask_detail, 'ACCUMULATED_AMOUNT'] = pl01_template.loc[mask_detail].apply(
            lambda row: calc_pl_amount(row, gl_accumulated_amount, 'code'), axis=1
        )

        # Tính cho dòng tổng sau - tính từng dòng một để đảm bảo cập nhật tuần tự
        mask_total = pl01_template['isTotalLine'].astype(str).str.upper() == 'TRUE'
        total_indices = pl01_template[mask_total].index.tolist()
        
        for idx in total_indices:
            row = pl01_template.loc[idx]
            pl01_template.loc[idx, 'CURRENT_AMOUNT'] =  calc_total_amount(row, pl01_template, 'code', 'CURRENT_AMOUNT')
            pl01_template.loc[idx, 'ACCUMULATED_AMOUNT'] = calc_total_amount(row, pl01_template, 'code', 'ACCUMULATED_AMOUNT')

        pl01_template['noteRef'] = pl01_template['noteRef'].astype(str).fillna('')
        pl01_final = pl01_template[['lineCode','lineName','code','noteRef','CURRENT_AMOUNT','ACCUMULATED_AMOUNT']].copy()

        # Drop the temporary column
        pl01_final = pl01_final.drop(columns=['lineCode_num'], errors='ignore')

        return pl01_final
    
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Error getting PL template: {str(e)}")

def create_profitloss_02(pl01_final,type_company):
    """
    Tạo báo cáo PL02 từ báo cáo PL01
    """
    try:
        pl02_template = get_pl02_template(type_company)
        
        # Sort PL02 by lineCode as numeric trước khi tính toán
        pl02_template['lineCode_num'] = pd.to_numeric(pl02_template['lineCode'], errors='coerce')
        pl02_template = pl02_template.sort_values('lineCode_num').reset_index(drop=True)
        
        # Khởi tạo CURRENT_AMOUNT
        pl02_template['CURRENT_AMOUNT'] = 0
        pl02_template['ACCUMULATED_AMOUNT'] = 0
        
        # Tính cho dòng chi tiết trước - sử dụng calc_total_amount để xử lý formula [51]
        mask_detail = pl02_template['isTotalLine'].astype(str).str.upper() == 'FALSE'
        for idx in pl02_template[mask_detail].index:
            row = pl02_template.loc[idx]
            pl02_template.loc[idx, 'CURRENT_AMOUNT'] = calc_total_amount(row, pl01_final, 'code', 'CURRENT_AMOUNT')
            pl02_template.loc[idx, 'ACCUMULATED_AMOUNT'] = calc_total_amount(row, pl01_final, 'code', 'ACCUMULATED_AMOUNT')

        # Tính cho dòng tổng sau - sử dụng calc_total_amount
        mask_total = pl02_template['isTotalLine'].astype(str).str.upper() == 'TRUE'
        total_indices = pl02_template[mask_total].index.tolist()
        
        for idx in total_indices:
            row = pl02_template.loc[idx]
            pl02_template.loc[idx, 'CURRENT_AMOUNT'] = calc_total_amount(row, pl02_template, 'code', 'CURRENT_AMOUNT')
            pl02_template.loc[idx, 'ACCUMULATED_AMOUNT'] = calc_total_amount(row, pl02_template, 'code', 'ACCUMULATED_AMOUNT')

        pl02_final = pl02_template[['lineCode','lineName','code','CURRENT_AMOUNT','ACCUMULATED_AMOUNT']].copy()

        # Drop the temporary column
        pl02_final = pl02_final.drop(columns=['lineCode_num'], errors='ignore')

        return pl02_final
    
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Error getting PL02 template: {str(e)}")

def create_cashflow_01(gl_data_mapping, type_company, report_year, report_period_code, report_period_value):
    """
    Tạo báo cáo Cash Flow 01 từ dữ liệu GL mapping
    """
    try:
        cf01_template = get_cf01_template(type_company)
        
        # Sort Cash Flow 01 by lineCode as numeric trước khi tính toán
        cf01_template['lineCode_num'] = pd.to_numeric(cf01_template['lineCode'], errors='coerce')
        cf01_template = cf01_template.sort_values('lineCode_num').reset_index(drop=True)
        
        # Tạo gl current amount
        gl_current_amount = gl_data_mapping[gl_data_mapping['INVOICE_DATE'].between(
            *create_report_period_dates(report_year, report_period_code, report_period_value, is_accumulated=True)
        )]

        # Tính cho dòng chi tiết trước
        cf01_template['CURRENT_AMOUNT'] = 0
        
        # Sửa lỗi logic: isTotalLine == FALSE là dòng chi tiết
        mask_detail = cf01_template['isTotalLine'].astype(str).str.upper() == 'FALSE'
        cf01_template.loc[mask_detail, 'CURRENT_AMOUNT'] = cf01_template.loc[mask_detail].apply(
            lambda row: calc_cashflow(row, gl_current_amount), axis=1
        )

        # Tính cho dòng tổng sau - tính từng dòng một để đảm bảo cập nhật tuần tự
        mask_total = cf01_template['isTotalLine'].astype(str).str.upper() == 'TRUE'
        total_indices = cf01_template[mask_total].index.tolist()
        
        for idx in total_indices:
            row = cf01_template.loc[idx]
            cf01_template.loc[idx, 'CURRENT_AMOUNT'] =  calc_total_amount(row, cf01_template, 'code', 'CURRENT_AMOUNT')

        cf01_template['noteRef'] = cf01_template['noteRef'].astype(str).fillna('')
        cf01_final = cf01_template[['lineCode','lineName','code','noteRef','CURRENT_AMOUNT']].copy()

        # Drop the temporary column
        cf01_final = cf01_final.drop(columns=['lineCode_num'], errors='ignore')

        return cf01_final
    
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Error getting Cash Flow 01 template: {str(e)}")

def create_cashflow_02(gl_data_mapping, type_company
                       , balance_sheet, trial_balance, pl01
                       , report_year, report_period_code, report_period_value):
    """
    Tạo báo cáo Cash Flow 02 từ dữ liệu GL mapping
    """
    try:
        cf02_template = get_cf02_template(type_company)
        
        # Sort Cash Flow 02 by lineCode as numeric trước khi tính toán
        cf02_template['lineCode_num'] = pd.to_numeric(cf02_template['lineCode'], errors='coerce')
        cf02_template = cf02_template.sort_values('lineCode_num').reset_index(drop=True)
        
        # Tạo gl current amount
        gl_current_amount = gl_data_mapping[gl_data_mapping['INVOICE_DATE'].between(
            *create_report_period_dates(report_year, report_period_code, report_period_value, is_accumulated=True)
        )]

        # Tính cho dòng chi tiết trước
        cf02_template['CURRENT_AMOUNT'] = 0
        
        # Sửa lỗi logic: isTotalLine == FALSE là dòng chi tiết
        mask_detail = cf02_template['isTotalLine'].astype(str).str.upper() == 'FALSE'
        cf02_template.loc[mask_detail, 'CURRENT_AMOUNT'] = cf02_template.loc[mask_detail].apply(
            lambda row: calc_cashflow(row, gl_current_amount), axis=1
        )

        # Tính cho dòng tổng sau - tính từng dòng một để đảm bảo cập nhật tuần tự
        mask_total = cf02_template['isTotalLine'].astype(str).str.upper() == 'TRUE'
        total_indices = cf02_template[mask_total].index.tolist()
        
        for idx in total_indices:
            row = cf02_template.loc[idx]
            cf02_template.loc[idx, 'CURRENT_AMOUNT'] =  calc_total_amount(row, cf02_template, 'code', 'CURRENT_AMOUNT')

        cf02_template['noteRef'] = cf02_template['noteRef'].astype(str).fillna('')
        cf02_final = cf02_template[['lineCode','lineName','code','noteRef','CURRENT_AMOUNT']].copy()

        # Drop the temporary column
        cf02_final = cf02_final.drop(columns=['lineCode_num'], errors='ignore')

        return cf02_final
    
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Error getting Cash Flow 02 template: {str(e)}")

def create_financial_report(gl_data, user_id, type_company
                            , opening_balance=None, report_year=None, report_period_code=None
                            , report_period_value=None):
    """
    Tạo báo cáo tài chính từ dữ liệu GL
    pl01: báo cáo kết quả hoạt động kinh doanh
    pl02: báo cáo kết quả tổng hợp
    cf01: báo cáo lưu chuyển tiền tệ (trực tiếp)
    cf02: báo cáo lưu chuyển tiền tệ (gián tiếp) - chưa làm
    balance_sheet: bảng cân đối kế toán
    """
    try:
        # Tạo GL mapping
        gl_data_mapping = create_gl_mapping(gl_data, user_id, report_year, report_period_code, report_period_value)

        # Tạo bảng cân đối thử
        trial_balance = create_trial_balance(gl_data_mapping, opening_balance)

        # Tạo bảng cân đối kế toán
        balance_sheet = create_balance_sheet(trial_balance, type_company)

        # Tạo báo cáo PL01 và PL02 từ gl_data_mapping
        pl01 = create_profitloss_01(gl_data_mapping, type_company, report_year, report_period_code, report_period_value)
        pl02 = create_profitloss_02(pl01, type_company)

        # Tạo báo cáo Cash Flow 01 từ gl_data_mapping
        cashflow_01 = create_cashflow_01(gl_data_mapping, type_company
                                         , report_year, report_period_code, report_period_value)

        # Tạo báo cáo Cash Flow 02 từ gl_data_mapping
        cashflow_02 = create_cashflow_02(gl_data_mapping, type_company
                                         , balance_sheet, trial_balance, pl01
                                         , report_year, report_period_code, report_period_value)

        return {
            "trial_balance": trial_balance,
            "balance_sheet": balance_sheet,
            "pl01": pl01,
            "pl02": pl02,
            "cf01": cashflow_01,
            "cf02": cashflow_02
        }
    
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Error creating financial report: {str(e)}")
