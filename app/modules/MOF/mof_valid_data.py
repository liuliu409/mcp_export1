import warnings
import pandas as pd
import numpy as np
from fastapi import HTTPException
from dateutil.parser import parse

def check_duplicate_value(series, col, templateName, df=None):
    # CLAIM_ID phải không trùng (CLM)
    if "CLM" in templateName and col == "CLAIM_ID":
        return not series.duplicated().any()
    # GWP: cặp (POLICY_ID, CERTIFICATE_ID) phải không trùng
    if "GWP" in templateName and col in ["POLICY_ID", "CERTIFICATE_ID"] and df is not None:
        if {"POLICY_ID", "CERTIFICATE_ID"}.issubset(df.columns):
            return not df.duplicated(subset=["POLICY_ID", "CERTIFICATE_ID"]).any()
    return True

def check_missing_value(series, is_null_expected):
    has_missing = series.isnull().any()
    return True if is_null_expected else (not has_missing)

DATE_FORMATS = [
    "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y", "%m/%d/%Y",
    "%Y/%m/%d", "%d.%m.%Y", "%m.%d.%Y", "%d %b %Y", "%d %B %Y",
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y %I:%M %p"
]

def is_date_column(series):
    try:
        clean = series.dropna().astype(str).str.strip()
        clean = clean[clean != ""]
        if clean.empty:
            return False

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            expanded = clean.str.split(",").explode().str.strip()
            expanded = expanded[(expanded != "") & expanded.notna()]
            if expanded.empty:
                return False

            for fmt in DATE_FORMATS:
                try:
                    pd.to_datetime(expanded, format=fmt, errors="raise")
                    return True
                except ValueError:
                    continue

            # Thử parse tự động cuối cùng
            try:
                expanded.apply(lambda x: parse(x, fuzzy=False))
                return True
            except (ValueError, TypeError):
                return False
    except (ValueError, TypeError):
        return False

def is_numeric_column(series):
    s = series.dropna()
    s = s[s != ""]
    return pd.to_numeric(s, errors="coerce").notna().all()

def check_type(series, col_type):
    if series.empty:
        return False
    t = (col_type or "").lower()
    if t == "date":
        return is_date_column(series)
    if t in ("double", "integer"):
        return is_numeric_column(series)
    return True  # các kiểu khác coi như pass

def analyze_dataframe(df, json_settings):
    try:
        # unwrap
        if 'json_settings' in json_settings:
            json_settings = json_settings['json_settings']

        if 'setting_cols' not in json_settings:
            raise KeyError("Missing required field: setting_cols")

        result = [{
            "dataframe_summary": {},
            "error_details": {
                "type_check": [],
                "missing_check": [],
                "unknown_check": [],
                "dup_check": []
            }
        }]

        template_name = json_settings.get("templateName", "")
        setting_cols = json_settings['setting_cols']

        # map import_name -> standard_name rồi rename trước khi check
        mapping = {item['import_name']: item['standard_name'] for item in setting_cols}
        df = df.rename(columns=mapping)
        df = df[df.columns.intersection(mapping.values())]

        for meta in setting_cols:
            standard_name = meta['standard_name']
            column_type = meta.get('data_type', '')
            allow_null = meta.get('allow_null', True)
            variable_type = meta.get('variable_type', '')

            # chỉ kiểm các cột INFO như yêu cầu cũ
            if variable_type != 'INFO':
                continue

            if standard_name in df.columns:
                series = df[standard_name]

                total_rows = len(df)
                missing_count = series.isnull().sum()
                missing_pct = (missing_count / total_rows * 100) if total_rows else 0

                series_str = series.astype(str) if not pd.api.types.is_string_dtype(series) else series
                unknown_count = series_str.str.startswith("Unknown", na=False).sum()
                unknown_pct = (unknown_count / total_rows * 100) if total_rows else 0

                # checks
                type_ok = check_type(series, column_type)
                if not type_ok:
                    result[0]["error_details"]["type_check"].append({
                        "column": standard_name, "error": "type failed"
                    })

                missing_ok = check_missing_value(series, allow_null)
                if not missing_ok:
                    msg = "must be not null or empty" if total_rows == 0 else f"contains null values ({missing_pct:.2f}%)"
                    result[0]["error_details"]["missing_check"].append({
                        "column": standard_name, "error": msg
                    })

                # unknown threshold = 0% (giữ nguyên mặc định)
                if unknown_pct > 0:
                    result[0]["error_details"]["unknown_check"].append({
                        "column": standard_name,
                        "error": f"contains unknown values ({unknown_pct:.2f}%)"
                    })

                dup_ok = check_duplicate_value(series, standard_name, template_name, df)
                if not dup_ok:
                    result[0]["error_details"]["dup_check"].append({
                        "column": standard_name, "error": "contains duplicate values"
                    })

                checkpass = "Pass" if (type_ok and missing_ok and (unknown_pct == 0) and dup_ok) else "Fail"

                # stats numeric
                min_val = max_val = avg_val = 'N/A'
                if (column_type or "").lower() in ['double', 'integer']:
                    num = pd.to_numeric(series, errors='coerce')
                    min_val = num.min()
                    max_val = num.max()
                    avg_val = num.mean()

                result[0]["dataframe_summary"][standard_name] = {
                    'Missing Count': missing_count,
                    'Missing Percentage': f"{missing_pct:.2f}%",
                    'Unknown Count': unknown_count,
                    'Unknown Percentage': f"{unknown_pct:.2f}%",
                    'Type': column_type,
                    'Min value': f"{min_val:,.2f}" if isinstance(min_val, (int, float, np.floating)) and pd.notna(min_val) else min_val,
                    'Max value': f"{max_val:,.2f}" if isinstance(max_val, (int, float, np.floating)) and pd.notna(max_val) else max_val,
                    'Average value': f"{avg_val:,.2f}" if isinstance(avg_val, (int, float, np.floating)) and pd.notna(avg_val) else avg_val,
                    'Check': checkpass
                }
            else:
                result[0]["dataframe_summary"][standard_name] = {
                    'Missing Count': "N/A",
                    'Missing Percentage': "N/A",
                    'Unknown Count': "N/A",
                    'Unknown Percentage': "N/A",
                    'Type': "N/A",
                    'Min value': "N/A",
                    'Max value': "N/A",
                    'Average value': "N/A",
                    'Check': "Column not found"
                }

        return result

    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"Error in analyze_dataframe: {str(e)}")
