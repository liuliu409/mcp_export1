import pandas as pd
from dateutil import parser
import json
import warnings
import numpy as np
from dateutil.parser import parse

def check_missing_value(series, is_null_expected):
    has_missing_value = series.isnull().any()
    if is_null_expected == True:
      return True
    else:
      return False if has_missing_value else True

# List of common date formats to check
DATE_FORMATS = [
    "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y", "%m/%d/%Y",  # Standard formats
    "%Y/%m/%d", "%d.%m.%Y", "%m.%d.%Y", "%d %b %Y", "%d %B %Y",  # Various separators
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m-%d-%Y %I:%M %p"  # DateTime formats
]

def is_date_column(series):
    try:
        # Loại bỏ giá trị trống hoặc NaN
        clean_series = series.dropna().astype(str).str.strip()
        clean_series = clean_series[clean_series != ""]
        
        if clean_series.empty:
            return False  # Nếu không còn giá trị nào thì không phải cột ngày

        # Xử lý trường hợp có dấu phẩy (giá trị chứa nhiều ngày)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            series_expanded = clean_series.str.split(",").explode().str.strip()

            # Kiểm tra nếu có giá trị hợp lệ
            series_expanded = series_expanded[series_expanded != ""].dropna()
            if series_expanded.empty:
                return False

            # Kiểm tra từng format một
            for fmt in DATE_FORMATS:
                try:
                    pd.to_datetime(series_expanded, format=fmt, errors="raise")
                    return True  # Nếu chuyển đổi thành công với bất kỳ format nào
                except ValueError:
                    continue  # Thử format tiếp theo

            # Nếu tất cả format đều thất bại, kiểm tra parse tự động
            try:
                series_expanded.apply(lambda x: parse(x, fuzzy=False))
                return True
            except (ValueError, TypeError):
                return False

    except (ValueError, TypeError):
        return False

def is_numeric_column(series):
    series = series.dropna()  # Loại bỏ NaN
    series = series[series != ""]  # Loại bỏ chuỗi rỗng
    return pd.to_numeric(series, errors="coerce").notna().all()

def check_type(series, col_type):
    if series.empty:
        return False  # Handle empty series
    
    match col_type.lower():
        case "date":
            return is_date_column(series)
        case "double":
            return is_numeric_column(series)
        case "integer":
            return is_numeric_column(series)
        case _:
            return True  # False cho các kiểu không được hỗ trợ

def analyze_dataframe_combine(df, json_settings):
    try:
        # Get the json_settings if it's wrapped
        if 'json_settings' in json_settings:
            json_settings = json_settings['json_settings']

        # Validate required field
        if 'setting_cols' not in json_settings:
            raise KeyError(f"Missing required field: setting_cols")

        all_data = [{
            "dataframe_summary": {},
            "error_details": {
                "type_check": [],
                "missing_check": [],
                "unknown_check": [],
                "dup_check": []
            }
        }]

        setting_cols = json_settings['setting_cols']

        # Create mapping dictionary from import_name to standard_name
        mapping_dict = {item['import_name']: item['standard_name'] for item in setting_cols}
        
        # Rename DataFrame columns using the mapping
        df = df.rename(columns=mapping_dict)
        df = df[df.columns.intersection(mapping_dict.values())]
        # Process each column according to the mapping
        for meta in setting_cols:
            standard_name = meta['standard_name']
            column_type = meta['data_type']
            allow_null = meta['allow_null']
            variable_type = meta['variable_type']

            if variable_type != 'INFO':
                continue

            # Get column metadata directly from meta
            if standard_name in df.columns:
                series = df[standard_name]
                
                # Calculate basic statistics
                missing_count = series.isnull().sum()
                total_rows = len(df)
                missing_percentage = (missing_count / total_rows) * 100 if total_rows > 0 else 0
                
                # Convert to string for unknown check
                series_str = series.astype(str) if not pd.api.types.is_string_dtype(series) else series
                
                unknown_count = series_str.str.startswith("Unknown", na=False).sum()
                unknown_percentage = (unknown_count / total_rows) * 100 if total_rows > 0 else 0

                # Perform checks
                type_check = check_type(series, column_type)
                if not type_check:
                    all_data[0]["error_details"]["type_check"].append({
                        "column": standard_name,
                        "error": "type failed"
                    })

                missing_check = check_missing_value(series, allow_null)
                if not missing_check:
                    if total_rows == 0:
                        all_data[0]["error_details"]["missing_check"].append({
                            "column": standard_name,
                            "error": "must be not null or empty"
                        })
                    else:
                        all_data[0]["error_details"]["missing_check"].append({
                            "column": standard_name,
                            "error": f"contains null values ({missing_percentage:.2f}%)"
                        })

                # Unknown value check
                threshold = 0
                check_unknown = unknown_percentage <= threshold if not allow_null else True
                if not check_unknown:
                    all_data[0]["error_details"]["unknown_check"].append({
                        "column": standard_name,
                        "error": f"contains unknown values ({unknown_percentage:.2f}%)"
                    })

                # Overall check status
                checkpass = "Pass" if (type_check and missing_check and check_unknown) else "Fail"

                # Calculate numeric statistics if applicable
                min_val = max_val = average_val = 'N/A'
                if column_type.lower() in ['double', 'integer']:
                    try:
                        numeric_series = pd.to_numeric(series, errors='coerce')
                        min_val = numeric_series.min()
                        max_val = numeric_series.max()
                        average_val = numeric_series.mean()
                    except:
                        pass

                # Store summary
                all_data[0]["dataframe_summary"][standard_name] = {
                    'Missing Count': missing_count,
                    'Missing Percentage': f"{missing_percentage:.2f}%",
                    'Unknown Count': unknown_count,
                    'Unknown Percentage': f"{unknown_percentage:.2f}%",
                    'Type': column_type,
                    'Min value': f"{min_val:,.2f}" if isinstance(min_val, (int, float)) else min_val,
                    'Max value': f"{max_val:,.2f}" if isinstance(max_val, (int, float)) else max_val,
                    'Average value': f"{average_val:,.2f}" if isinstance(average_val, (int, float)) else average_val,
                    'Check': checkpass
                }
            else:
                # Column not found in DataFrame
                all_data[0]["dataframe_summary"][standard_name] = {
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

        return all_data

    except KeyError as e:
        raise KeyError(f"Missing required field: {str(e)}")
    except Exception as e:
        raise Exception(f"Error in analyze_dataframe_combine: {str(e)}")