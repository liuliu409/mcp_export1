import pandas as pd
from dateutil import parser
import json
import warnings
import numpy as np
from dateutil.parser import parse

jsommm = '''
{
    "url_file": "http://xperiacloud.live/data_GWP_edited_updated%20(1).xlsx",
    "file_type": "GWP",
    "setting_cols": [
        {
            "standard_name": "POLICY_ID",
            "allow_null": false,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "Số HĐ"
        },
        {
            "standard_name": "CERTIFICATE_ID",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "GCN"
        },
        {
            "standard_name": "COVERAGE_ID",
            "allow_null": false,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "Nghiệp vụ"
        },
        {
            "standard_name": "TYPE_VEHICLE",
            "allow_null": false,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Phân loại Xe"
        },
        {
            "standard_name": "PLAN_ID",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "Mã QL"
        },
        {
            "standard_name": "INIT_DATE",
            "allow_null": true,
            "data_type": "Date",
            "variable_type": "INFO",
            "import_name": "Ngày Nhập"
        },
        {
            "standard_name": "REG_NO",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "Biển số xe"
        },
        {
            "standard_name": "START_DATE",
            "allow_null": false,
            "data_type": "Date",
            "variable_type": "INFO",
            "import_name": "Ngày hiệu lực"
        },
        {
            "standard_name": "EXPIRY_DATE",
            "allow_null": false,
            "data_type": "Date",
            "variable_type": "INFO",
            "import_name": "Ngày kết thúc"
        },
        {
            "standard_name": "VEHICLE_MODEL",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Loại xe"
        },
        {
            "standard_name": "VEHICLE_BRAND",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Nhà sản xuất"
        },
        {
            "standard_name": "VEHICLE_YEAR",
            "allow_null": true,
            "data_type": "Integer",
            "variable_type": "VARS",
            "import_name": "Năm sản xuất"
        },
        {
            "standard_name": "VEHICLE_AGE",
            "allow_null": true,
            "data_type": "Integer",
            "variable_type": "CATE",
            "import_name": "Tuổi xe"
        },
        {
            "standard_name": "VEHICLE_VALUE",
            "allow_null": false,
            "data_type": "Double",
            "variable_type": "CATE",
            "import_name": "Giá trị Xe"
        },
        {
            "standard_name": "VEHICLE_FUEL",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Nhiên liệu"
        },
        {
            "standard_name": "VEHICLE_SEATS",
            "allow_null": true,
            "data_type": "Integer",
            "variable_type": "CATE",
            "import_name": "Số chỗ ngồi"
        },
        {
            "standard_name": "VEHICLE_WEIGHT",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "CATE",
            "import_name": "Trọng tải"
        },
        {
            "standard_name": "ADDRESS",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "Địa chỉ"
        },
        {
            "standard_name": "PROVINCE",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Tỉnh Thành"
        },
        {
            "standard_name": "CUSTOMER_TYPE",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Loại khách hàng"
        },
        {
            "standard_name": "CUSTOMER_CODE",
            "allow_null": false,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "mã KH"
        },
        {
            "standard_name": "GWP",
            "allow_null": false,
            "data_type": "Double",
            "variable_type": "INFO",
            "import_name": "Phí bảo hiểm gốc"
        },
        {
            "standard_name": "COMMIS",
            "allow_null": true,
            "data_type": "Double",
            "variable_type": "INFO",
            "import_name": "Hoa hồng"
        },
        {
            "standard_name": "GWP_FO",
            "allow_null": true,
            "data_type": "Double",
            "variable_type": "INFO",
            "import_name": "Phí tái bảo hiểm"
        },
        {
            "standard_name": "COMM_FO",
            "allow_null": true,
            "data_type": "Double",
            "variable_type": "INFO",
            "import_name": "Hoa hồng tái"
        },
        {
            "standard_name": "NWP",
            "allow_null": true,
            "data_type": "Double",
            "variable_type": "INFO",
            "import_name": "phí bảo hiểm giữ lại"
        },
        {
            "standard_name": "NWC",
            "allow_null": true,
            "data_type": "Double",
            "variable_type": "INFO",
            "import_name": "hoa hồng net"
        },
        {
            "standard_name": "SALES_CHANNEL",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "Kênh phân phối"
        },
        {
            "standard_name": "DEDUCTIBLE",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "INFO",
            "import_name": "Mức miễn thường"
        },
        {
            "standard_name": "INSTALLMENT",
            "allow_null": true,
            "data_type": "Date",
            "variable_type": "INFO",
            "import_name": "Kỳ thanh toán"
        },
        {
            "standard_name": "STATUS_CONTRACT",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Dạng HĐ"
        },
        {
            "standard_name": "USAGE",
            "allow_null": true,
            "data_type": "Text",
            "variable_type": "VARS",
            "import_name": "Mục đích sử dụng"
        }
    ]
}

'''

def check_duplicate_value(series, col):
    match col:
        case "POLICY_ID":
            return not series.duplicated().any()
    return True

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
    
    match col_type:
        case "Date":
            return is_date_column(series)
        case "Double":
            return is_numeric_column(series)
        case "Integer":
            return is_numeric_column(series)
        case _:
            return True  # False cho các kiểu không được hỗ trợ



def analyze_dataframe_gwp(df, json_settings):
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
            
            # Get column metadata directly from meta
            if standard_name in df.columns:
                series = df[standard_name]
                missing_count = series.isnull().sum()
                total_rows = len(df)
                missing_percentage = (missing_count / total_rows) * 100
                column_type =  meta['data_type']
                type_check = check_type(series, column_type)
                if type_check==False :  all_data[0]["error_details"]["type_check"].append({"column": standard_name, 'error':  "type failed"})
                missing_check = check_missing_value(series, meta['allow_null'])
                if missing_check==False : 
                    if missing_count ==0 :
                        all_data[0]["error_details"]["missing_check"].append({"column": standard_name, 'error':  'must be not null or empty'})
                    else :
                        all_data[0]["error_details"]["missing_check"].append({"column": standard_name, 'error': f"contains null values ({missing_percentage:.2f}%)"})
                
                check_duplicate = check_duplicate_value(series, standard_name)
                if check_duplicate==False :  all_data[0]["error_details"]["dup_check"].append({"column": standard_name, 'error': "must be unique values"})
                checkpass = "Fail"
                if type_check and missing_check and check_duplicate : checkpass = "Pass"
                


                # Calculate numeric statistics if applicable
                min_val = max_val = average_val = 'N/A'
                if column_type in ['Double', 'Integer']:
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
                    'Type': column_type,
                    'Min value': min_val,
                    'Max value': max_val,
                    'Average value': average_val,
                    'Check': checkpass
                }
            else:
                # Column not found in DataFrame
                all_data[0]["dataframe_summary"][standard_name] = {
                    'Missing Count': "N/A",
                    'Missing Percentage': "N/A",
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
        raise Exception(f"Error in analyze_dataframe_gwp: {str(e)}")


# metadata = json.loads(jsommm)# Ensure UTF-8 encoding
# import sys
# import io
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
# print(metadata)
# df = pd.read_excel(r"C:\Users\admin\Documents\Zalo Received Files\data_GWP_edited_updated (1).xlsx", engine="openpyxl")
# print(analyze_dataframe_gwp(df, metadata))
