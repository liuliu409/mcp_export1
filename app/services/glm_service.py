from datetime import datetime
import io
import os
from urllib.parse import urlparse
from fastapi import HTTPException
import httpx
import numpy as np
import requests
import pandas as pd
from exceptions import ConflictException
import json
import re
from utils.json_encoder import NpEncoder
from modules.GLM.glm_valid_claim import analyze_dataframe_claim
from modules.GLM.glm_valid_gwp import analyze_dataframe_gwp
from modules.GLM.glm_valid_combine import analyze_dataframe_combine
from modules.db_parquet import upload_to_s3, cfg
from modules.GLM.glm_varb_analysis import (
    categorize_car,
    categorize_health,
    OWA_func,
    TWA_func,
    threeway_func,
    fourway_func,
)

class GLMService:
    async def extract_mapping_columns(self, url_file: str) -> dict:
        start_time = datetime.now()

        if not url_file:
            raise HTTPException(status_code=400, detail="url_file is required")

        try:
            async with httpx.AsyncClient(timeout=300.0) as client: # timeout 300 giây = 5 phút
                response = await client.get(url_file)
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=400, detail=f"Error downloading file: {e}")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"HTTP error: {e}")

        contents = response.content

        # Lấy phần mở rộng từ path trong URL, bỏ qua query string
        parsed_url = urlparse(url_file)
        file_name = os.path.basename(parsed_url.path)
        file_extension = os.path.splitext(file_name)[1].lower()

        try:
            if file_extension == ".csv":
                df = pd.read_csv(io.BytesIO(contents), nrows=2)
            elif file_extension in [".xlsx", ".xls", ".xlsm"]:
                df = pd.read_excel(io.BytesIO(contents), nrows=2, engine="openpyxl")
            else:
                raise HTTPException(status_code=400, detail="Unsupported file format")
        except ValueError as e:
            raise ConflictException(f"File không đúng định dạng: {e}")
        except Exception as e:
            raise ConflictException(f"Lỗi đọc file: {e}")

        if (
            df.iloc[0]
            .apply(lambda x: isinstance(x, (np.float64, np.float32, pd.Timestamp)))
            .any()
        ):
            raise ConflictException("Không đúng mẫu file, vui lòng kiểm tra lại")

        column_mapping = dict(zip(df.columns, df.iloc[0]))

        return {
            "status": True,
            "message": f"File name: '{file_name}' created mapping columns successfully",
            "times_run": datetime.now() - start_time,
            "data": {"columnNames": column_mapping},
        }
    
    async def download_file_from_url(self, url_file: str) -> tuple[bytes, str, str]:
        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                response = await client.get(url_file)
                response.raise_for_status()
        except httpx.RequestError as e:
            raise HTTPException(status_code=400, detail=f"Error downloading file: {str(e)}")

        contents = response.content
        parsed_url = urlparse(url_file)
        file_name = os.path.basename(parsed_url.path)
        file_extension = os.path.splitext(file_name)[1].lower()

        return contents, file_name, file_extension

    async def analyze_excel_structure(self, url_file: str) -> dict:
        """
        Phân tích cấu trúc file Excel để xem có những sheet nào và thông tin cơ bản
        
        Args:
            url_file (str): URL của file Excel
            
        Returns:
            dict: Thông tin về cấu trúc file Excel
        """
        start_time = datetime.now()
        
        if not url_file:
            raise HTTPException(status_code=400, detail="url_file is required")

        try:
            # Download file
            contents, file_name, file_extension = await self.download_file_from_url(url_file)
            
            # Chỉ xử lý file Excel
            if file_extension not in [".xlsx", ".xls", ".xlsm"]:
                raise HTTPException(status_code=409, detail="Only Excel files are supported for structure analysis")

            # Phân tích cấu trúc
            excel_file = pd.ExcelFile(io.BytesIO(contents))
            sheet_info = {}
            data_sheets = []
            other_sheets = []
            
            for sheet_name in excel_file.sheet_names:
                try:
                    # Đọc chỉ vài dòng đầu để kiểm tra cấu trúc
                    df_preview = pd.read_excel(
                        io.BytesIO(contents), 
                        sheet_name=sheet_name, 
                        nrows=5
                    )
                    
                    # Đếm tổng số dòng trong sheet
                    df_full = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name)
                    total_rows = len(df_full)
                    
                    sheet_info[sheet_name] = {
                        'total_rows': total_rows,
                        'columns': len(df_preview.columns),
                        'column_names': list(df_preview.columns)[:10],  # Chỉ lấy 10 cột đầu
                        'has_data': total_rows > 1,  # Có data ngoài header
                        'preview_data': df_preview.head(3).to_dict('records') if total_rows > 0 else []
                    }
                    
                    # Phân loại sheet
                    if sheet_name.startswith("DATA"):
                        data_sheets.append(sheet_name)
                    else:
                        other_sheets.append(sheet_name)
                        
                except Exception as sheet_error:
                    sheet_info[sheet_name] = {
                        'error': f'Could not read sheet: {str(sheet_error)}',
                        'readable': False
                    }
            
            return {
                "status": True,
                "message": f"File '{file_name}' structure analyzed successfully",
                "times_run": datetime.now() - start_time,
                "data": {
                    "file_name": file_name,
                    "total_sheets": len(excel_file.sheet_names),
                    "data_sheets": sorted(data_sheets),
                    "other_sheets": other_sheets,
                    "data_sheets_count": len(data_sheets),
                    "sheet_details": sheet_info,
                    "recommended_action": {
                        "can_import": len(data_sheets) > 0,
                        "message": f"Found {len(data_sheets)} DATA sheets_name for import" if len(data_sheets) > 0 
                                 else "No DATA sheets found. Please check file format."
                    }
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error analyzing Excel structure: {str(e)}")

    def analyze_excel_structure_from_contents(self, contents: bytes, file_name: str) -> dict:
        """
        Phân tích cấu trúc file Excel từ contents đã download
        
        Args:
            contents (bytes): Nội dung file Excel
            file_name (str): Tên file
            
        Returns:
            dict: Thông tin về cấu trúc file Excel
        """
        try:
            excel_file = pd.ExcelFile(io.BytesIO(contents))
            sheet_info = {}
            data_sheets = []
            other_sheets = []
            
            for sheet_name in excel_file.sheet_names:
                try:
                    # Đọc chỉ vài dòng đầu để kiểm tra cấu trúc
                    df_preview = pd.read_excel(
                        io.BytesIO(contents), 
                        sheet_name=sheet_name, 
                        nrows=3,
                        engine="openpyxl"
                    )
                    
                    # Ước lượng tổng số dòng (để tránh load toàn bộ file lớn)
                    try:
                        df_count = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name, engine="openpyxl")
                        total_rows = len(df_count)
                        del df_count  # Free memory
                    except:
                        total_rows = len(df_preview)  # Fallback
                    
                    sheet_info[sheet_name] = {
                        'total_rows': total_rows,
                        'columns': len(df_preview.columns),
                        'column_names': list(df_preview.columns)[:10],  # Chỉ lấy 10 cột đầu
                        'has_data': total_rows > 1,
                        'is_data_sheet': sheet_name.startswith("DATA")
                    }
                    
                    # Phân loại sheet
                    if sheet_name.startswith("DATA"):
                        data_sheets.append(sheet_name)
                    else:
                        other_sheets.append(sheet_name)
                        
                except Exception as sheet_error:
                    sheet_info[sheet_name] = {
                        'error': f'Could not read sheet: {str(sheet_error)}',
                        'readable': False,
                        'is_data_sheet': sheet_name.startswith("DATA")
                    }
            
            return {
                "file_name": file_name,
                "total_sheets": len(excel_file.sheet_names),
                "data_sheets": sorted(data_sheets),
                "other_sheets": other_sheets,
                "data_sheets_count": len(data_sheets),
                "sheet_details": sheet_info,
                "can_import": len(data_sheets) > 0
            }
            
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error analyzing Excel structure: {str(e)}")

    async def parse_file_from_url(self, url_file: str, skiprows: int = 1, include_data_sheets_only: bool = False) -> pd.DataFrame:
        """
        Download và parse file từ URL với hỗ trợ CSV và Excel (bao gồm multiple sheets)
        
        Args:
            url_file (str): URL của file cần parse
            skiprows (int): Số dòng bỏ qua khi đọc file (default: 1)
            include_data_sheets_only (bool): Chỉ đọc các sheet có tên bắt đầu bằng "DATA" (default: False)
            
        Returns:
            pd.DataFrame: DataFrame chứa dữ liệu đã parse
        """
        
        if not url_file:
            raise HTTPException(status_code=400, detail="url_file is required")

        try:
            # Download file
            contents, file_name, file_extension = await self.download_file_from_url(url_file)
            
            # Parse file dựa trên extension
            if file_extension == ".csv":
                df_import = pd.read_csv(io.BytesIO(contents))
                print(f"✓ CSV file parsed: {len(df_import):,} rows, {len(df_import.columns)} columns")
                
            elif file_extension in [".xlsx", ".xls", ".xlsm"]:
                if include_data_sheets_only:
                    # Parse Excel với chỉ DATA sheets
                    df_import = self._parse_excel_data_sheets(contents, file_name, skiprows)
                else:
                    # Parse Excel thông thường (sheet đầu tiên)
                    df_import = pd.read_excel(io.BytesIO(contents), skiprows=skiprows, engine="openpyxl")
                    print(f"✓ Excel file parsed: {len(df_import):,} rows, {len(df_import.columns)} columns")
                    
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported file format: {file_extension}")
                
            return df_import
            
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=409, detail=f"Error parsing file '{file_name}': {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error parsing file: {str(e)}")

    def _parse_excel_data_sheets(self, contents: bytes, file_name: str, skiprows: int = 1) -> pd.DataFrame:
        """
        Parse Excel file với chỉ các sheet có tên bắt đầu bằng "DATA"
        
        Args:
            contents (bytes): Nội dung file Excel
            file_name (str): Tên file
            skiprows (int): Số dòng bỏ qua
            
        Returns:
            pd.DataFrame: DataFrame gộp từ tất cả DATA sheets
        """
        try:
            # Phân tích cấu trúc file trước
            structure = self.analyze_excel_structure_from_contents(contents, file_name)
            data_sheets = structure['data_sheets']
            
            print(f"📊 Excel Structure Analysis for '{file_name}':")
            print(f"📊 Total sheets: {structure['total_sheets']}")
            print(f"📊 DATA sheets found: {len(data_sheets)} - {data_sheets}")
            print(f"📊 Other sheets: {structure['other_sheets']}")
            
            if not data_sheets:
                available_sheets = list(structure['sheet_details'].keys())
                raise HTTPException(
                    status_code=400,
                    detail=f"Không tìm thấy DATA sheets trong file '{file_name}'. Available sheets: {available_sheets}"
                )
            
            # Import data từ DATA sheets
            df_list = []
            total_rows = 0
            
            for sheet_name in sorted(data_sheets):
                try:
                    df_sheet = pd.read_excel(
                        io.BytesIO(contents), 
                        sheet_name=sheet_name, 
                        skiprows=skiprows,
                        engine="openpyxl"
                    )
                    
                    if len(df_sheet) > 0:
                        # Thêm thông tin sheet (optional)
                        df_sheet['_SOURCE_SHEET'] = sheet_name
                        df_list.append(df_sheet)
                        total_rows += len(df_sheet)
                        print(f"✓ Sheet {sheet_name}: {len(df_sheet):,} rows imported")
                    else:
                        print(f"⚠️ Sheet {sheet_name}: Empty sheet, skipped")
                        
                except Exception as e:
                    print(f"❌ Error reading sheet {sheet_name}: {str(e)}")
                    continue
            
            if not df_list:
                raise HTTPException(
                    status_code=400,
                    detail=f"Không có dữ liệu hợp lệ trong các DATA sheets của file '{file_name}'"
                )
            
            # Gộp tất cả dataframes
            df_combined = pd.concat(df_list, ignore_index=True)
            
            # Loại bỏ cột _SOURCE_SHEET nếu không cần thiết
            if '_SOURCE_SHEET' in df_combined.columns:
                df_combined = df_combined.drop(columns=['_SOURCE_SHEET'])
            
            print(f"=" * 50)
            print(f"✅ EXCEL PARSE SUMMARY:")
            print(f"✅ Processed {len(data_sheets)} DATA sheets: {', '.join(sorted(data_sheets))}")
            print(f"✅ Total rows imported: {len(df_combined):,}")
            print(f"✅ Total columns: {len(df_combined.columns)}")
            print(f"=" * 50)
            
            return df_combined
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail=f"Error parsing DATA sheets from '{file_name}': {str(e)}"
            )

    async def parse_and_validate_file(self, url_file: str, skiprows: int = 1, 
                                    include_data_sheets_only: bool = False,
                                    expected_columns: list = None) -> dict:
        """
        Parse file và validate cơ bản
        
        Args:
            url_file (str): URL của file
            skiprows (int): Số dòng bỏ qua
            include_data_sheets_only (bool): Chỉ đọc DATA sheets
            expected_columns (list): Danh sách các cột bắt buộc
            
        Returns:
            dict: Kết quả parse và validation
        """
        start_time = datetime.now()
        
        try:
            # Parse file
            df = await self.parse_file_from_url(url_file, skiprows, include_data_sheets_only)
            
            # Basic validation
            validation_results = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'column_names': list(df.columns),
                'has_data': len(df) > 0,
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
            }
            
            # Check expected columns if provided
            if expected_columns:
                missing_columns = [col for col in expected_columns if col not in df.columns]
                extra_columns = [col for col in df.columns if col not in expected_columns]
                
                validation_results.update({
                    'expected_columns': expected_columns,
                    'missing_columns': missing_columns,
                    'extra_columns': extra_columns,
                    'has_all_expected_columns': len(missing_columns) == 0
                })
            
            # Check for completely empty rows
            empty_rows = df.isnull().all(axis=1).sum()
            validation_results['empty_rows'] = empty_rows
            
            return {
                "status": True,
                "message": f"File parsed and validated successfully",
                "times_run": datetime.now() - start_time,
                "data": df,
                "validation": validation_results
            }
            
        except Exception as e:
            return {
                "status": False,
                "message": f"Error parsing file: {str(e)}",
                "times_run": datetime.now() - start_time,
                "data": None,
                "validation": None
            }

    async def _extract_request_data(self, request_body) -> dict:
        """Helper function để extract và validate request data"""
        try:
            return {
                "url": request_body.json_settings["url"],
                "nameFunc": request_body.json_settings["nameFunc"],
                "userName": request_body.json_settings["userName"],
                "nameProduct": request_body.json_settings["nameProduct"],
                "validStatus": request_body.json_settings.get("validStatus"),
                "templateName": request_body.json_settings.get("templateName")
            }
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")

    async def _parse_and_prepare_data(self, url: str) -> tuple[pd.DataFrame, dict]:
        """Helper function để parse file và chuẩn bị data"""
        if not url:
            raise HTTPException(status_code=400, detail="No file or URL provided")

        try:
            parse_result = await self.parse_and_validate_file(
                url, 
                skiprows=1, 
                include_data_sheets_only=True
            )
            
            if not parse_result["status"]:
                raise HTTPException(status_code=400, detail=parse_result["message"])
                
            return parse_result["data"], parse_result["validation"]
            
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")

    def _map_columns(self, df_import: pd.DataFrame, request_body) -> tuple[pd.DataFrame, dict]:
        """Helper function để map columns từ business names sang system names"""
        try:
            column_mapping = request_body.json_settings.get("setting_cols", [])
            system_name_cols = [col["standard_name"] for col in column_mapping]
            system_type_cols = [col.get("data_type") for col in column_mapping]
            business_name_cols = [col["import_name"] for col in column_mapping]

            # Check for duplicates in import_name
            duplicate_import_names = [
                name for name in set(business_name_cols) if business_name_cols.count(name) > 1
            ]
            if duplicate_import_names:
                raise HTTPException(
                    status_code=409,
                    detail=f"Vui lòng không nhập trùng tên (dòng 2): {', '.join(duplicate_import_names)}"
                )

            business_to_system = {
                col["import_name"]: col["standard_name"] for col in column_mapping
            }
            system_name_type = dict(zip(system_name_cols, system_type_cols)) if system_type_cols else {}

            if not system_name_cols:
                raise HTTPException(
                    status_code=409,
                    detail="Vui lòng lưu thiết lập đã chuẩn hoá trước khi import",
                )

            df_mapped = df_import.rename(columns=business_to_system)
            df_mapped = df_mapped[df_mapped.columns.intersection(business_to_system.values())]
            
            return df_mapped, {
                "system_name_cols": system_name_cols,
                "business_name_cols": business_name_cols,
                "system_name_type": system_name_type,
                "business_to_system": business_to_system
            }
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error mapping columns: {str(e)}")

    def _convert_column_types(self, df: pd.DataFrame, system_name_type: dict, for_parquet: bool = False) -> pd.DataFrame:
        """
        Helper function để convert column types theo settings với parquet optimization
        
        Args:
            df: DataFrame cần convert
            system_name_type: Dictionary mapping column -> data type
            for_parquet: Nếu True, sẽ optimize cho parquet compatibility
        """
        try:
            date_formats = [
                "%d/%m/%Y %H:%M",  # 01/01/2019 01:15
                "%d/%m/%Y",  # 01/01/2019
                "%Y-%m-%d %H:%M:%S",  # 2019-01-01 01:15:00
                "%Y-%m-%d",  # 2019-01-01
            ]

            df_converted = df.copy()
            
            print(f"📊 Converting column types (parquet_mode: {for_parquet}):")
            
            for col, dtype in system_name_type.items():
                if col not in df_converted.columns:
                    continue
                
                current_dtype = str(df_converted[col].dtype)
                    
                if dtype and dtype.lower() == "date":
                    # Convert to datetime
                    for date_format in date_formats:
                        try:
                            df_converted[col] = pd.to_datetime(
                                df_converted[col].astype(str).str.strip(),
                                format=date_format,
                                errors="raise",
                            )
                            break
                        except ValueError:
                            continue

                    if not pd.api.types.is_datetime64_any_dtype(df_converted[col]):
                        df_converted[col] = pd.to_datetime(df_converted[col], errors="coerce")

                    # CRITICAL: Remove timezone for parquet compatibility
                    if hasattr(df_converted[col], "dt") and df_converted[col].dt.tz:
                        df_converted[col] = df_converted[col].dt.tz_localize(None)

                    df_converted[col] = pd.to_datetime(df_converted[col].dt.strftime("%Y-%m-%d"))
                    print(f"✓ {col}: {current_dtype} → {df_converted[col].dtype} (date)")
                    
                elif dtype and dtype.lower() == "integer":
                    # Convert to numeric first
                    df_converted[col] = pd.to_numeric(df_converted[col], errors="coerce")
                    
                    if for_parquet:
                        # Handle NaN values for parquet (integers can't have NaN in parquet)
                        nan_count = df_converted[col].isnull().sum()
                        if nan_count > 0:
                            df_converted[col] = df_converted[col].fillna(-999)
                            print(f"⚠️ {col}: Filled {nan_count} NaN values with -999 for parquet")
                        
                        # Optimize integer type based on range
                        min_val = df_converted[col].min()
                        max_val = df_converted[col].max()
                        
                        if min_val >= 0:  # Unsigned
                            if max_val <= 255:
                                df_converted[col] = df_converted[col].astype('uint8')
                            elif max_val <= 65535:
                                df_converted[col] = df_converted[col].astype('uint16')
                            elif max_val <= 4294967295:
                                df_converted[col] = df_converted[col].astype('uint32')
                            else:
                                df_converted[col] = df_converted[col].astype('uint64')
                        else:  # Signed
                            if min_val >= -128 and max_val <= 127:
                                df_converted[col] = df_converted[col].astype('int8')
                            elif min_val >= -32768 and max_val <= 32767:
                                df_converted[col] = df_converted[col].astype('int16')
                            elif min_val >= -2147483648 and max_val <= 2147483647:
                                df_converted[col] = df_converted[col].astype('int32')
                            else:
                                df_converted[col] = df_converted[col].astype('int64')
                    else:
                        # Regular integer conversion (cho database)
                        df_converted[col] = pd.to_numeric(df_converted[col], errors="coerce")
                    
                    print(f"✓ {col}: {current_dtype} → {df_converted[col].dtype} (integer)")
                    
                elif dtype and dtype.lower() == "double":
                    df_converted[col] = pd.to_numeric(df_converted[col], errors="coerce")
                    
                    if for_parquet:
                        # Optimize float precision
                        if df_converted[col].max() < 3.4e38 and df_converted[col].min() > -3.4e38:
                            df_converted[col] = df_converted[col].astype('float32')
                        else:
                            df_converted[col] = df_converted[col].astype('float64')
                    
                    print(f"✓ {col}: {current_dtype} → {df_converted[col].dtype} (double)")
                    
                elif dtype and dtype.lower() == "text":
                    df_converted[col] = df_converted[col].astype(str).fillna("Unknown")
                    
                    if for_parquet:
                        # Check if should use category (low cardinality)
                        unique_ratio = df_converted[col].nunique() / len(df_converted[col])
                        if unique_ratio < 0.5:  # Less than 50% unique values
                            df_converted[col] = df_converted[col].astype('category')
                            print(f"✓ {col}: {current_dtype} → category (text with low cardinality)")
                        else:
                            # Use object dtype for better parquet compatibility (not 'string')
                            df_converted[col] = df_converted[col].astype('object')
                            print(f"✓ {col}: {current_dtype} → object (text)")
                    else:
                        print(f"✓ {col}: {current_dtype} → {df_converted[col].dtype} (text)")
            
            # Additional parquet compatibility fixes
            if for_parquet:
                print(f"\n📊 Final parquet compatibility check:")
                problematic_cols = []
                
                for col in df_converted.columns:
                    dtype_str = str(df_converted[col].dtype)
                    
                    # Fix problematic nullable integer types
                    if dtype_str in ['Int64', 'Int32', 'Int16', 'Int8', 'UInt64', 'UInt32', 'UInt16', 'UInt8']:
                        df_converted[col] = df_converted[col].fillna(-999).astype(dtype_str.lower())
                        problematic_cols.append((col, dtype_str, 'fixed'))
                    
                    # Fix string dtype
                    elif dtype_str == 'string':
                        df_converted[col] = df_converted[col].astype('object')
                        problematic_cols.append((col, dtype_str, 'fixed'))
                
                if problematic_cols:
                    print(f"⚠️ Fixed {len(problematic_cols)} parquet compatibility issues:")
                    for col, old_dtype, status in problematic_cols:
                        print(f"   - {col}: {old_dtype} → {df_converted[col].dtype}")
                else:
                    print(f"✅ All dtypes are parquet-compatible")
                
                # Memory optimization info
                original_memory = df.memory_usage(deep=True).sum()
                optimized_memory = df_converted.memory_usage(deep=True).sum()
                reduction_pct = ((original_memory - optimized_memory) / original_memory) * 100
                
                print(f"📊 Memory optimization:")
                print(f"📊 Original: {original_memory / 1024 / 1024:.2f} MB")
                print(f"📊 Optimized: {optimized_memory / 1024 / 1024:.2f} MB")
                print(f"📊 Reduction: {reduction_pct:.1f}%")
                    
            return df_converted
            
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error converting columns: {str(e)}")

    def _validate_data_by_function(self, df: pd.DataFrame, nameFunc: str, request_json: dict) -> tuple[bool, str, dict]:
        """Helper function để validate data theo loại function"""
        try:
            validation_results = None

            if nameFunc == "GLM_CLM":
                validation_results = analyze_dataframe_claim(df, request_json)
            elif nameFunc == "GLM_GWP":
                validation_results = analyze_dataframe_gwp(df, request_json)
            elif nameFunc == "GLM_CMB":
                validation_results = analyze_dataframe_combine(df, request_json)
            else:
                raise HTTPException(
                    status_code=400, detail=f"Invalid file type: {nameFunc}"
                )

            if not validation_results:
                raise HTTPException(
                    status_code=500, detail="Validation produced no results"
                )

            # Transform dataframe summary to list format
            validation_results[0]["dataframe_summary"] = [
                {"Column": col, **details}
                for col, details in validation_results[0]["dataframe_summary"].items()
            ]

            # Check validation results
            has_type_errors = len(validation_results[0]["error_details"]["type_check"]) > 0
            has_missing_errors = len(validation_results[0]["error_details"]["missing_check"]) > 0
            has_duplicate_errors = (
                "dup_check" in validation_results[0]["error_details"]
                and len(validation_results[0]["error_details"]["dup_check"]) > 0
            )

            # Set status and message based on validation results
            if not (has_type_errors or has_missing_errors or has_duplicate_errors):
                status = True
                message = "Data validation completed successfully"
            else:
                status = False
                message = "Data validation failed !"
                if has_type_errors:
                    message += "\n Due to wrong type: " + ", ".join(
                        [str(item) for item in validation_results[0]["error_details"]["type_check"]]
                    )
                elif has_missing_errors:
                    message += "\n Due to missing: " + ", ".join(
                        [str(item) for item in validation_results[0]["error_details"]["missing_check"]]
                    )
                else:
                    message += "\n Due to duplicate: " + ", ".join(
                        [str(item) for item in validation_results[0]["error_details"]["dup_check"]]
                    )

            return status, message, validation_results[0]
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error during validation: {str(e)}")

    def _generate_table_name_and_s3_key(self, url: str, userName: str, nameFunc: str, nameProduct: str, templateName: str) -> tuple[str, str, str]:
        """Helper function để generate table name và S3 key"""
        try:
            match = re.search(r"_([0-9]{14})\.[^.]+$", os.path.basename(url))
            if match:
                number_part = match.group(1)
                table_name = f"{userName}_{templateName}_IMPORT_{number_part}"
            else:
                table_name = (
                    userName
                    + "_"
                    + nameFunc
                    + "_"
                    + nameProduct
                    + "_"
                    + templateName.split("_")[-1]
                    + "_IMPORT_"
                    + datetime.now().strftime("%Y%m%d%H%M%S")
                )
            
            file_name = f"{table_name}.parquet"
            s3_key = f"report-software/mof/{userName}/analysis_data/{file_name}"
            
            return table_name, file_name, s3_key
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error generating table name: {str(e)}")

    def _extract_additional_vars_ac(self, request_body) -> dict:
        """
        Helper function để extract additional codes và descriptions từ VARS_AC variables

        Args:
            request_body: Request body chứa json_settings với setting_cols

        Returns:
            dict: Dictionary mapping AC codes to descriptions
                  Trả về None nếu không có VARS_AC variables
                  Format: {"AC01": "QB01: Tử vong do Bệnh", "AC02": "QL01: Tử vong do tai nạn", ...}
                  Loại bỏ các trường hợp key == value (ví dụ: "AC09": "AC09")
        """
        try:
            column_mapping = request_body.json_settings.get("setting_cols", [])

            # Filter VARS_AC variables
            vars_ac_items = [
                col for col in column_mapping 
                if col.get("variable_type") == "VARS_AC"
            ]

            if not vars_ac_items:
                return None

            # Extract mapping AC codes to descriptions
            ac_mapping = {}

            for item in vars_ac_items:
                standard_name = item.get("standard_name", "")
                import_name = item.get("import_name", "")

                # Extract AC code from standard_name (e.g., "NUM_CLAIMS_AC01" -> "AC01")
                if standard_name:
                    ac_match = re.search(r'(AC\d+)', standard_name)
                    if ac_match:
                        ac_code = ac_match.group(1)  # "AC01", "AC09", etc.
                        
                        # Determine description based on import_name
                        if import_name:
                            # Case 1: import_name có format "NUM_CLAIMS_QB01: Tử vong do Bệnh" hoặc "CLAIM_PMT_QB01: Tử vong do Bệnh"
                            if ":" in import_name:
                                # Tìm vị trí của dấu ":"
                                colon_index = import_name.find(":")
                                prefix_part = import_name[:colon_index]  # "NUM_CLAIMS_QB01" hoặc "CLAIM_PMT_QB01"
                                suffix_part = import_name[colon_index:]  # ": Tử vong do Bệnh"
                                
                                # Extract code part từ prefix, loại bỏ NUM_CLAIMS_ hoặc CLAIM_PMT_
                                # Pattern để match: loại bỏ NUM_CLAIMS_ hoặc CLAIM_PMT_ ở đầu
                                code_part = re.sub(r'^(NUM_CLAIMS_|CLAIM_PMT_)', '', prefix_part)
                                description = f"{code_part}{suffix_part}"  # "QB01: Tử vong do Bệnh"
                            
                            # Case 2: import_name có format "NUM_CLAIMS_AUU001.AAA" hoặc "CLAIM_PMT_AUU001.AAA"
                            else:
                                # Loại bỏ NUM_CLAIMS_ hoặc CLAIM_PMT_ prefix để lấy code part
                                description = re.sub(r'^(NUM_CLAIMS_|CLAIM_PMT_)', '', import_name)  # "AUU001.AAA"
                        else:
                            # Fallback: Sử dụng AC code làm description
                            description = ac_code
                        
                        # Chỉ add vào mapping nếu key != value
                        if ac_code != description:
                            ac_mapping[ac_code] = description

            # Return sorted dictionary if we have data
            if ac_mapping:
                return dict(sorted(ac_mapping.items()))
            else:
                return None

        except Exception as e:
            print(f"Warning: Could not extract additional codes/descriptions: {str(e)}")
            return None

############### API Services ###############

    async def glm_valid_data(self, request_body):
        start_time = datetime.now()

        # Extract and validate request data
        request_data = await self._extract_request_data(request_body)
        
        # Parse file and prepare data
        df_import, validation_info = await self._parse_and_prepare_data(request_data["url"])
        
        # Map columns
        df_mapped, mapping_info = self._map_columns(df_import, request_body)
        
        # Validate data
        if not (mapping_info["system_name_cols"] and mapping_info["business_name_cols"]):
            raise HTTPException(
                status_code=400, detail="No validation settings provided"
            )

        rq2json = json.dumps(request_body.model_dump(), ensure_ascii=False, indent=4)
        status, message, validation_results = self._validate_data_by_function(
            df_mapped, request_data["nameFunc"], json.loads(rq2json)
        )

        # Serialize validation results
        updated_json_data = json.dumps(validation_results, cls=NpEncoder, indent=4)

        # Return response
        return {
            "isValidated": status,
            "times_run": datetime.now() - start_time,
            "message": message,
            "data": json.loads(updated_json_data) if status else None,
        }

    async def glm_import_data_after_mapping(self, request_body):
        start_time = datetime.now()

        # Extract and validate request data
        request_data = await self._extract_request_data(request_body)
        
        if request_data["validStatus"] == "Validated":
            raise HTTPException(status_code=400, detail="Data has been validated")
        
        # Parse file and prepare data
        df_import, validation_info = await self._parse_and_prepare_data(request_data["url"])
        
        # Map columns
        df_mapped, mapping_info = self._map_columns(df_import, request_body)
        
        # Convert columns based on settings
        df_converted = self._convert_column_types(df_mapped, mapping_info["system_name_type"], for_parquet=True)

        # Extract additional codes mapping from VARS_AC variables
        list_additional = self._extract_additional_vars_ac(request_body)

        # Generate table name and S3 key
        table_name, file_name, s3_key = self._generate_table_name_and_s3_key(
            request_data["url"],
            request_data["userName"],
            request_data["nameFunc"],
            request_data["nameProduct"],
            request_data["templateName"]
        )

        # Save parquet file to S3 with optimized settings
        try:
            buffer = io.BytesIO()
            # Use pyarrow with specific settings for better compatibility
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
                
                # Create Arrow table
                table = pa.Table.from_pandas(df_converted, preserve_index=False)
                
                # Write with optimized settings
                pq.write_table(
                    table, 
                    buffer,
                    compression='snappy',
                    use_dictionary=True,
                    row_group_size=50000,
                    use_deprecated_int96_timestamps=False,
                    coerce_timestamps='ms',
                    store_schema=True
                )
                print(f"✅ Parquet saved with pyarrow optimization")
                
            except Exception as arrow_error:
                print(f"⚠️ Arrow failed, using pandas fallback: {arrow_error}")
                # Fallback to pandas
                df_converted.to_parquet(buffer, index=False, engine='pyarrow')
            
            buffer.seek(0)
            upload_to_s3(
                bucket=cfg["BUCKET"],
                key=s3_key,
                data_bytes=buffer.getvalue(),
                mimetype="application/octet-stream"
            )
            
            print(f"✅ Parquet file uploaded: {len(buffer.getvalue()) / 1024 / 1024:.2f} MB")
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error uploading parquet to S3: {str(e)}")

        # Return response
        return {
            "status": True,
            "times_run": datetime.now() - start_time,
            "message": f"Table '{table_name}' created and data inserted successfully",
            "data": {
                "s3_bucket": cfg["BUCKET"],
                "s3_key": s3_key,
                "file_name": file_name,
                "list_additional": list_additional,
            },
        }

class GLMAnalysis:
    """Class để xử lý phân tích GLM từ file parquet đã được import"""
    
    def __init__(self):
        pass

    def _extract_and_validate_request(self, request_body):
        """Đoạn 1: Đọc JSON và validate"""
        try:
            # Extract basic info
            parquet_url = request_body.json_settings.tableName
            user_name = request_body.json_settings.userName
            product_name = request_body.json_settings.nameProduct
            name_func = request_body.json_settings.nameFunc
            valid_status = request_body.json_settings.validStatus

            # Validate status
            if valid_status != "Validated":
                raise HTTPException(
                    status_code=409,
                    detail="Data needs to be validated before processing",
                )

            # Extract settings
            var_single_cols = request_body.json_settings.setting_cols.var_single_settings
            var_category_settings = request_body.json_settings.setting_cols.var_cate_settings
            var_cal_year = request_body.json_settings.setting_cols.calYear
            var_info = request_body.json_settings.setting_cols.var_info_settings
            var_additional_settings = request_body.json_settings.setting_cols.var_additional_settings

            # Process category column names
            var_category_cols = [list(item.keys())[0] for item in var_category_settings]
            var_bf_category_cols = [col.split("_GROUP")[0] for col in var_category_cols]
            
            # process additional settings if present
            if var_additional_settings:
                additional_apply = True
                additional_codes = list(var_additional_settings.keys()) # Lấy danh sách mã (keys)
                additional_descriptions = list(var_additional_settings.values()) # Lấy danh sách mô tả (values)
            else:
                additional_apply = False
                additional_codes = None
                additional_descriptions = None

            return {
                'parquet_url': parquet_url,
                'user_name': user_name,
                'product_name': product_name,
                'name_func': name_func,
                'var_single_cols': var_single_cols,
                'var_category_settings': var_category_settings,
                'var_category_cols': var_category_cols,
                'var_bf_category_cols': var_bf_category_cols,
                'var_cal_year': var_cal_year,
                'var_info': var_info,
                'var_additional_settings': var_additional_settings,
                'additional_apply': additional_apply,
                'additional_codes': additional_codes,
                'additional_descriptions': additional_descriptions
            }
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error processing request: {str(e)}")

    def _read_parquet_data(self, parquet_url: str, var_info: list, var_bf_category_cols: list, 
                          var_single_cols: list, additional_apply: bool, additional_codes: list):
        """Đoạn 2: Đọc file parquet"""
        try:
            # Đọc trực tiếp từ URL signed
            df = pd.read_parquet(parquet_url)
            
            # Select only required columns
            if additional_apply:
                list_add = [f"{prefix}_{code}" for code in additional_codes for prefix in ["NUM_CLAIMS", "CLAIM_PMT"]]
                df = df[var_info + var_bf_category_cols + var_single_cols + list_add]
            else:
                df = df[var_info + var_bf_category_cols + var_single_cols]
            return df
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error reading parquet file from URL: {str(e)}")

    def _process_category_columns(self, df: pd.DataFrame, var_category_settings: list, product_name: str):
        """Đoạn 3: Xử lý category columns"""
        try:
            df_processed = pd.DataFrame()
            for setting in var_category_settings:
                col = list(setting.keys())[0]  # Get column name
                var_settings = setting[col]  # Get settings for this column

                bins = var_settings.bin
                units = var_settings.unit
                # Convert infinity strings to float values
                bins = [float("inf") if x == "Infinity" else float(x) for x in bins]

                # Get original column name without _GROUP suffix
                col_import = col.split("_GROUP")[0]
                df[col] = df[col_import]
                if product_name == "HEALTH":
                    df_processed = categorize_health(df, col, bins, units)
                elif product_name == "CAR":
                    df_processed = categorize_car(df, col, bins, units)

            return df_processed
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error processing category columns: {str(e)}")

    def _generate_table_name_and_save(self, df_final: pd.DataFrame, parquet_url: str, 
                                     user_name: str, name_func: str, product_name: str, 
                                     analysis_type: str, additional_apply: bool, additional_codes: str):
        """Đoạn 5: Đặt tên và lưu database/S3"""
        try:
            # Extract timestamp từ URL
            from urllib.parse import urlparse
            
            parsed_url = urlparse(parquet_url)
            file_path = parsed_url.path
            
            # Extract timestamp từ tên file
            match = re.search(r"_([0-9]{14})\.parquet", file_path)
            timestamp = match.group(1)
            
            if additional_apply:
                table_detail_name = f"{user_name}_{name_func}_{product_name}_{additional_codes}_{analysis_type}_{timestamp}"
            else:
                table_detail_name = f"{user_name}_{name_func}_{product_name}_AC00_{analysis_type}_{timestamp}"
            sub_folder = f"report-software/glm/{user_name}/analysis_data"
            s3_key = f"{sub_folder}/{table_detail_name}.parquet"

            # Lưu file parquet lên S3
            buffer = io.BytesIO()
            df_final.to_parquet(buffer, index=False)
            buffer.seek(0)
            upload_to_s3(
                bucket=cfg["BUCKET"],
                key=s3_key,
                data_bytes=buffer.getvalue(),
                mimetype="application/octet-stream"
            )

            return {
                'table_detail_name': table_detail_name,
                's3_key': s3_key,
                's3_bucket': cfg["BUCKET"],
                'sub_folder': sub_folder
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error generating table name or saving: {str(e)}")

    def _process_analysis_generic(self, df_processed, request_data, analysis_func, var_combinations, add_codes=None, add_desc=None):
        """Generic helper function để xử lý các loại analysis khác nhau"""
        df_final = pd.DataFrame()
        idx = 1

        # Xử lý var_cal_year để tạo range nếu cần
        var_cal_year = request_data['var_cal_year']
        
        if len(var_cal_year) == 2:
            if var_cal_year[0] < var_cal_year[1]:
                # Trường hợp [2015, 2019] → tạo range [2015, 2016, 2017, 2018, 2019]
                unique_years = list(range(var_cal_year[0], var_cal_year[1] + 1))
            else:
                # Trường hợp [2015, 2015] → giữ nguyên [2015]
                unique_years = [var_cal_year[0]]
        else:
            # Các trường hợp khác (không nên xảy ra theo yêu cầu)
            unique_years = list(set(var_cal_year))

        for combination in var_combinations:
            for pol_year in unique_years:
                try:
                    var_code = str(idx).zfill(3)
                    dftemp = analysis_func(
                        pol_year,
                        df_processed,
                        combination,
                        var_code,
                        request_data,
                        add_codes,
                        add_desc
                    )
                    df_final = pd.concat([df_final, dftemp], axis=0)
                    
                except Exception as e:
                    combination_str = combination if isinstance(combination, str) else " & ".join(combination)
                    raise HTTPException(
                        status_code=409,
                        detail=f"Error in combination '{combination_str}' for year '{pol_year}': {str(e)}"
                    )
            idx += 1
        
        return df_final

    def _call_owa_func(self, pol_year, df_processed, combination, var_code, request_data, add_codes=None, add_desc=None):
        """Helper function để gọi OWA_func"""
        return OWA_func(
            int(pol_year),
            df_processed,
            combination,  # single column
            var_code,
            request_data['product_name'],
            request_data['additional_apply'],
            add_codes,
            add_desc
        )

    def _call_twa_func(self, pol_year, df_processed, combination, var_code, request_data, add_codes=None, add_desc=None):
        """Helper function để gọi TWA_func"""
        return TWA_func(
            int(pol_year),
            df_processed,
            combination[0],  # first column
            combination[1],  # second column
            var_code,
            request_data['product_name'],
            request_data['additional_apply'] if request_data['additional_apply'] else False,
            add_codes or "",
            add_desc or ""
        )

    def _call_threeway_func(self, pol_year, df_processed, combination, var_code, request_data, add_codes=None, add_desc=None):
        """Helper function để gọi threeway_func"""
        return threeway_func(
            int(pol_year),
            df_processed,
            combination[0],  # first column
            combination[1],  # second column
            combination[2],  # third column
            var_code,
            request_data['product_name'],
            request_data['additional_apply'] if request_data['additional_apply'] else False,
            add_codes or "",
            add_desc or ""
        )

    def _call_fourway_func(self, pol_year, df_processed, combination, var_code, request_data, add_codes=None, add_desc=None):
        """Helper function để gọi fourway_func"""
        return fourway_func(
            int(pol_year),
            df_processed,
            combination[0],  # first column
            combination[1],  # second column
            combination[2],  # third column
            combination[3],  # fourth column
            var_code,
            request_data['product_name'],
            request_data['additional_apply'] if request_data['additional_apply'] else False,
            add_codes or "",
            add_desc or ""
        )

    def _generate_combinations(self, var_cols, n_way):
        """Helper function để tạo các combinations cho analysis"""
        from itertools import combinations
        
        if n_way == 1:
            return var_cols
        else:
            return list(combinations(var_cols, n_way))

    def _process_multiple_codes_analysis(self, df_processed, request_data, analysis_func, var_combinations, analysis_type):
        """Helper function để xử lý analysis với multiple additional codes"""
        table_detail = []
        
        if request_data['additional_apply'] and request_data['additional_codes']:
            # Có additional codes
            for add_codes, add_desc in zip(request_data['additional_codes'], request_data['additional_descriptions']):
                df_final = self._process_analysis_generic(
                    df_processed, request_data, analysis_func, var_combinations, add_codes, add_desc
                )
                
                save_result = self._generate_table_name_and_save(
                    df_final,
                    request_data['parquet_url'],
                    request_data['user_name'],
                    request_data['name_func'],
                    request_data['product_name'],
                    analysis_type,
                    request_data['additional_apply'],
                    add_codes
                )
                table_detail.append(save_result['table_detail_name'])

            # Thêm bảng tổng hợp cho ALLBENE
            df_final = self._process_analysis_generic(
                df_processed, request_data, analysis_func, var_combinations, None, None
            )

            save_result = self._generate_table_name_and_save(
                df_final,
                request_data['parquet_url'],
                request_data['user_name'],
                request_data['name_func'],
                request_data['product_name'],
                analysis_type,
                False,
                ""
            )
            table_detail.append(save_result['table_detail_name'])
        else:
            # Không có additional codes
            df_final = self._process_analysis_generic(
                df_processed, request_data, analysis_func, var_combinations
            )
            
            save_result = self._generate_table_name_and_save(
                df_final,
                request_data['parquet_url'],
                request_data['user_name'],
                request_data['name_func'],
                request_data['product_name'],
                analysis_type,
                False,
                ""
            )
            table_detail.append(save_result['table_detail_name'])

        return table_detail, save_result['s3_key'], save_result['sub_folder']

    # Main service methods
    async def glm_1wa(self, request_body):
        """GLM 1-Way Analysis"""
        start_time = datetime.now()

        # Đoạn 1: Extract và validate request
        request_data = self._extract_and_validate_request(request_body)

        # Đoạn 2: Đọc file parquet
        df = self._read_parquet_data(
            request_data['parquet_url'],
            request_data['var_info'],
            request_data['var_bf_category_cols'],
            request_data['var_single_cols'],
            request_data['additional_apply'],
            request_data['additional_codes']
        )

        # Đoạn 3: Xử lý category columns
        df_processed = self._process_category_columns(df, request_data['var_category_settings'], request_data['product_name'])

        # Đoạn 4: Tạo combinations và perform analysis
        var_cols = list(request_data['var_category_cols']) + request_data['var_single_cols']
        var_combinations = self._generate_combinations(var_cols, 1)
        
        table_detail, s3_key, sub_folder = self._process_multiple_codes_analysis(
            df_processed, request_data, self._call_owa_func, var_combinations, "1WA"
        )

        # Return response
        return {
            "status": True,
            "times_run": datetime.now() - start_time,
            "message": f"Tables created and data inserted successfully",
            "data": {
                "s3_bucket": cfg["BUCKET"],
                "s3_key": s3_key,
                "sub_folder": sub_folder,
                "table_detail_name": table_detail,
            }
        }

    async def glm_2wa(self, request_body):
        """GLM 2-Way Analysis"""
        start_time = datetime.now()

        # Đoạn 1: Extract và validate request
        request_data = self._extract_and_validate_request(request_body)

        # Đoạn 2: Đọc file parquet
        df = self._read_parquet_data(
            request_data['parquet_url'],
            request_data['var_info'],
            request_data['var_bf_category_cols'],
            request_data['var_single_cols'],
            request_data['additional_apply'],
            request_data['additional_codes']
        )

        # Đoạn 3: Xử lý category columns
        df_processed = self._process_category_columns(df, request_data['var_category_settings'], request_data['product_name'])

        # Đoạn 4: Tạo combinations và perform analysis
        var_cols = list(request_data['var_category_cols']) + request_data['var_single_cols']
        var_combinations = self._generate_combinations(var_cols, 2)
        
        table_detail, s3_key, sub_folder = self._process_multiple_codes_analysis(
            df_processed, request_data, self._call_twa_func, var_combinations, "2WA"
        )

        # Return response
        return {
            "status": True,
            "times_run": datetime.now() - start_time,
            "message": f"Tables created and data inserted successfully",
            "data": {
                "s3_bucket": cfg["BUCKET"],
                "s3_key": s3_key,
                "sub_folder": sub_folder,
                "table_detail_name": table_detail,
            }
        }

    async def glm_3wa(self, request_body):
        """GLM 3-Way Analysis"""
        start_time = datetime.now()

        # Đoạn 1: Extract và validate request
        request_data = self._extract_and_validate_request(request_body)
        
        # Lấy selected columns cho 3-way analysis từ options
        if not request_body.json_settings.setting_cols.options:
            raise HTTPException(status_code=409, detail="3-way analysis requires 'options' field in setting_cols")
        
        options = request_body.json_settings.setting_cols.options
        
        # Validate có đúng 3 variables
        required_vars = ['VAR_NAME_1', 'VAR_NAME_2', 'VAR_NAME_3']
        missing_vars = [var for var in required_vars if var not in options]
        if missing_vars:
            raise HTTPException(
                status_code=409, 
                detail=f"3-way analysis requires variables: {', '.join(required_vars)}. Missing: {', '.join(missing_vars)}"
            )
        
        list_var_selected = [options[var].col for var in required_vars]

        # Đoạn 2: Đọc file parquet
        df = self._read_parquet_data(
            request_data['parquet_url'],
            request_data['var_info'],
            request_data['var_bf_category_cols'],
            request_data['var_single_cols'],
            request_data['additional_apply'],
            request_data['additional_codes']
        )

        # Đoạn 3: Xử lý category columns (chỉ cho selected columns)
        df_processed = self._process_category_columns(df, 
            [setting for setting in request_data['var_category_settings'] 
            if list(setting.keys())[0] in list_var_selected], request_data['product_name']
        )

        # Đoạn 4: Perform analysis
        var_combinations = [list_var_selected]  # Only one combination
        
        table_detail, s3_key, sub_folder = self._process_multiple_codes_analysis(
            df_processed, request_data, self._call_threeway_func, var_combinations, "3WA"
        )

        # Return response
        return {
            "status": True,
            "times_run": datetime.now() - start_time,
            "message": f"Tables created and data inserted successfully",
            "data": {
                "s3_bucket": cfg["BUCKET"],
                "s3_key": s3_key,
                "sub_folder": sub_folder,
                "table_detail_name": table_detail,
            }
        }

    async def glm_4wa(self, request_body):
        """GLM 4-Way Analysis"""
        start_time = datetime.now()

        # Đoạn 1: Extract và validate request
        request_data = self._extract_and_validate_request(request_body)
        
        # Lấy selected columns cho 4-way analysis từ options
        if not request_body.json_settings.setting_cols.options:
            raise HTTPException(status_code=409, detail="4-way analysis requires 'options' field in setting_cols")
        
        options = request_body.json_settings.setting_cols.options
        
        # Validate có đúng 4 variables
        required_vars = ['VAR_NAME_1', 'VAR_NAME_2', 'VAR_NAME_3', 'VAR_NAME_4']
        missing_vars = [var for var in required_vars if var not in options]
        if missing_vars:
            raise HTTPException(
                status_code=409, 
                detail=f"4-way analysis requires variables: {', '.join(required_vars)}. Missing: {', '.join(missing_vars)}"
            )
        
        list_var_selected = [options[var].col for var in required_vars]

        # Đoạn 2: Đọc file parquet
        df = self._read_parquet_data(
            request_data['parquet_url'],
            request_data['var_info'],
            request_data['var_bf_category_cols'],
            request_data['var_single_cols'],
            request_data['additional_apply'],
            request_data['additional_codes']
        )

        # Đoạn 3: Xử lý category columns (chỉ cho selected columns)
        df_processed = self._process_category_columns(df, 
            [setting for setting in request_data['var_category_settings'] 
            if list(setting.keys())[0] in list_var_selected], request_data['product_name']
        )

        # Đoạn 4: Perform analysis
        var_combinations = [list_var_selected]  # Only one combination
        
        table_detail, s3_key, sub_folder = self._process_multiple_codes_analysis(
            df_processed, request_data, self._call_fourway_func, var_combinations, "4WA"
        )

        # Return response
        return {
            "status": True,
            "times_run": datetime.now() - start_time,
            "message": f"Tables created and data inserted successfully",
            "data": {
                "s3_bucket": cfg["BUCKET"],
                "s3_key": s3_key,
                "sub_folder": sub_folder,
                "table_detail_name": table_detail,
            }
        }
