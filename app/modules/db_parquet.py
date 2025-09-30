import boto3
from botocore.client import Config
import io
from config.constants import S3
import pandas as pd
from urllib.parse import urlparse
from config.google_sheets_config import google_sheets_config
from config.log_config import logger
from typing import Any, Dict, Optional
from datetime import datetime

cfg = S3

def upload_parquet_to_s3_buffer(df, object_name):

    s3 = boto3.client(
        "s3",
        region_name=cfg["REGION"],
        endpoint_url=cfg["ENDPOINT"],
        aws_access_key_id=cfg["ACCESS_KEY"],
        aws_secret_access_key=cfg["SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, cfg["BUCKET"], object_name)
    print(f"Uploaded DataFrame to s3://{cfg['BUCKET']}/{object_name}")

def upload_to_s3(bucket, key, data_bytes, mimetype):
    try:
        s3 = boto3.client(
            "s3",
            region_name=cfg["REGION"],
            endpoint_url=cfg["ENDPOINT"],
            aws_access_key_id=cfg["ACCESS_KEY"],
            aws_secret_access_key=cfg["SECRET_KEY"],
            config=Config(s3={"addressing_style": "path"}),
        )
        s3.put_object(Bucket=bucket, Key=key, Body=data_bytes, ContentType=mimetype)
        file_url = f"{cfg['ENDPOINT'].rstrip('/')}/{bucket}/{key}"
        return file_url
    except Exception as e:
        raise RuntimeError(f"Failed to upload file to S3: {e}")

def read_parquet_from_s3(key):
    """
    Đọc file parquet từ S3 về DataFrame sử dụng cấu hình trong cfg.
    :param key: Đường dẫn (key) của file trên S3 bucket.
    :return: pandas.DataFrame
    """
    s3_url = f"s3://{cfg['BUCKET']}/{key}"
    storage_options = {
        "key": cfg["ACCESS_KEY"],
        "secret": cfg["SECRET_KEY"],
        "client_kwargs": {"endpoint_url": cfg["ENDPOINT"]},
    }
    df = pd.read_parquet(s3_url, storage_options=storage_options)
    return df

def extract_parquet_key(parquet_url: str) -> str:
    parsed = urlparse(parquet_url)
    # Nếu path bắt đầu bằng /<bucket>/ thì bỏ luôn cả bucket
    path = parsed.path.lstrip('/')
    bucket = cfg["BUCKET"]
    if path.startswith(bucket + "/"):
        path = path[len(bucket) + 1 :]
    return path

class GoogleSheetToS3Service:
    def __init__(self):
        self.sheets_client = google_sheets_config.get_client()
        self.bucket_name = S3["BUCKET"]
    
    async def read_sheet_data(self, sheet_id: str, worksheet_name: str = "DATA") -> pd.DataFrame:
        """Đọc data từ Google Sheets worksheet"""
        try:
            logger.info(f"Reading data from sheet {sheet_id}, worksheet: {worksheet_name}")
            
            # Mở Google Sheet
            sheet = self.sheets_client.open_by_key(sheet_id)
            worksheet = sheet.worksheet(worksheet_name)
            
            # Lấy all records
            records = worksheet.get_all_records()
            
            if not records:
                logger.warning(f"No data found in worksheet {worksheet_name}")
                return pd.DataFrame()
            
            # Convert sang DataFrame
            df = pd.DataFrame(records)
            
            logger.info(f"Successfully read {len(df)} rows and {len(df.columns)} columns")
            logger.info(f"Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading sheet data: {e}")
            raise
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Làm sạch DataFrame trước khi lưu"""
        try:
            # Remove empty rows
            df = df.dropna(how='all')
            
            # Remove empty columns
            df = df.dropna(axis=1, how='all')
            
            # Convert data types if needed
            for col in df.columns:
                # Try to convert numeric columns
                if df[col].dtype == 'object':
                    # Check if column contains numeric data
                    numeric_data = pd.to_numeric(df[col], errors='coerce')
                    if not numeric_data.isna().all():
                        df[col] = numeric_data
                
                # Handle date columns if any
                if 'date' in col.lower() or 'time' in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except:
                        pass
            
            logger.info(f"DataFrame cleaned: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error cleaning DataFrame: {e}")
            return df
    
    async def save_to_s3_parquet(self, df: pd.DataFrame, s3_key: str) -> Dict[str, Any]:
        """Lưu DataFrame vào S3 dưới dạng parquet sử dụng db_parquet module"""
        try:
            logger.info(f"Saving DataFrame to S3: s3://{self.bucket_name}/{s3_key}")
            
            # Sử dụng function từ db_parquet.py
            upload_parquet_to_s3_buffer(df, s3_key)
            
            # Tính toán file size (estimate từ memory usage)
            file_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
            
            logger.info(f"Successfully uploaded parquet file: ~{file_size_mb:.2f} MB")
            
            return {
                'status': 'success',
                'bucket': self.bucket_name,
                's3_key': s3_key,
                'file_size_mb': round(file_size_mb, 2),
                'rows': len(df),
                'columns': len(df.columns),
                's3_url': f's3://{self.bucket_name}/{s3_key}',
                'public_url': f"{S3['ENDPOINT'].rstrip('/')}/{self.bucket_name}/{s3_key}",
                'upload_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error saving to S3: {e}")
            raise
    
    async def read_from_s3_parquet(self, s3_key: str) -> pd.DataFrame:
        """Đọc parquet file từ S3 về DataFrame"""
        try:
            logger.info(f"Reading parquet from S3: s3://{self.bucket_name}/{s3_key}")
            
            # Sử dụng function từ db_parquet.py
            df = read_parquet_from_s3(s3_key)
            
            logger.info(f"Successfully read {len(df)} rows and {len(df.columns)} columns from S3")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from S3: {e}")
            raise
    
    async def process_sheet_to_s3(self, sheet_id: str, 
                                 worksheet_name: str = "DATA",
                                 s3_prefix: str = "google-sheets-data",
                                 filename_prefix: str = "sheet_data") -> Dict[str, Any]:
        """Complete pipeline: Google Sheets -> Cleaning -> S3 Parquet"""
        try:
            logger.info(f"Starting pipeline: Sheet {sheet_id} -> S3")
            
            # 1. Read data from Google Sheets
            df = await self.read_sheet_data(sheet_id, worksheet_name)
            
            if df.empty:
                return {
                    'status': 'warning',
                    'message': 'No data found in the worksheet'
                }
            
            # 2. Clean data
            df_cleaned = self.clean_dataframe(df)
            
            # 3. Generate S3 key with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{s3_prefix}/{filename_prefix}_{timestamp}.parquet"
            
            # 4. Save to S3
            result = await self.save_to_s3_parquet(df_cleaned, s3_key)
            
            logger.info("Pipeline completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    async def sync_sheet_to_s3_and_compare(self, sheet_id: str, 
                                          existing_s3_key: Optional[str] = None,
                                          worksheet_name: str = "DATA",
                                          s3_prefix: str = "google-sheets-data") -> Dict[str, Any]:
        """So sánh data hiện tại với data trước đó và chỉ upload nếu có thay đổi"""
        try:
            logger.info(f"Starting sync with comparison: Sheet {sheet_id}")
            
            # 1. Read current sheet data
            current_df = await self.read_sheet_data(sheet_id, worksheet_name)
            current_df_cleaned = self.clean_dataframe(current_df)
            
            if current_df_cleaned.empty:
                return {
                    'status': 'warning',
                    'message': 'No data found in the worksheet'
                }
            
            # 2. Compare with existing data if provided
            if existing_s3_key:
                try:
                    previous_df = await self.read_from_s3_parquet(existing_s3_key)
                    
                    # Compare DataFrames
                    if current_df_cleaned.equals(previous_df):
                        return {
                            'status': 'no_change',
                            'message': 'No changes detected, skipping upload',
                            'existing_s3_key': existing_s3_key,
                            'rows': len(current_df_cleaned)
                        }
                except Exception as e:
                    logger.warning(f"Could not read existing file for comparison: {e}")
            
            # 3. Upload new data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{s3_prefix}/sheet_data_{timestamp}.parquet"
            
            result = await self.save_to_s3_parquet(current_df_cleaned, s3_key)
            result['comparison'] = {
                'previous_file': existing_s3_key,
                'changes_detected': True
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Sync with comparison failed: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

# Service instance
google_sheet_to_s3_service = GoogleSheetToS3Service()