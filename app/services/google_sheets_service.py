from modules.db_parquet import cfg, upload_to_s3
from config.google_sheets_config import google_sheets_config
from fastapi import HTTPException
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime
import gspread

class GoogleSheetsService:
    def __init__(self):
        # Sử dụng config đã được thiết lập
        self.config = google_sheets_config

    async def sheet_to_s3(self, request):
        try:
            # Lấy thông tin từ Pydantic model
            spreadsheet_id = request.sheet_id
            worksheet_name = request.worksheet_name
            
            # Tạo s3_key từ prefix và filename_prefix
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            s3_key = f"{request.s3_prefix}/{request.filename_prefix}_{timestamp}.parquet"
            
            if not spreadsheet_id:
                raise HTTPException(status_code=400, detail="sheet_id is required")
            
            # Debug: In thông tin
            print(f"Trying to access sheet ID: {spreadsheet_id}")
            print(f"Worksheet name: {worksheet_name}")
            
            # Sử dụng client từ config
            gc = self.config.get_client()
            
            # Mở Google Sheet
            spreadsheet = gc.open_by_key(spreadsheet_id)
            print(f"Successfully opened spreadsheet: {spreadsheet.title}")
            
            worksheet = spreadsheet.worksheet(worksheet_name)
            print(f"Successfully accessed worksheet: {worksheet.title}")
            
            # Đọc dữ liệu từ sheet
            data = worksheet.get_all_records()
            
            if not data:
                raise HTTPException(status_code=404, detail="No data found in the worksheet")
            
            # Chuyển đổi sang DataFrame
            df = pd.DataFrame(data)
            
            # Tạo file parquet trong memory
            parquet_buffer = io.BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)
            
            # Upload lên S3
            upload_result = await upload_to_s3(
                file_obj=parquet_buffer,
                key=s3_key,
                content_type="application/octet-stream"
            )
            
            return {
                "status": "success",
                "message": f"Successfully uploaded {len(data)} rows to S3",
                "s3_key": s3_key,
                "rows_count": len(data),
                "columns": list(df.columns),
                "upload_result": upload_result
            }
            
        except gspread.exceptions.SpreadsheetNotFound:
            raise HTTPException(status_code=404, detail="Spreadsheet not found. Check if the sheet_id is correct.")
        except gspread.exceptions.WorksheetNotFound:
            raise HTTPException(status_code=404, detail=f"Worksheet '{worksheet_name}' not found in the spreadsheet.")
        except gspread.exceptions.APIError as e:
            print(f"Google API Error: {e}")
            if e.response.status_code == 403:
                raise HTTPException(
                    status_code=403,
                    detail=f"Permission denied. API Error: {e.response.text}"
                )
            raise HTTPException(status_code=500, detail=f"Google API Error: {str(e)}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            # Reset client nếu có lỗi kết nối
            self.config.reset_client()
            error_msg = str(e)
            if "PERMISSION_DENIED" in error_msg or "forbidden" in error_msg.lower():
                raise HTTPException(
                    status_code=403,
                    detail=f"Permission denied. Error details: {error_msg}"
                )
            raise HTTPException(status_code=500, detail=f"Error processing sheet: {error_msg}")