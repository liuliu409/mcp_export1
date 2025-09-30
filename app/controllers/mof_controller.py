from fastapi import HTTPException, Depends
from sqlalchemy.orm import Session
import os
import requests
import io
import pandas as pd
import json
from datetime import datetime
import re
import numpy as np
from services.glm_service import GLMService
from exceptions import ConflictException
from controllers.base.base_controller import BaseController
from utils.database import get_db
from utils.json_encoder import NpEncoder
from modules.MOF.mof_valid_data import analyze_dataframe
from modules.MOF.mof_pnt_11 import (
    apply_mapping,
    summary_gwp,
    summary_claim,
    summary_reserve,
    summary_begining_report,
    combine_summaries
)
from modules.MOF.mof_fin_report import (
    create_financial_report
)
from modules.db_parquet import read_parquet_from_s3, upload_to_s3, cfg, extract_parquet_key
from schemas.mof_report import (
    MOF_PNT_11_Request,
    ImportDataAfterMapping,
    ImportValidateRequest,
    FinancialStatementRequest,
)

class MOFReportController(BaseController):
    def __init__(self):
        super().__init__(prefix="/mof-report", tags=["MOF Report Controller"])
        self.service = GLMService()
        
        self.router.add_api_route(
            "/mof-valid-data/", self.mof_valid_data, methods=["POST"]
        )
        self.router.add_api_route(
            "/mof-import-data-after-mapping/",
            self.mof_import_data_after_maping,
            methods=["POST"],
        )
        self.router.add_api_route("/mof-pnt-11/", self.mof_pnt_11, methods=["POST"])
        self.router.add_api_route("/mof-pnt-bctcq/", self.mof_pnt_bctcq, methods=["POST"])

    async def mof_valid_data(self, request_body: ImportValidateRequest):
        start_time = datetime.now()

        # Extract and validate request data
        try:
            rq_url = request_body.json_settings["url"]
            rq_nameFunc = request_body.json_settings["nameFunc"]
            rq_userName = request_body.json_settings["userName"]
            rq_nameProduct = request_body.json_settings["nameProduct"]
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")

        if not rq_url:
            raise HTTPException(status_code=400, detail="No file or URL provided")

        contents, file_name, file_extension = await self.service.download_file_from_url(rq_url)

        # Parse file
        try:
            if file_extension == ".csv":
                df_import = pd.read_csv(io.BytesIO(contents), skiprows=1)
            elif file_extension in [".xlsx", ".xls", ".xlsm"]:
                df_import = pd.read_excel(io.BytesIO(contents), skiprows=1)
            else:
                raise HTTPException(status_code=400, detail="Unsupported file format")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")

        # Map columns
        try:
            column_mapping = request_body.json_settings.get("setting_cols", [])
            system_name_cols = [col["standard_name"] for col in column_mapping]
            business_name_cols = [col["import_name"] for col in column_mapping]
            business_to_system = {
                col["import_name"]: col["standard_name"] for col in column_mapping
            }
            df = df_import.rename(columns=business_to_system)
            df = df[df.columns.intersection(business_to_system.values())]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error mapping columns: {str(e)}")

        # Validate data
        try:
            status = False
            message = "Data validation failed !"

            # Check if validation settings are provided
            if (len(system_name_cols) > 0) and (len(business_name_cols) > 0):
                rq2json = json.dumps(request_body.model_dump(), ensure_ascii=False, indent=4)

                validation_results = None
                validation_results = analyze_dataframe(df, json.loads(rq2json))

                if not validation_results:
                    raise HTTPException(
                        status_code=500, detail="Validation produced no results"
                    )

                # Transform dataframe summary to list format
                validation_results[0]["dataframe_summary"] = [
                    {"Column": col, **details}
                    for col, details in validation_results[0]["dataframe_summary"].items()
                ]

                # Serialize validation results
                updated_json_data = json.dumps(validation_results[0], cls=NpEncoder, indent=4)

                # Check validation results
                has_type_errors = len(validation_results[0]["error_details"]["type_check"]) > 0
                has_missing_errors = len(validation_results[0]["error_details"]["missing_check"]) > 0
                has_unknown_errors = len(validation_results[0]["error_details"]["unknown_check"]) > 0
                has_duplicate_errors = (
                    "dup_check" in validation_results[0]["error_details"]
                    and len(validation_results[0]["error_details"]["dup_check"]) > 0
                )

                # Set status based on validation results
                if not (has_type_errors or has_missing_errors or has_duplicate_errors or has_unknown_errors):
                    status = True
                    message = "Data validation completed successfully"
                else:
                    if has_type_errors:
                        message += "\n Due to wrong type: " + ", ".join(
                            [str(item) for item in validation_results[0]["error_details"]["type_check"]]
                        )
                    elif has_missing_errors:
                        message += "\n Due to missing: " + ", ".join(
                            [str(item) for item in validation_results[0]["error_details"]["missing_check"]]
                        )
                    elif has_unknown_errors:
                        message += "\n Due to unknown: " + ", ".join(
                            [str(item) for item in validation_results[0]["error_details"]["unknown_check"]]
                        )
                    else:
                        message += "\n Due to duplicate: " + ", ".join(
                            [str(item) for item in validation_results[0]["error_details"]["dup_check"]]
                        )
            else:
                raise HTTPException(
                    status_code=400, detail="No validation settings provided"
                )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error during validation: {str(e)}")

        # Return response
        return {
            "isValidated": status,
            "times_run": datetime.now() - start_time,
            "message": message,
            "data": json.loads(updated_json_data) #if status else None,
        }

    async def mof_import_data_after_maping(
        self, request_body: ImportDataAfterMapping, db: Session = Depends(get_db)
    ):
        start_time = datetime.now()

        # Extract and validate request data
        try:
            rq_url = request_body.json_settings["url"]
            rq_nameFunc = request_body.json_settings["nameFunc"]
            rq_userName = request_body.json_settings["userName"]
            rq_nameProduct = request_body.json_settings["nameProduct"]
            rq_validStatus = request_body.json_settings["validStatus"]
            rq_templateName = request_body.json_settings["templateName"]

        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")

        if rq_validStatus == "Validated":
            raise HTTPException(status_code=400, detail="Data has been validated")

        if not rq_url:
            raise HTTPException(status_code=400, detail="No file or URL provided")

        contents, file_name, file_extension = await self.service.download_file_from_url(rq_url)

        # Parse file
        try:
            if file_extension == ".csv":
                df_import = pd.read_csv(io.BytesIO(contents), skiprows=1)
            elif file_extension in [".xlsx", ".xls", ".xlsm"]:
                df_import = pd.read_excel(io.BytesIO(contents), skiprows=1)
            else:
                raise HTTPException(status_code=400, detail="Unsupported file format")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")

        # Map columns
        try:
            column_mapping = request_body.json_settings.get("setting_cols", [])
            system_name_cols = [col["standard_name"] for col in column_mapping]
            system_type_cols = [col["data_type"] for col in column_mapping]
            business_name_cols = [col["import_name"] for col in column_mapping]

            # Kiểm tra trùng lặp trong danh sách import_name
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
            system_name_type = dict(zip(system_name_cols, system_type_cols))

            if not system_name_cols:
                raise HTTPException(
                    status_code=409,
                    detail="Vui lòng lưu thiết lập đã chuẩn hoá trước khi import",
                )

            df = df_import.rename(columns=business_to_system)
            df = df[df.columns.intersection(business_to_system.values())]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error mapping columns: {str(e)}")

        # Convert columns based on settings
        try:
            date_formats = [
                "%d/%m/%Y %H:%M",  # 01/01/2019 01:15
                "%d/%m/%Y",  # 01/01/2019
                "%Y-%m-%d %H:%M:%S",  # 2019-01-01 01:15:00
                "%Y-%m-%d",  # 2019-01-01
            ]

            for col, dtype in system_name_type.items():
                if dtype.lower() == "date":
                    for date_format in date_formats:
                        try:
                            df[col] = pd.to_datetime(
                                df[col].astype(str).str.strip(),
                                format=date_format,
                                errors="raise",
                            )
                            break
                        except ValueError:
                            continue

                    if not pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = pd.to_datetime(df[col], errors="coerce")

                    if hasattr(df[col], "dt") and df[col].dt.tz:
                        df[col] = df[col].dt.tz_localize(None)

                    df[col] = pd.to_datetime(df[col].dt.strftime("%Y-%m-%d"))
                elif dtype.lower() == "double" or dtype.lower() == "integer":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype.lower() == "text":
                    # df[col] = df[col].apply(lambda x: str(x) if not pd.isnull(x) else "Unknown")
                    df[col] = df[col].astype(str).fillna("Unknown")

            # Change format of COVERAGE_ID column to "0xxxxx"
            if "COVERAGE_ID" in df.columns:
                pattern = re.compile(r"^0.")
                df["COVERAGE_ID"] = df["COVERAGE_ID"].astype(str).agg(lambda x: f"0{x}" if not pattern.match(x) else x)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error converting columns: {str(e)}")

        # Generate table name
        try:
            match = re.search(r"_([0-9]{14})\.[^.]+$", os.path.basename(rq_url))
            if match:
                number_part = match.group(1)
                table_name = f"{rq_userName}_{rq_templateName}_IMPORT_{number_part}"
            else:
                table_name = (
                    rq_userName
                    + "_"
                    + rq_templateName
                    # + "_"
                    # + rq_nameProduct
                    + "_IMPORT_"
                    + datetime.now().strftime("%Y%m%d%H%M%S")
                )
            file_name = f"{table_name}.parquet"
            s3_key = f"report-software/mof/{rq_userName}/analysis_data/{file_name}"
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error generating table name: {str(e)}")

        # Lưu file parquet lên S3
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            upload_to_s3(
                bucket=cfg["BUCKET"],
                key=s3_key,
                data_bytes=buffer.getvalue(),
                mimetype="application/octet-stream"
            )
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
            },
        }

    async def mof_pnt_11(self, request_body: MOF_PNT_11_Request):
        start_time = datetime.now()
        
        user_name = request_body.userName
        report_code = request_body.reportCode
        report_year = request_body.reportYear
        report_period_code = request_body.reportPeriodCode
        report_period_value = request_body.reportPeriodValue

        settings_list = [
            ("gwp", request_body.gwp_json_settings),
            ("clm", request_body.clm_json_settings),
            ("res", request_body.res_json_settings),
            ("beg_report", request_body.begining_report)
        ]

        dfs = {} # dict with key as 'gwp', 'clm', 'res' and value as DataFrame
        for key, setting in settings_list:
            try:
                if key == "beg_report" and setting is None:
                    continue # Skip if no begining report settings
                table_name = setting.tableName
                valid_status = setting.validStatus
                # Lấy key file parquet từ URL
                parquet_key = extract_parquet_key(table_name)
                # Đọc file parquet từ S3
                df = read_parquet_from_s3(parquet_key)
                if key != "beg_report":
                    var_single_settings = setting.setting_cols.var_single_settings
                    var_cate_settings = setting.setting_cols.var_cate_settings
                    # Kiểm tra xem bảng đã được validate chưa
                    if valid_status != "Validated":
                        raise HTTPException(
                            status_code=409,
                            detail=f"Data in {key.upper()} needs to be validated before processing",
                        )

                    dfs[key] = apply_mapping(df,var_single_settings, var_cate_settings)
                else:
                    if valid_status != "hoan_thanh":
                        raise HTTPException(
                            status_code=409,
                            detail=f"Data in {key.upper()} needs to be completed before processing",
                        )
                    dfs[key] = df

            except KeyError as e:
                raise HTTPException(status_code=409, detail=f"Missing required field in {key.upper()}: {str(e)}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error loading {key.upper()} parquet: {str(e)}")

        # process GWP, CLM, and RES data
        try:
            dfgwp = summary_gwp(dfs["gwp"])
            dfclm = summary_claim(dfs["clm"])
            dfres = summary_reserve(dfs["res"],request_body.res_json_settings.templateName)
            if request_body.begining_report is not None:
                dfbeg = summary_begining_report(dfs["beg_report"])
                dfcombine = combine_summaries(dfgwp, dfclm, dfres, dfbeg)
            else:
                if request_body.res_json_settings.templateName != "RES_PNT_11_02":
                    raise HTTPException(
                        status_code=409,
                        detail="Vui lòng sử dụng mẫu RES_PNT_11_02 để có số liệu đầu kỳ!",
                    )
                dfcombine = combine_summaries(dfgwp, dfclm, dfres, None)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")
        
        # Generate table name
        try:
            table_name = (
                user_name
                + "_"
                + report_code
                + "_"
                + str(report_year) + "-" + report_period_code + "-" + str(report_period_value)
            )
            file_name = f"{table_name}.parquet"
            s3_key = f"report-software/mof/{user_name}/analysis_data/{file_name}"
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error generating table name: {str(e)}")

        # Lưu file parquet lên S3
        try:
            buffer = io.BytesIO()
            dfcombine.to_parquet(buffer, index=False)
            buffer.seek(0)
            upload_to_s3(
                bucket=cfg["BUCKET"],
                key=s3_key,
                data_bytes=buffer.getvalue(),
                mimetype="application/octet-stream"
            )
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
            },
        }
    
    async def mof_pnt_bctcq(self, request_body: FinancialStatementRequest):
        start_time = datetime.now()
        user_id = request_body.userID
        user_name = request_body.userName
        report_code = request_body.reportCode
        report_year = request_body.reportYear
        report_period_code = request_body.reportPeriodCode
        report_period_value = request_body.reportPeriodValue
        type_company = request_body.typeCOMPANY
        # Extract GL data settings
        gl_settings = request_body.gl_data_settings
        
        try:
            # 1. Load GL_DATA từ S3
            table_name = gl_settings.tableName
            valid_status = gl_settings.validStatus
            
            # Kiểm tra xem GL data đã được validate chưa
            if valid_status != "Validated":
                raise HTTPException(
                    status_code=409,
                    detail="GL_DATA needs to be validated before processing",
                )
            
            # Lấy key file parquet từ URL
            parquet_key = extract_parquet_key(table_name)
            # Đọc file parquet từ S3
            gl_data = read_parquet_from_s3(parquet_key)
            
            # 2. Validate DEBIT_ACC và CREDIT_ACC mapping
            var_single_settings = gl_settings.setting_cols.var_single_settings
            
            # Check if var_single_settings exists and is not empty
            if not var_single_settings or len(var_single_settings) == 0:
                raise HTTPException(
                    status_code=409,
                    detail="GL_DATA mapping settings are required",
                )
            
            # Extract DEBIT_ACC and CREDIT_ACC validation status
            debit_validated = False
            credit_validated = False
            
            for var_setting in var_single_settings:
                # Check DEBIT_ACC validation
                if hasattr(var_setting, "DEBIT_ACC") and var_setting.DEBIT_ACC:
                    debit_acc_settings = var_setting.DEBIT_ACC
                    if debit_acc_settings and len(debit_acc_settings) > 0:
                        debit_status = getattr(debit_acc_settings[0], "valid_mapping", "")
                        if debit_status == "Validated":
                            debit_validated = True

                # Check CREDIT_ACC validation
                if hasattr(var_setting, "CREDIT_ACC") and var_setting.CREDIT_ACC:
                    credit_acc_settings = var_setting.CREDIT_ACC
                    if credit_acc_settings and len(credit_acc_settings) > 0:
                        credit_status = getattr(credit_acc_settings[0], "valid_mapping", "")
                        if credit_status == "Validated":
                            credit_validated = True
            
            # Kiểm tra validation status
            if not debit_validated:
                raise HTTPException(
                    status_code=409,
                    detail="DEBIT_ACC mapping must be validated before processing Trial Balance",
                )
            
            if not credit_validated:
                raise HTTPException(
                    status_code=409,
                    detail="CREDIT_ACC mapping must be validated before processing Trial Balance",
                )
            
            # 3. Load Opening Balance (optional)
            opening_balance_data = None
            if hasattr(request_body, 'begining_report') and request_body.begining_trial_balance is not None:
                try:
                    beg_table_name = request_body.begining_trial_balance.tableName
                    beg_valid_status = request_body.begining_trial_balance.validStatus

                    if beg_valid_status != "hoan_thanh":
                        raise HTTPException(
                            status_code=409,
                            detail="Beginning Trial Balance data needs to be completed before processing",
                        )
                    
                    beg_parquet_key = extract_parquet_key(beg_table_name)
                    opening_balance_data = read_parquet_from_s3(beg_parquet_key)
                    
                except Exception as e:
                    # Log warning but continue without opening balance
                    print(f"Warning: Could not load opening balance data: {str(e)}")
                    opening_balance_data = None

            results = create_financial_report(gl_data, user_id, type_company
                                              , opening_balance_data, report_year
                                              , report_period_code, report_period_value
            )
            # 4. Process Trial Balance
            if results["trial_balance"] is None or results["trial_balance"].empty:
                raise HTTPException(
                    status_code=500,
                    detail=f"Error processing Trial Balance: {results.get('message', 'No data')}",
                )
            
            # 5. Save results to S3
            trial_balance = results["trial_balance"]
            balance_sheet = results["balance_sheet"]
            pl01_report = results["pl01"] # bảng báo cáo lãi lỗ theo hoạt động kinh doanh
            pl02_report = results["pl02"] # bảng báo cáo lãi lỗ tổng hợp
            cf01_report = results["cf01"] # bảng báo cáo lưu chuyển tiền tệ (trực tiếp)
            cf02_report = results["cf02"] # bảng báo cáo lưu chuyển tiền tệ (gián tiếp)
            
            # Generate file names
            base_name = f"{user_name}_{report_code}_{report_year}{report_period_code}{report_period_value}"
            
            saved_files = []
            
            # Save Trial Balance
            if trial_balance is not None and not trial_balance.empty:
                tb_file_name = f"{base_name}_TRIAL_BALANCE.parquet"
                tb_s3_key = f"report-software/mof/{user_name}/financial_reports/{tb_file_name}"
                
                buffer = io.BytesIO()
                trial_balance.to_parquet(buffer, index=False)
                buffer.seek(0)
                upload_to_s3(
                    bucket=cfg["BUCKET"],
                    key=tb_s3_key,
                    data_bytes=buffer.getvalue(),
                    mimetype="application/octet-stream"
                )
                saved_files.append({"type": "trial_balance", "file_name": tb_file_name, "s3_key": tb_s3_key})
            
            # Save Balance Sheet
            if balance_sheet is not None and not balance_sheet.empty:
                bs_file_name = f"{base_name}_BALANCE_SHEET.parquet"
                bs_s3_key = f"report-software/mof/{user_name}/financial_reports/{bs_file_name}"
                
                buffer = io.BytesIO()
                balance_sheet.to_parquet(buffer, index=False)
                buffer.seek(0)
                upload_to_s3(
                    bucket=cfg["BUCKET"],
                    key=bs_s3_key,
                    data_bytes=buffer.getvalue(),
                    mimetype="application/octet-stream"
                )
                saved_files.append({"type": "balance_sheet", "file_name": bs_file_name, "s3_key": bs_s3_key})
            
            # Save PL01
            if pl01_report is not None and not pl01_report.empty:
                pl01_file_name = f"{base_name}_PL01.parquet"
                pl01_s3_key = f"report-software/mof/{user_name}/financial_reports/{pl01_file_name}"
                
                buffer = io.BytesIO()
                pl01_report.to_parquet(buffer, index=False)
                buffer.seek(0)
                upload_to_s3(
                    bucket=cfg["BUCKET"],
                    key=pl01_s3_key,
                    data_bytes=buffer.getvalue(),
                    mimetype="application/octet-stream"
                )
                saved_files.append({"type": "pl01", "file_name": pl01_file_name, "s3_key": pl01_s3_key})
            
            # Save PL02
            if pl02_report is not None and not pl02_report.empty:
                pl02_file_name = f"{base_name}_PL02.parquet"
                pl02_s3_key = f"report-software/mof/{user_name}/financial_reports/{pl02_file_name}"
                
                buffer = io.BytesIO()
                pl02_report.to_parquet(buffer, index=False)
                buffer.seek(0)
                upload_to_s3(
                    bucket=cfg["BUCKET"],
                    key=pl02_s3_key,
                    data_bytes=buffer.getvalue(),
                    mimetype="application/octet-stream"
                )
                saved_files.append({"type": "pl02", "file_name": pl02_file_name, "s3_key": pl02_s3_key})
            
            # Save CF01
            if cf01_report is not None and not cf01_report.empty:
                cf01_file_name = f"{base_name}_CF01.parquet"
                cf01_s3_key = f"report-software/mof/{user_name}/financial_reports/{cf01_file_name}"
                
                buffer = io.BytesIO()
                cf01_report.to_parquet(buffer, index=False)
                buffer.seek(0)
                upload_to_s3(
                    bucket=cfg["BUCKET"],
                    key=cf01_s3_key,
                    data_bytes=buffer.getvalue(),
                    mimetype="application/octet-stream"
                )
                saved_files.append({"type": "cf01", "file_name": cf01_file_name, "s3_key": cf01_s3_key})

            # Save CF02
            if cf02_report is not None and not cf02_report.empty:
                cf02_file_name = f"{base_name}_CF02.parquet"
                cf02_s3_key = f"report-software/mof/{user_name}/financial_reports/{cf02_file_name}"
                
                buffer = io.BytesIO()
                cf02_report.to_parquet(buffer, index=False)
                buffer.seek(0)
                upload_to_s3(
                    bucket=cfg["BUCKET"],
                    key=cf02_s3_key,
                    data_bytes=buffer.getvalue(),
                    mimetype="application/octet-stream"
                )
                saved_files.append({"type": "cf02", "file_name": cf02_file_name, "s3_key": cf02_s3_key})

        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing Trial Balance: {str(e)}")
        
        # Return response
        return {
            "status": True,
            "times_run": datetime.now() - start_time,
            "message": f"Financial reports processed successfully",
            "data": {
                "s3_bucket": cfg["BUCKET"],
                "saved_files": saved_files,
                "summary": {
                    "trial_balance_records": len(trial_balance) if trial_balance is not None else 0,
                    "balance_sheet_records": len(balance_sheet) if balance_sheet is not None else 0,
                    "pl01_records": len(pl01_report) if pl01_report is not None else 0,
                    "pl02_records": len(pl02_report) if pl02_report is not None else 0,
                    "cf01_records": len(cf01_report) if cf01_report is not None else 0,
                    "cf02_records": len(cf02_report) if cf02_report is not None else 0,
                    "has_opening_balance": opening_balance_data is not None
                }
            },
        }

mof_car_controller = MOFReportController()
router = mof_car_controller.router
