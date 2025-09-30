from pydantic import BaseModel
from typing import Any, Dict, Optional, List

class ImportValidateRequest(BaseModel):
    json_settings: Dict[str, Any]

class ImportDataAfterMapping(BaseModel):
    json_settings: Dict[str, Any]

class PNTSettingCols(BaseModel):
    var_single_settings: List[dict]
    var_cate_settings: List[dict]

class PNTJsonSettings(BaseModel):
    tableName: str
    templateName: str
    validStatus: str
    setting_cols: PNTSettingCols

class BeginingReportSettings(BaseModel):
    tableName: str
    validStatus: str

class MOF_PNT_11_Request(BaseModel):
    userName: str
    reportCode: str
    reportYear: int
    reportPeriodCode: str
    reportPeriodValue: int
    gwp_json_settings: PNTJsonSettings
    clm_json_settings: PNTJsonSettings
    res_json_settings: PNTJsonSettings
    begining_report: Optional[BeginingReportSettings] = None

class FinancialStatementRequest(BaseModel):
    userID: str
    userName: str
    reportCode: str
    reportYear: int
    reportPeriodCode: str
    reportPeriodValue: int
    typeCOMPANY: str
    gl_data_settings: Any
    begining_trial_balance: Optional[Any] = None
