from pydantic import BaseModel, Field
from typing import Any, List, Optional, Dict

# ---- GLM shared ----
class ImportValidateRequest(BaseModel):
    json_settings: Dict[str, Any]

class ImportDataAfterMapping(BaseModel):
    json_settings: Dict[str, Any]

class GLMSettingColsOptionsItem(BaseModel):
    col: str

class GLMSettingCols(BaseModel):
    var_single_settings: List[Any] = []
    var_cate_settings: List[Any] = []
    options: Optional[Dict[str, GLMSettingColsOptionsItem]] = None
    calYear: Optional[List[int]] = None
    var_info_settings: Optional[List[str]] = None
    var_additional_settings: Optional[Dict[str, str]] = None

class GLMJsonSettings(BaseModel):
    tableName: Optional[str] = None
    userName: str
    nameProduct: str
    nameFunc: str
    validStatus: Optional[str] = None
    templateName: Optional[str] = None
    setting_cols: GLMSettingCols

class GLMRequest(BaseModel):
    json_settings: GLMJsonSettings
