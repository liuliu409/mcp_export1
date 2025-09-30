import pandas as pd
import re
import unicodedata

def standardize_reg_no(reg_no):
    if pd.isna(reg_no) or str(reg_no).strip() == "":
        return "ERROR_REG_NO"

    reg_no_str = str(reg_no).strip().upper()
    
    #Chuẩn hoá Unicode các ký tự đặc biệt 
    reg_no_str = unicodedata.normalize("NFKC", reg_no_str)

    #Xoá dấu gạch ngang và dấu chấm (bao gồm cả Unicode)
    reg_no_str = re.sub(r"[-‐‑–‒—―\.]", "", reg_no_str)

    #Xoá khoảng trắng giữa các chữ cái (VD: L D → LD)
    reg_no_str = re.sub(r'(?<=[A-Z])\s+(?=[A-Z])', '', reg_no_str)

    #Xoá khoảng trắng còn lại (nếu có)
    reg_no_str = re.sub(r"\s+", "", reg_no_str)

    #Regex biển số thông thường hợp lệ thoả các yêu cầu:
    ##1–2 ký tự đầu là mã tỉnh (có thể là chữ hoặc số)
    ##theo sau là 1–3 chữ cái (cho biết loại xe)
    ##sau đó 3–6 chữ số
    ##cuối cùng là 1 chữ cái tuỳ chọn (T, M, P, ...)
    pattern = r'^[A-Z0-9]{1,2}[A-Z]{1,3}\d{3,6}[A-Z]?$'

    if re.fullmatch(pattern, reg_no_str):
        return reg_no_str
    #Regex BKS đặc biệt
    ##5 chữ số + NN / NG / QT / CV + 2 chữ số
    foreign_pattern = r'^\d{5}(NN|NG|QT|CV)\d{2}$'
    if re.fullmatch(foreign_pattern, reg_no_str):
        return reg_no_str
    return "ERROR_REG_NO"
