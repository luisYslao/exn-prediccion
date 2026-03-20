class DenegadasTable:
    TABLE_NAME = "Denegadas"
    MCC = "MCC"
    CODCOMERCIO = "CODCOMERCIO"
    FECHAOP = "FECHAOP"

class DenegadasSchema:
    TABLE_NAME = "Denegadas"
    MCC = "MCC"
    CODCOMERCIO = "CODCOMERCIO"
    FECHA = "FECHA"
    TOTAL = "TOTAL"

class TransaccionesTable:
    TABLE_NAME = "Yearsxxx"
    MCC = "MCC"
    CODCOMERCIO = "CODCOMERCIO"
    FECHAOPERACION = "FECHAOPERACION"

class TransaccionesSchema:
    TABLE_NAME = "transacciones"
    MCC = "MCC"
    CODCOMERCIO = "CODCOMERCIO"
    FECHA = "FECHA"
    TOTAL = "TOTAL"

class MlTrainSchema:
    MCC = "MCC"
    CODCOMERCIO = "CODCOMERCIO"
    MIN_FECHA = "MIN_FECHA"
    FECHA = "FECHA"
    TOTAL = "TOTAL"

    DOW = "DOW"
    WEEK = "WEEK"
    DAY = "DAY"
    MONTH = "MONTH"
    YEAR = "YEAR"
    IS_WEEKEND = "IS_WEEKEND"
    IS_HOLIDAY = "IS_HOLIDAY"
    IS_PAYDAY = "IS_PAYDAY"

    DEN_LAG_1 = "DEN_LAG_1"
    DEN_LAG_2 = "DEN_LAG_2"
    DEN_LAG_7 = "DEN_LAG_7"
    DEN_LAG_14 = "DEN_LAG_14"
    DEN_LAG_30 = "DEN_LAG_30"
    DEN_MA_7 = "DEN_MA_7"
    DEN_MA_14 = "DEN_MA_14"
    DEN_STD_7 = "DEN_STD_7"
    DIFF_WEEK = "DIFF_WEEK"
    IS_MONTH_START = "IS_MONTH_START"
    IS_MONTH_END = "IS_MONTH_END"

    CAT_FEATURE = [
        #CODCOMERCIO, 
        DOW,
        IS_WEEKEND, 
        IS_HOLIDAY,
        IS_PAYDAY,
        IS_MONTH_START,
        IS_MONTH_END
    ]

    FEATURES = [
        # Calendar
        DOW, WEEK, DAY, MONTH, YEAR, 
        IS_WEEKEND, IS_HOLIDAY, IS_PAYDAY,
        IS_MONTH_START, IS_MONTH_END,
        # Temp
        DEN_LAG_1, DEN_LAG_2, DEN_LAG_7, DEN_LAG_14, DEN_LAG_30,
        DEN_MA_7, DEN_MA_14,
        DEN_STD_7, DIFF_WEEK,
        # Agg
        # Segment
        CODCOMERCIO
    ]

class CEspecialesSchema:
    ID = "ID"
    RUC = "RUC"
    RAZON_SOCIAL = "RAZON_SOCIAL"
    NOMBRE_COMERCIAL = "NOMBRE_COMERCIAL"
    GRUPO_CTA_ESPECIAL = "GRUPO_CTA_ESPECIAL"
    NUEVA_ASIGNACION_DN_2025 = "NUEVA_ASIGNACION_DN_2025"
    NOMBRE_KAM = "NOMBRE_KAM"
    TIPO_DE_CUENTA = "TIPO_DE_CUENTA"
    CORREO_NOTI = "CORREO_NOTI"

class ComerciosSchema:
    RUC = "RUC"
    CODCOMERCIO = "CODCOMERCIO"
    NOMCOMERCIAL = "NOMCOMERCIAL"
    MCC = "MCC"

class PrediccionesSchema:
    RUC = "RUC"
    CODCOMERCIO = "CODCOMERCIO"
    MCC = "MCC"
    FECHA = "FECHA"
    PREDICCION = "PREDICCION"
    NOTIFICA = "NOTIFICA"
