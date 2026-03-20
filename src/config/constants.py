AGG_DEN_PATH = "tmp/aggregated_features/denied"
AGG_TX_PATH = "tmp/aggregated_features/transactions"
TRAIN_DEN_PATH = "tmp/train_features/denied"
TRAIN_TX_PATH = "tmp/train_features/transactions"
CURRENT_DEN_PATH = "tmp/current_features/denied"
CURRENT_TX_PATH = "tmp/current_features/transactions"
MODEL_DEN_PATH = "tmp/models/denied"
MODEL_TX_PATH = "tmp/models/transactions"
#TO-DO - load 
# .0from db 
# 1-1: Año Nuevo, 5-1: Trabajo, 7-28/29: Fiestas Patrias, 8-30: Sta Rosa,
#  10-8: Angamos, 11-1: Todos Santos, 12-8: Concepción, 12-25: Navidad
PERU_HOLIDAYS = [
    "1-1", "5-1", "7-28", "7-29", "8-30", 
    "10-8", "11-1", "12-8", "12-9", "12-25"
]
DB_PREDICTION_NAME = "DBPrediction"
DB_COMERCIOS_NAME = "Comercios"
TABLE_COMERCIOS_NAME = "[dbo].[comercios]"
TABLE_CESPECIALES_NAME = "[dbo].[CuentasEspeciales] CS"
TABLE_PREDICCIONES_NAME = "[dbo].[Predicciones]"
