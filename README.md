# Prediccion3 — Sistema de Predicción de Transacciones Denegadas

---

## Guía de operación

### ¿Qué hace este sistema?

Todos los días al mediodía el sistema analiza el historial de transacciones denegadas de cada comercio y predice cuántas denegaciones tendrá al día siguiente. Si la predicción supera un nivel de alerta, envía automáticamente un correo de notificación al responsable de cuenta de ese comercio.

---

### Notificaciones por correo

Las notificaciones se envían únicamente a los comercios registrados en la tabla **`[DBPrediction].[dbo].[CuentasEspeciales]`**. Un comercio sin registro en esa tabla no recibirá alertas aunque su predicción sea alta.

**Para agregar o modificar un comercio con notificación**, insertar o actualizar directamente en esa tabla.

Agregar nuevo comercio:
```sql
INSERT INTO [DBPrediction].[dbo].[CuentasEspeciales]
    (RUC, RAZON_SOCIAL, NOMBRE_COMERCIAL, CORREO_NOTI, ...)
VALUES
    ('00000000000', 'Empresa S.A.', 'Nombre Comercial', 'correo@empresa.com', ...)
```

Actualizar correo de un comercio existente:
```sql
UPDATE [DBPrediction].[dbo].[CuentasEspeciales]
SET CORREO_NOTI = 'nuevo@empresa.com'
WHERE RUC = '00000000000'
```

> **Correos múltiples:** para enviar la notificación a más de un destinatario, separar los correos con `|` en el campo `CORREO_NOTI`.
> Ejemplo: `correo1@empresa.com|correo2@empresa.com|correo3@empresa.com`

---

### Cuándo se dispara la alerta

La notificación se envía cuando se cumplen las tres condiciones a la vez:

- La predicción del día siguiente **es mayor o igual al promedio de los últimos 7 días**
- Ese promedio de 7 días **es mayor a 0** (el comercio tiene actividad reciente)
- La predicción **es de 5 o más transacciones denegadas**

Un comercio que cumple estas condiciones pero no está en `CuentasEspeciales` igualmente se guarda en `Predicciones` con `NOTIFICA = 1`, pero no recibe correo.

---

### Mantenimiento del modelo — reentrenamiento periódico

Los modelos de predicción aprenden del comportamiento histórico de las transacciones. Con el tiempo, los patrones cambian y el modelo pierde precisión.

**Se recomienda reentrenar los modelos cada 6 meses** ejecutando el script de entrenamiento (`main.py`). Si se observa que las predicciones empiezan a ser consistentemente muy altas o muy bajas respecto a lo real, es señal de que el modelo necesita actualizarse antes de ese plazo.

Los modelos entrenados se guardan en `tmp/models/denied/`. Al reemplazarlos, el proceso del día siguiente ya usará los nuevos automáticamente sin necesidad de reiniciar el servicio.

---

### Verificar que el sistema está funcionando

El sistema corre como un servicio de Windows llamado **`PrediccionService`**. Para ver su estado:

```cmd
sc query PrediccionService
```

Si el servicio está corriendo y aun así no se están guardando predicciones o no llegan correos, revisar los logs en la carpeta `logs/`:

- `service_stdout.log` — salida normal del proceso
- `service_stderr.log` — errores

---

### Pausar o reiniciar el proceso

```cmd
net stop PrediccionService      # Detener
net start PrediccionService     # Iniciar
```

> Cualquier cambio en la configuración (`.env`) o en el código requiere reiniciar el servicio para tomar efecto.

---

## Descripción técnica

Sistema que predice el volumen de transacciones denegadas por comercio para el día siguiente, usando modelos LightGBM entrenados por categoría MCC. Cuando la predicción supera un umbral de alerta, envía una notificación por correo vía Azure Queue Storage.

---

## Estructura del proyecto

```
Prediccion3/
├── src/
│   ├── prediction.py              # Módulo principal de predicción
│   ├── scheduler_service.py       # Servicio que ejecuta el proceso en horario
│   ├── main.py                    # Scripts ETL y utilidades
│   ├── config/
│   │   ├── constants.py           # Rutas, nombres de tablas y BD
│   │   ├── settings.py            # Variables de entorno
│   │   └── logging_config.py      # Configuración de logs
│   ├── domain/
│   │   └── schemas.py             # Definición de columnas por tabla/schema
│   └── repository/
│       └── spark_repository.py    # Funciones JDBC (read/write SQL Server)
├── libs/
│   ├── mssql-jdbc-12.10.2.jre11.jar
│   └── nssm.exe
├── logs/                          # Logs del servicio Windows
├── tmp/                           # Archivos temporales Spark (parquet, modelos)
├── db.sql                         # Scripts DDL de las tablas SQL Server
├── install_service.bat            # Instalador del servicio Windows (NSSM)
└── .env                           # Variables de entorno (no versionar)
```

---

## Bases de datos

| Base de datos | Descripción |
|---|---|
| `Denegadas` | Historial de transacciones denegadas por año |
| `Comercios` | Catálogo maestro de comercios |
| `DBPrediction` | Base de datos del sistema de predicción |

### Tablas en `DBPrediction`

**`[dbo].[CuentasEspeciales]`** — Comercios con cuenta especial asignada. Provee el correo de notificación vinculado por RUC.

**`[dbo].[Predicciones]`** — Resultados diarios del proceso de predicción.

| Campo | Tipo | Descripción |
|---|---|---|
| `ID` | int (PK, identity) | Identificador único del registro |
| `RUC` | nvarchar | RUC del comercio |
| `CODCOMERCIO` | nvarchar | Código único del comercio |
| `MCC` | int | Código de categoría del comercio (Merchant Category Code) |
| `FECHA` | date | Fecha para la que se realizó la predicción |
| `PREDICCION` | decimal(18,10) | Volumen de denegadas predicho para esa fecha |
| `NOTIFICA` | bit | `1` si la predicción superó el umbral de alerta, `0` si no |

---

## Arquitectura

```
main()
  └── process()
        ├── load_data()       → Lee denegadas desde SQL Server, agrega y guarda en parquet
        ├── build_data()      → Genera features ML y guarda parquet por MCC
        └── prepare_data()    → Carga modelos, predice y guarda resultados
              └── por cada modelo MCC:
                    ├── load_lag_data()        → Carga parquet del MCC + filtra comercios
                    ├── build_prediction_ml()  → Construye fila a predecir con lags
                    ├── predict_next_day()     → Aplica modelo, evalúa umbral, notifica
                    └── save_predictions()     → Guarda predicciones en SQL Server
```

---

## Funciones

#### `load_data(spark)`
Lee las tablas de denegadas año por año desde la BD `Denegadas`, agrega por `CODCOMERCIO + FECHA + MCC` y guarda el resultado en parquet particionado por MCC en `tmp/aggregated_features/denied`.

#### `agregate_data(df_den)`
Recibe un DataFrame de denegadas crudas, limpia `CODCOMERCIO`, convierte `FECHAOP` a fecha, reemplaza MCC nulo por `-1` y hace `COUNT(*)` por comercio/fecha/MCC.

#### `build_data(spark)`
Lee el parquet agregado, valida que existan registros del día anterior a `EXECUTION_DAY` y llama a `build_ml()`.

#### `build_ml(df, current_path)`
Genera el dataset de entrenamiento completo:
- Expande el calendario por comercio (sin huecos de fechas)
- Agrega features de calendario: DOW, WEEK, DAY, MONTH, YEAR, IS_WEEKEND, IS_HOLIDAY, IS_PAYDAY, IS_MONTH_START, IS_MONTH_END
- Calcula features de series de tiempo: lags 1/2/7/14/30, medias móviles 7/14, desviación estándar 7, diferencia semanal
- Guarda solo los últimos `LAG_EXECUTION` días en `tmp/current_features/denied` particionado por MCC

#### `skip_model(model_path)`
Retorna `True` si el archivo no es un modelo válido (descarta MCC=-1, MCC=0 y archivos sin "MCC" en el nombre).

#### `load_lag_data(spark, date, data_path, model_path, df_comercios)`
Extrae el MCC del nombre del modelo, carga el parquet correspondiente filtrando `FECHA < date`, agrega la columna MCC y filtra `df_comercios` por ese MCC.
**Retorna:** `(df_data, df_comercios_filtrado)`

#### `build_prediction_ml(date, df_data, df_comercios)`
Construye la fila de predicción para `date`:
1. Crea `df_last`: fila con CODCOMERCIO del día anterior, FECHA=date, TOTAL=0
2. Agrega features de calendario para `date`
3. Une con `df_data` (histórico) para calcular lags y medias móviles en contexto
4. Filtra solo la fila de `date`
5. Hace JOIN con `df_comercios` para enriquecer con RUC, NOMCOMERCIAL, CORREO_NOTI, MCC

**Retorna:** `df_predict` con una fila por comercio del MCC

#### `prepare_data(spark)`
Función principal de predicción:
- Extrae los MCCs disponibles desde los archivos de modelo
- Carga `df_com` desde `[Comercios].[dbo].[comercios]` filtrado por MCC en SQL
- Carga `df_ce` desde `[DBPrediction].[dbo].[CuentasEspeciales]` (broadcast join por RUC)
- Itera sobre cada modelo, genera predicciones y las guarda

#### `predict_next_day(df_predict, model_path, df_data)`
Aplica el modelo LightGBM a cada fila de `df_predict`:
- **Umbral de notificación:** `prediccion >= media_movil_7dias` AND `media_movil > 0` AND `prediccion >= 5`
- Si notifica: obtiene últimos `HISTORY_DAYS` del histórico, construye labels/valores del gráfico (incluyendo la predicción como último punto) y encola el mensaje

**Retorna:** lista de dicts para `save_predictions()`

#### `save_predictions(spark, results)`
Crea un DataFrame Spark con el schema de `Predicciones` y lo escribe en `[DBPrediction].[dbo].[Predicciones]` en modo `append`.

#### `send_queue_mail(correo_noti, code, prediction, d_code, ruc, labels, data)`
Encola un mensaje en Azure Queue Storage con el template de notificación. El campo `labels` y `data` contienen el histórico + predicción formateados para el gráfico del email.

---

## Ejecución automática diaria

El proceso corre como servicio Windows (`PrediccionService`) gestionado por NSSM. El scheduler está definido en `scheduler_service.py` usando APScheduler.

**Horario:** todos los días a las **12:00** (mediodía).

En cada ejecución el scheduler:
1. Toma la fecha actual como `EXECUTION_DAY`
2. Llama a `main()` del módulo `prediction.py`
3. Si el proceso falla, el servicio se reinicia automáticamente tras 5 segundos

### Instalación del servicio

```bat
# Ejecutar como Administrador
install_service.bat
```

Configura el servicio con:
- Inicio automático al arrancar Windows
- Reinicio automático si falla
- Logs rotativos en `logs/service_stdout.log` y `logs/service_stderr.log` (máx 5MB por archivo)

### Comandos de gestión

```cmd
sc query PrediccionService              # Ver estado
net stop PrediccionService              # Detener
net start PrediccionService             # Iniciar
Restart-Service PrediccionService       # Reiniciar (PowerShell admin)
nssm remove PrediccionService confirm   # Desinstalar
```

> Los cambios en el código requieren reiniciar el servicio para tomar efecto.

---

## Variables de entorno relevantes

| Variable | Descripción |
|---|---|
| `EXECUTION_DAY` | Fecha de ejecución `YYYY-MM-DD` |
| `LAG_EXECUTION` | Días de histórico a cargar para features |
| `HISTORY_DAYS` | Días de histórico a mostrar en el gráfico del email (default: 15) |
| `DOUBLE_FROM_YEAR` | Año desde el que aplica tabla doble de transacciones |
| `JDBC_URL` | URL de conexión SQL Server |
| `JDBC_USER` | Usuario JDBC |
| `JDBC_PASSWORD` | Contraseña JDBC |
| `QUEUE_NAME` | Nombre del queue en Azure Storage |
| `QUEUE_CN` | Connection string de Azure Queue |
