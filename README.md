# data-profiler

`data-profiler` es un **perfilador de datos modular en Python**, diseñado para escenarios de ingeniería de datos y análisis, permitiendo inspeccionar tablas y consultas en bases de datos empresariales.

En esta primera fase se enfoca en:

- **Motores soportados**:
  - Microsoft **SQL Server**
  - **Oracle**

- **Objetivo**:
  - Generar métricas de **perfilado** a nivel tabla y columna.
  - Detectar **outliers** en columnas numéricas.
  - Permitir ejecutar el perfilado **solo sobre objetos seleccionados** mediante una whitelist (tablas y/o queries definidas en un archivo JSON).

El diseño es altamente modular para permitir ampliaciones futuras (nuevos motores, nuevas métricas, UI, reportes HTML, etc.).

---

# **Características – Fase 1**

## 1. Conectividad a bases de datos

El sistema utiliza conectores especializados, detrás de una interfaz común:

### **Interfaz base: `DatabaseConnector`**

Todo conector debe implementar:

- `connect()`
- `list_schemas()`
- `list_tables(schema)`
- `get_columns(schema, table)`
- `run_query(sql)`
- `sample_data(sql, sample_rows)`

### **Motores incluidos**

- `SqlServerConnector`
- `OracleConnector`

### Métodos responsables de:

- Establecer conexión con la BD.
- Obtener metadatos (schemas, tablas, columnas).
- Ejecutar queries y obtener muestras de datos.

---

## 2. Selección de objetos a perfilar (WHITELIST)

Para evitar escanear toda la base de datos, el perfilador solo procesa objetos explícitamente definidos por el usuario en un archivo JSON (targets.json).

### **Tipos de target admitidos**

1. **Tablas**
2. **Queries completamente arbitrarias**
3. **Tablas con filtros (WHERE)**

### **Estructura de ejemplo:**

```json
{
  "targets": [
    {
      "type": "table",
      "schema": "dbo",
      "table": "Customer",
      "where": "IsActive = 1",
      "sample_rows": 15000
    },
    {
      "type": "table",
      "schema": "sales",
      "table": "FactSales"
    },
    {
      "type": "query",
      "name": "CustomerWithOrders",
      "sql": "SELECT c.CustomerId, c.Name, o.OrderDate, o.Amount
              FROM dbo.Customer c
              JOIN dbo.[Order] o ON c.CustomerId = o.CustomerId
              WHERE o.OrderDate >= '2024-01-01'",
      "sample_rows": 5000
    }
  ]
}
```

---

## Requerimientos e instalación

- Python 3.10+ recomendado.
- Crear entorno virtual y instalar dependencias:

```bash
python -m venv .venv
.venv\Scripts\activate  # en Windows
pip install -r requirements.txt
```

- Drivers por motor:
  - SQL Server: requiere controlador ODBC instalado (por ejemplo, Microsoft ODBC Driver 17/18) para que `pyodbc` funcione.
  - Oracle: `oracledb` (modo thin) funciona sin cliente; si usas modo thick necesitarás el Oracle Client. `cx_Oracle` puede usarse como alternativa.

## Ejecución por CLI

```bash
python -m profiler ^
  --engine sqlserver ^
  --connstr "Driver={ODBC Driver 18 for SQL Server};Server=mi_servidor;Database=mi_db;UID=user;PWD=pass;Encrypt=no" ^
  --targets-file targets.json ^
  --sample-rows 10000 ^
  --outdir out ^
  --outliers-method both
```

- `--engine`: `sqlserver` u `oracle`.
- `--targets-file`: ruta al JSON con los targets (ver ejemplo arriba).
- `--outdir`: carpeta de salida. Se generan `table_profile.csv`, `column_profile.csv` y `outliers.csv` (solo si hay outliers).

## Uso desde Python

```python
from profiler.config import Config
from profiler.profiling.profiler import Profiler

config = Config(
    engine="sqlserver",
    connection_string="...",
    targets_file="targets.json",
    sample_rows=10000,
    outdir="out",
)

results = Profiler(config).run()
print(results.table_profile.head())
```

## Pruebas

- Instalar dependencias de desarrollo (`pytest` incluido en `requirements.txt`).
- Ejecutar:

```bash
pytest
```

Los tests unitarios mockean conectores y usan dataframes en memoria, no necesitan acceso a bases de datos.
