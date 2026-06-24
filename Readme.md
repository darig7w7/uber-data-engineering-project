# Uber Data Engineering Project
### End-to-End ETL Pipeline con GCP, Mage, BigQuery y Looker Studio

Proyecto de ingeniería de datos basado en el tutorial de Darshil Parmar. Procesa datos de viajes de taxi de NYC (Yellow Taxi, marzo 2016) aplicando un modelo dimensional (esquema en estrella) para análisis en BigQuery y visualización en Looker Studio.

---

## Arquitectura general

```
uber_data.csv
     │
     ▼
Google Cloud Storage (bucket)
     │
     ▼
Mage AI (VM en Compute Engine)
 ├── Data Loader   → lee el CSV desde GCS
 ├── Transformer   → aplica el modelo dimensional (Python/pandas)
 └── Data Exporter → escribe las 8 tablas en BigQuery
     │
     ▼
BigQuery (data warehouse)
 └── analytics_query.sql → tabla tbl_analytics (JOIN de todas las dimensiones)
     │
     ▼
Looker Studio (dashboard)
```

---

## Dataset

- **Fuente:** TLC Trip Record Data — Yellow Taxi, marzo 2016
- **Archivo:** `uber_data.csv` (100.000 filas, 19 columnas)
- **Repo original:** https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project

---

## Modelo de datos (Star Schema)

La tabla de hechos central (`fact_table`) se conecta con 7 dimensiones via claves foráneas (FK):

| Tabla | Tipo | Descripción |
|---|---|---|
| `fact_table` | Fact | Métricas del viaje (tarifas, propinas, recargos) |
| `datetime_dim` | Dimensión | Fecha/hora de pickup y dropoff descompuesta |
| `passenger_count_dim` | Dimensión | Cantidad de pasajeros |
| `trip_distance_dim` | Dimensión | Distancia del viaje |
| `rate_code_dim` | Dimensión | Tipo de tarifa (Standard, JFK, Newark, etc.) |
| `pickup_location_dim` | Dimensión | Coordenadas de origen (lat/long) |
| `dropoff_location_dim` | Dimensión | Coordenadas de destino (lat/long) |
| `payment_type_dim` | Dimensión | Forma de pago (tarjeta, efectivo, etc.) |

---

## Requisitos previos

- Cuenta de Google Cloud Platform (GCP) con facturación habilitada
- Python 3.x instalado localmente (para pruebas en notebook)
- WSL (Ubuntu) o cualquier terminal Linux/Mac
- Docker instalado en la VM de GCP

---

## Paso 1 — Preparación local (notebook)

### 1.1 Crear carpeta del proyecto en WSL

```bash
mkdir -p /home/TU_USUARIO/Maestria/Curso_02/Trabajos/Uber_project
cd /home/TU_USUARIO/Maestria/Curso_02/Trabajos/Uber_project
```

### 1.2 Crear entorno virtual y activarlo

```bash
python3 -m venv venv
source venv/bin/activate
```

Agregar alias permanente en `~/.bashrc` para no tener que escribirlo cada vez:

```bash
echo "alias uber-env='cd /home/TU_USUARIO/Maestria/Curso_02/Trabajos/Uber_project && source venv/bin/activate'" >> ~/.bashrc
source ~/.bashrc
```

### 1.3 Instalar dependencias

```bash
pip install pandas numpy jupyter notebook
```

### 1.4 Descargar dataset y notebook

```bash
wget https://raw.githubusercontent.com/darshilparmar/uber-etl-pipeline-data-engineering-project/main/data/uber_data.csv
wget "https://raw.githubusercontent.com/darshilparmar/uber-etl-pipeline-data-engineering-project/main/Uber%20Data%20Pipeline%20(Video%20Version).ipynb"
```

### 1.5 Levantar Jupyter y explorar el notebook

```bash
jupyter notebook --no-browser
```

Copiá la URL con el token que aparece en la terminal y abrila en el navegador de Windows (si usás WSL). Corré el notebook celda por celda con `Shift+Enter` para validar que las 8 tablas se generan correctamente antes de pasar a la nube.

---

## Paso 2 — Google Cloud Platform

### 2.1 Crear proyecto en GCP

1. Ir a https://console.cloud.google.com
2. Hacer clic en el selector de proyectos (arriba a la izquierda) → **"Nuevo proyecto"**
3. Nombre: `uber-data-project` → **Crear**
4. Confirmar que el proyecto nuevo esté seleccionado

### 2.2 Habilitar facturación

Necesario para usar Cloud Storage, Compute Engine y BigQuery. Google ofrece $300 de crédito gratuito por 90 días para cuentas nuevas. La verificación pide datos de tarjeta pero no realiza cobros automáticos salvo que se active manualmente una cuenta paga.

### 2.3 Crear bucket en Cloud Storage

1. Ir a **Cloud Storage → Buckets → Crear**
2. Nombre del bucket: `uber-project-darwin-2026` (debe ser único a nivel mundial)
3. Ubicación: **`US (multiple regions in United States)`** — importante que coincida con la ubicación del dataset de BigQuery
4. Clase de almacenamiento: **Standard**
5. Acceso: dejar configuración por defecto (privado)
6. Hacer clic en **Crear**
7. Una vez creado, hacer clic en **"Subir archivos"** y seleccionar `uber_data.csv`

### 2.4 Crear dataset en BigQuery

1. Ir a **BigQuery → (panel izquierdo) → tres puntitos del proyecto → "Crear conjunto de datos"**
2. ID del conjunto de datos: `uber_data_engineering`
3. Ubicación: **`US (multiple regions in United States)`** — debe coincidir con el bucket
4. Hacer clic en **Crear conjunto de datos**

### 2.5 Crear Service Account (credenciales para Mage)

1. Ir a **IAM y administración → Cuentas de servicio → Crear cuenta de servicio**
2. Nombre: `mage-bigquery-access`
3. Rol: **BigQuery Admin** (o BigQuery Data Editor + BigQuery Job User para permisos mínimos)
4. Una vez creada, hacer clic en la cuenta → pestaña **"Claves"** → **"Agregar clave"** → **"Crear clave nueva"** → tipo **JSON**
5. Guardar el archivo `.json` descargado — contiene credenciales sensibles, no compartir

### 2.6 Crear VM en Compute Engine

1. Ir a **Compute Engine → Instancias de VM → Crear instancia**
2. Nombre: `uber-project-instance`
3. Región: `us-central1 (Iowa)`
4. Serie de máquina: **E2**, tipo: **`e2-standard-4`** (4 vCPUs, 16 GB RAM)
5. Sistema operativo: Debian GNU/Linux (por defecto)
6. En la sección **Redes → Firewall**: marcar **"Permitir tráfico HTTP"** y **"Permitir tráfico HTTPS"**
7. Hacer clic en **Crear**

### 2.7 Abrir puerto 6789 en el Firewall

Mage usa el puerto 6789, que no queda abierto por defecto:

1. Ir a **VPC Network → Firewall → Crear regla de firewall**
2. Nombre: `allow-mage-6789`
3. Dirección: Entrada (Ingress)
4. Acción: Permitir
5. Rangos de IP de origen: `0.0.0.0/0`
6. Protocolos y puertos: **TCP → `6789`**
7. Hacer clic en **Crear**

---

## Paso 3 — Instalación de Mage en la VM

### 3.1 Conectarse por SSH

Desde la consola de GCP → Compute Engine → Instancias de VM → botón **"SSH"** al lado de la instancia.

### 3.2 Instalar Docker

```bash
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg -y
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io -y
```

> **¿Por qué Docker y no pip directo?** El video original instala Mage con pip, pero versiones recientes de Debian (Python 3.12+) tienen incompatibilidades con dependencias antiguas de Mage (como Pillow). Docker resuelve esto usando un contenedor con el entorno exacto que Mage necesita, independientemente de la versión del sistema.

### 3.3 Arrancar Mage con Docker

```bash
mkdir -p ~/uber_project
sudo docker run -it -p 6789:6789 \
  -v $(pwd)/uber_project:/home/src/uber_project \
  mageai/mageai mage start uber_project
```

Este comando descarga la imagen de Mage (solo la primera vez, puede tardar varios minutos) y arranca el servidor en el puerto 6789. Dejarlo corriendo en esta terminal — no cerrarlo.

### 3.4 Acceder a Mage desde el navegador

Obtener la IP externa de la VM desde la consola de GCP (columna "IP externa" en la lista de instancias) y abrir en el navegador:

```
http://IP_EXTERNA:6789
```

Credenciales por defecto de la primera vez:
- Email: `admin@admin.com`
- Password: `admin`

> Recomendado: cambiar la contraseña desde Settings → Users después del primer login.

### 3.5 Copiar el archivo `io_config.yaml` al proyecto

El archivo de configuración de Mage no se genera automáticamente dentro del proyecto. Copiarlo desde la instalación del contenedor:

```bash
# En una terminal SSH nueva (sin cerrar la que tiene Mage corriendo)
sudo docker exec -it $(sudo docker ps -q --filter ancestor=mageai/mageai) \
  cp /usr/local/lib/python3.10/site-packages/mage_ai/data_preparation/templates/repo/io_config.yaml \
  /home/src/uber_project/io_config.yaml
```

---

## Paso 4 — Configurar credenciales de Google Cloud en Mage

### 4.1 Editar `io_config.yaml`

En la interfaz de Mage, abrir el archivo `io_config.yaml` desde el panel de archivos (izquierda). Encontrar el bloque `GOOGLE_SERVICE_ACC_KEY` dentro del perfil `default` y completar con los valores del archivo JSON descargado en el paso 2.5:

```yaml
default:
  GOOGLE_SERVICE_ACC_KEY:
    type: service_account
    project_id: "tu-project-id"
    private_key_id: "tu-private-key-id"
    private_key: "-----BEGIN PRIVATE KEY-----\nTU_CLAVE_LARGA\n-----END PRIVATE KEY-----\n"
    client_email: "tu-service-account@tu-project.iam.gserviceaccount.com"
    client_id: "tu-client-id"
    auth_uri: "https://accounts.google.com/o/oauth2/auth"
    token_uri: "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url: "tu-cert-url"
  GOOGLE_SERVICE_ACC_KEY_FILEPATH: "/path/to/your/service/account/key.json"
  GOOGLE_LOCATION: US
```

> Importante: no incluir coma al final de la última línea del bloque YAML (a diferencia de JSON). El campo `private_key` debe copiarse exactamente como aparece en el JSON, con los `\n` incluidos como texto.

---

## Paso 5 — Armar el pipeline en Mage

### 5.1 Crear el pipeline

1. Desde la pantalla principal de Mage → **"New pipeline"**
2. Nombre: `uber_data_pipeline`
3. Tipo: **Standard (batch)**

### 5.2 Bloque 1 — Data Loader

Agregar un bloque de tipo **Data Loader → Python → Generic** y reemplazar el código de plantilla por:

```python
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(*args, **kwargs):
    url = 'https://storage.googleapis.com/TU_BUCKET/uber_data.csv'
    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
```

> Reemplazar `TU_BUCKET` por el nombre real del bucket (ej: `uber-project-darwin-2026`). Para que la URL pública funcione, el objeto debe tener acceso público habilitado, o usar las credenciales del `io_config.yaml` con el conector nativo de GCS de Mage.

### 5.3 Bloque 2 — Transformer

Agregar un bloque de tipo **Transformer → Python** conectado al Data Loader. Código completo:

```python
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(df, *args, **kwargs):
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day',
                                  'pick_month', 'pick_year', 'pick_weekday', 'tpep_dropoff_datetime',
                                  'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

    rate_code_type = {1:"Standard rate", 2:"JFK", 3:"Newark",
                      4:"Nassau or Westchester", 5:"Negotiated fare", 6:"Group ride"}
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']]

    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

    payment_type_name = {1:"Credit card", 2:"Cash", 3:"No charge",
                         4:"Dispute", 5:"Unknown", 6:"Voided trip"}
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    fact_table = (
        df.merge(passenger_count_dim, on='passenger_count')
          .merge(trip_distance_dim, on='trip_distance')
          .merge(rate_code_dim, on='RatecodeID')
          .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude'])
          .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude'])
          .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
          .merge(payment_type_dim, on='payment_type')
          [['VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 'rate_code_id',
            'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id',
            'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
            'improvement_surcharge', 'total_amount']]
    )

    return {"datetime_dim": datetime_dim.to_dict(orient="dict"),
            "passenger_count_dim": passenger_count_dim.to_dict(orient="dict"),
            "trip_distance_dim": trip_distance_dim.to_dict(orient="dict"),
            "rate_code_dim": rate_code_dim.to_dict(orient="dict"),
            "pickup_location_dim": pickup_location_dim.to_dict(orient="dict"),
            "dropoff_location_dim": dropoff_location_dim.to_dict(orient="dict"),
            "payment_type_dim": payment_type_dim.to_dict(orient="dict"),
            "fact_table": fact_table.to_dict(orient="dict")}

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
```

### 5.4 Bloque 3 — Data Exporter

> **Importante:** crear este bloque usando el botón "+" que aparece directamente sobre el bloque `uber_transformation` en el canvas (vista Tree), NO desde el menú de la parte inferior de la pantalla. Esto garantiza que la conexión entre bloques quede correctamente configurada desde el principio.

Seleccionar **Data Exporter → Python → Generic** y pegar este código:

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

PROJECT_ID = 'tu-project-id'        # reemplazar con tu ID de proyecto GCP
DATASET = 'uber_data_engineering'   # nombre del dataset creado en BigQuery

@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    for key, value in data.items():
        table_id = '{}.{}.{}'.format(PROJECT_ID, DATASET, key)
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            DataFrame(value),
            table_id,
            if_exists='replace',
        )
```

### 5.5 Verificar conexiones del pipeline

Antes de correr, verificar en el archivo `metadata.yaml` del pipeline que las conexiones estén bien:

```yaml
# load_uber_data debe tener solo uber_transformation como downstream
downstream_blocks:
- uber_transformation

# uber_transformation debe apuntar a uber_bq_load
downstream_blocks:
- uber_bq_load

# uber_bq_load debe tener solo uber_transformation como upstream
upstream_blocks:
- uber_transformation
```

### 5.6 Correr el pipeline

Correr los bloques en orden, uno por uno, esperando que cada uno termine:
1. `load_uber_data`
2. `uber_transformation`
3. `uber_bq_load`

Al finalizar correctamente, deben aparecer exactamente **8 mensajes** de "Exporting data to table..." con los nombres de las dimensiones y la fact_table.

---

## Paso 6 — Query de análisis en BigQuery

Una vez que el pipeline cargó las 8 tablas a BigQuery, correr esta query en el editor de BigQuery para crear la tabla analítica final (JOIN de todas las dimensiones con la fact_table):

```sql
CREATE OR REPLACE TABLE `tu-project-id.uber_data_engineering.tbl_analytics` AS (
  SELECT
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    pu.pickup_latitude,
    pu.pickup_longitude,
    dl.dropoff_latitude,
    dl.dropoff_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
  FROM `tu-project-id.uber_data_engineering.fact_table` f
  JOIN `tu-project-id.uber_data_engineering.datetime_dim`         d   ON f.datetime_id         = d.datetime_id
  JOIN `tu-project-id.uber_data_engineering.passenger_count_dim`  p   ON f.passenger_count_id  = p.passenger_count_id
  JOIN `tu-project-id.uber_data_engineering.trip_distance_dim`    t   ON f.trip_distance_id    = t.trip_distance_id
  JOIN `tu-project-id.uber_data_engineering.rate_code_dim`        r   ON f.rate_code_id        = r.rate_code_id
  JOIN `tu-project-id.uber_data_engineering.pickup_location_dim`  pu  ON f.pickup_location_id  = pu.pickup_location_id
  JOIN `tu-project-id.uber_data_engineering.dropoff_location_dim` dl  ON f.dropoff_location_id = dl.dropoff_location_id
  JOIN `tu-project-id.uber_data_engineering.payment_type_dim`     pay ON f.payment_type_id     = pay.payment_type_id
);
```

Reemplazar `tu-project-id` por el ID real del proyecto GCP.

---

## Paso 7 — Dashboard en Looker Studio

1. Ir a https://lookerstudio.google.com
2. Hacer clic en **"Crear"** → **"Informe"**
3. Conectar fuente de datos: elegir **"BigQuery"**
4. Seleccionar el proyecto → dataset `uber_data_engineering` → tabla `tbl_analytics`
5. Hacer clic en **"Agregar al informe"**
6. Armar las visualizaciones usando los campos disponibles de la tabla analítica. Algunas ideas:
   - Gráfico de barras: `total_amount` por `rate_code_name`
   - Mapa de puntos: usando `pickup_latitude` y `pickup_longitude`
   - Tabla resumen: `payment_type_name` con conteo de viajes y suma de `tip_amount`
   - Métrica: promedio de `fare_amount` total
7. Compartir el dashboard: botón **"Compartir"** → generar enlace público

---

## Cómo apagar y retomar el proyecto

### Para apagar

```bash
# En la terminal SSH de la VM:
sudo docker stop compassionate_feistel

# Desde la consola de GCP:
# Compute Engine → Instancias de VM → Seleccionar instancia → "Detener"
# (esto evita que siga consumiendo crédito mientras no se usa)
```

### Para retomar

```bash
# 1. Iniciar la VM desde la consola de GCP → botón "Iniciar"
# 2. Conectarse por SSH
# 3. Verificar si el contenedor existe:
sudo docker ps -a

# 4a. Si está detenido (status "Exited"):
sudo docker start compassionate_feistel

# 4b. Si no existe (fue eliminado):
cd ~/uber_project
sudo docker run -it -p 6789:6789 \
  -v $(pwd):/home/src/uber_project \
  mageai/mageai mage start uber_project

# 5. Obtener la nueva IP externa desde la consola de GCP
# (puede cambiar cada vez que se inicia la VM)
# 6. Abrir en el navegador: http://IP_EXTERNA:6789
```

---

## Stack tecnológico

| Herramienta | Uso |
|---|---|
| Python / pandas | Transformación de datos y modelado dimensional |
| Jupyter Notebook | Desarrollo y prueba local del script ETL |
| WSL (Ubuntu) | Entorno local de desarrollo |
| Google Cloud Storage | Almacenamiento del CSV crudo |
| Google Compute Engine | VM para correr Mage |
| Docker | Contenedor para Mage (evita problemas de compatibilidad con Python 3.13) |
| Mage AI | Orquestación del pipeline ETL (load → transform → export) |
| Google BigQuery | Data warehouse para almacenar y consultar las tablas del star schema |
| Looker Studio | Visualización y dashboard final |

---

## Referencias

- Video tutorial original: https://www.youtube.com/watch?v=WpQECq5Hx9g
- Repo del proyecto: https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project
- Documentación de Mage: https://docs.mage.ai
- Dataset TLC NYC: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
