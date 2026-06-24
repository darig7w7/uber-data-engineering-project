# Uber Data Engineering Project
### End-to-End ETL Pipeline con GCP, Mage, BigQuery y Looker Studio

Proyecto de ingeniería de datos basado en el tutorial de Darshil Parmar. Procesa datos de viajes de taxi de NYC (Yellow Taxi, marzo 2016) aplicando un modelo dimensional (esquema en estrella) para análisis en BigQuery y visualización en Looker Studio.

---

## Arquitectura general

<img src="docs/Arquitectura.png">

---
## Ideas de negocio - Plataforma de analítica para empresas de transporte
### Problema:

Muchas empresas de taxis, colectivos, delivery o transporte privado tienen datos pero no saben:

¿Cuáles son sus rutas más rentables?
¿En qué horarios hay mayor demanda?
¿Qué conductores generan más ingresos?
¿Dónde pierden dinero?
### Solución:
Crear un servicio "Data Analytics para transporte":
**Fuente de datos**
GPS de vehículos
Aplicación móvil de conductores
Sistema de pagos
Registro de viajes
## Dataset

- **Fuente:** TLC Trip Record Data — Yellow Taxi, marzo 2016
- **Archivo:** `uber_data.csv` (100.000 filas, 19 columnas)
- **Descarga:** https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project/blob/main/data/uber_data.csv
- **Documentación oficial TLC:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

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
- Python 3.x instalado localmente
- WSL (Ubuntu), Linux o Mac para la terminal local
- Docker instalado en la VM de GCP

---

## Paso 1 — Preparación local (notebook)

### 1.1 Clonar o crear la carpeta del proyecto

```bash
git clone https://github.com/TU_USUARIO/uber-data-engineering-project.git
cd uber-data-engineering-project
```

O simplemente crear la carpeta donde se prefiera:

```bash
mkdir uber-data-engineering-project
cd uber-data-engineering-project
```

### 1.2 Crear entorno virtual y activarlo

```bash
python3 -m venv venv
source venv/bin/activate   # Linux/Mac/WSL
# venv\Scripts\activate   # Windows
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
jupyter notebook
```

Correr el notebook celda por celda con `Shift+Enter` para validar que las 8 tablas se generan correctamente antes de pasar a la nube.

> Si se usa WSL, agregar el flag `--no-browser` y copiar la URL con el token que aparece en la terminal para abrirla en el navegador de Windows.

---

## Paso 2 — Google Cloud Platform

### 2.1 Crear proyecto en GCP

1. Ir a https://console.cloud.google.com
2. Hacer clic en el selector de proyectos → **"Nuevo proyecto"**
3. Asignar un nombre descriptivo (ej: `uber-data-project`) → **Crear**
4. Confirmar que el proyecto nuevo esté seleccionado antes de continuar

### 2.2 Habilitar facturación

Necesario para usar Cloud Storage, Compute Engine y BigQuery. Google ofrece $300 de crédito gratuito por 90 días para cuentas nuevas. La verificación pide datos de tarjeta pero no realiza cobros automáticos salvo que se active manualmente una cuenta paga al terminar el período de prueba.

### 2.3 Crear bucket en Cloud Storage

1. Ir a **Cloud Storage → Buckets → Crear**
2. Nombre del bucket: debe ser único a nivel mundial (ej: `uber-data-project-2026`)
3. Ubicación: **`US (multiple regions in United States)`** — debe coincidir con la ubicación del dataset de BigQuery
4. Clase de almacenamiento: **Standard**
5. Acceso: dejar configuración por defecto (privado)
6. Hacer clic en **Crear**
7. Una vez creado, hacer clic en **"Subir archivos"** y seleccionar `uber_data.csv`

### 2.4 Crear dataset en BigQuery

1. Ir a **BigQuery → panel izquierdo → tres puntitos del proyecto → "Crear conjunto de datos"**
2. ID del conjunto de datos: `uber_data_engineering`
3. Ubicación: **`US (multiple regions in United States)`** — debe coincidir con el bucket
4. Hacer clic en **Crear conjunto de datos**

### 2.5 Crear Service Account (credenciales para Mage)

1. Ir a **IAM y administración → Cuentas de servicio → Crear cuenta de servicio**
2. Nombre: `mage-bigquery-access`
3. Rol: **BigQuery Admin**
4. Una vez creada, hacer clic en la cuenta → pestaña **"Claves"** → **"Agregar clave"** → **"Crear clave nueva"** → tipo **JSON**
5. Guardar el archivo `.json` descargado de forma segura — contiene credenciales privadas, nunca subir al repositorio

### 2.6 Crear VM en Compute Engine

1. Ir a **Compute Engine → Instancias de VM → Crear instancia**
2. Nombre: `uber-project-instance`
3. Región: `us-central1 (Iowa)`
4. Serie de máquina: **E2**, tipo: **`e2-standard-4`** (4 vCPUs, 16 GB RAM)
5. Sistema operativo: Debian GNU/Linux (por defecto)
6. En la sección **Redes → Firewall**: marcar **"Permitir tráfico HTTP"** y **"Permitir tráfico HTTPS"**
7. Hacer clic en **Crear**

### 2.7 Abrir el puerto 6789 en el Firewall

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

> **¿Por qué Docker en vez de pip?** El video original instala Mage con pip, pero versiones recientes de Debian (Python 3.12+) tienen incompatibilidades con algunas dependencias antiguas de Mage. Docker resuelve esto usando un contenedor con el entorno exacto que Mage necesita, independientemente de la versión del sistema operativo de la VM.

### 3.3 Arrancar Mage con Docker

```bash
mkdir -p ~/uber_project
cd ~
sudo docker run -it -p 6789:6789 \
  -v $(pwd)/uber_project:/home/src/uber_project \
  mageai/mageai mage start uber_project
```

La primera ejecución descarga la imagen de Mage (puede tardar varios minutos). Dejar esta terminal corriendo — es el proceso del servidor de Mage.

### 3.4 Acceder a Mage desde el navegador

Obtener la IP externa de la VM desde la consola de GCP (columna "IP externa") y abrir en el navegador:

```
http://IP_EXTERNA:6789
```

Credenciales por defecto del primer login:
- Email: `admin@admin.com`
- Password: `admin`

> Se recomienda cambiar la contraseña desde Settings → Users después del primer acceso.

### 3.5 Copiar el archivo `io_config.yaml` al proyecto

```bash
# Abrir una segunda terminal SSH (sin cerrar la que tiene Mage corriendo)
sudo docker exec -it $(sudo docker ps -q --filter ancestor=mageai/mageai) \
  cp /usr/local/lib/python3.10/site-packages/mage_ai/data_preparation/templates/repo/io_config.yaml \
  /home/src/uber_project/io_config.yaml
```

---

## Paso 4 — Configurar credenciales de Google Cloud en Mage

En la interfaz de Mage, abrir el archivo `io_config.yaml` desde el panel de archivos. Completar el bloque `GOOGLE_SERVICE_ACC_KEY` dentro del perfil `default` con los valores del archivo JSON generado en el paso 2.5:

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

> No incluir coma al final de la última línea del bloque (el formato YAML no usa comas entre campos, a diferencia de JSON). El campo `private_key` debe copiarse exactamente como aparece en el JSON, incluyendo los `\n` como texto literal.

---

## Paso 5 — Armar el pipeline en Mage

### 5.1 Crear el pipeline

1. Desde la pantalla principal de Mage → **"New pipeline"**
2. Nombre: `uber_data_pipeline`
3. Tipo: **Standard (batch)**

### 5.2 Bloque 1 — Data Loader

Agregar **Data Loader → Python → Generic** con el código de `mage-files/load.py`.

### 5.3 Bloque 2 — Transformer

Agregar **Transformer → Python** conectado al Data Loader, con el código de `mage-files/transform.py`.

### 5.4 Bloque 3 — Data Exporter

> **Importante:** crear este bloque usando el botón **"+"** que aparece directamente sobre el bloque del Transformer en la vista **Tree** (canvas), NO desde el menú de bloques de la parte inferior. Esto garantiza que la conexión entre bloques quede configurada correctamente desde el principio, evitando problemas de routing en el pipeline.

Seleccionar **Data Exporter → Python → Generic** y usar el código de `mage-files/export.py`, reemplazando `PROJECT_ID` con el ID real del proyecto GCP.

### 5.5 Verificar conexiones en `metadata.yaml`

Antes de correr el pipeline, confirmar que el archivo `metadata.yaml` (dentro de `pipelines/uber_data_pipeline/`) tenga estas conexiones:

```yaml
# load_uber_data
downstream_blocks:
- uber_transformation

# uber_transformation
downstream_blocks:
- uber_bq_load

# uber_bq_load
upstream_blocks:
- uber_transformation
```

### 5.6 Correr el pipeline

Correr los bloques en orden, uno por uno:
1. `load_uber_data`
2. `uber_transformation`
3. `uber_bq_load`

Al finalizar correctamente, aparecen exactamente **8 mensajes** de exportación con los nombres de las dimensiones y `fact_table`.

---

## Paso 6 — Query de análisis en BigQuery

Ejecutar la query de `sql/analytics_query.sql` en el editor de BigQuery, reemplazando `tu-project-id` con el ID real del proyecto:

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

---

## Paso 7 — Dashboard en Looker Studio

1. Ir a https://lookerstudio.google.com → **"Crear"** → **"Informe"**
2. Conectar fuente de datos: **BigQuery**
3. Seleccionar el proyecto → dataset `uber_data_engineering` → tabla `tbl_analytics`
4. Hacer clic en **"Agregar al informe"**
5. Armar visualizaciones con los campos disponibles:
   - Gráfico de barras: `total_amount` por `rate_code_name`
   - Mapa de puntos: `pickup_latitude` / `pickup_longitude`
   - Tabla resumen: `payment_type_name` con conteo de viajes y suma de `tip_amount`
   - Métricas: promedio de `fare_amount`, suma de `total_amount`
6. Compartir: botón **"Compartir"** → generar enlace público

---

## Estructura del repositorio

```
uber-data-engineering-project/
├── data/
│   └── uber_data.csv              # Dataset crudo (ignorado por .gitignore si es muy pesado)
├── notebook/
│   └── Uber Data Pipeline.ipynb  # Exploración y pruebas locales
├── mage-files/
│   ├── load.py                   # Bloque Data Loader de Mage
│   ├── transform.py              # Bloque Transformer de Mage
│   └── export.py                 # Bloque Data Exporter de Mage
├── sql/
│   └── analytics_query.sql       # Query final de análisis en BigQuery
├── docs/
│   └── data_dictionary.md        # Diccionario de datos del dataset
├── .gitignore
└── README.md
```

---

## Cómo apagar y retomar el proyecto

### Para apagar

```bash
# Detener el contenedor de Mage (en la terminal SSH de la VM)
sudo docker stop $(sudo docker ps -q --filter ancestor=mageai/mageai)

# Detener la VM desde la consola de GCP para evitar consumo de crédito:
# Compute Engine → Instancias de VM → Seleccionar instancia → "Detener"
```

### Para retomar

```bash
# 1. Iniciar la VM desde la consola de GCP → botón "Iniciar"
# 2. Conectarse por SSH
# 3. Verificar el estado del contenedor:
sudo docker ps -a

# Si está detenido (Exited):
sudo docker start NOMBRE_CONTENEDOR

# Si fue eliminado, recrearlo:
cd ~
sudo docker run -it -p 6789:6789 \
  -v $(pwd)/uber_project:/home/src/uber_project \
  mageai/mageai mage start uber_project

# 4. Obtener la nueva IP externa desde la consola de GCP
#    (puede cambiar con cada reinicio de la VM)
# 5. Abrir en el navegador: http://IP_EXTERNA:6789
```

---

## Stack tecnológico

| Herramienta | Uso |
|---|---|
| Python / pandas | Transformación de datos y modelado dimensional |
| Jupyter Notebook | Desarrollo y prueba local del script ETL |
| Google Cloud Storage | Almacenamiento del CSV crudo |
| Google Compute Engine | VM para correr Mage |
| Docker | Contenedor para Mage (compatibilidad con versiones modernas de Python) |
| Mage AI | Orquestación del pipeline ETL (load → transform → export) |
| Google BigQuery | Data warehouse para almacenar y consultar las tablas del star schema |
| Looker Studio | Visualización y dashboard final |

---

## Referencias

- Video tutorial original: https://www.youtube.com/watch?v=WpQECq5Hx9g
- Repo base del proyecto: https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project
- Documentación de Mage: https://docs.mage.ai
- Dataset TLC NYC: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
