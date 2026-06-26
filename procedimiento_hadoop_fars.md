# Procedimiento: Carga de Datos FARS al Clúster Hadoop

**Fecha:** 2026-06-26  
**Usuario:** kali (WSL) → debian (clúster)  
**Dataset:** FARS (Fatality Analysis Reporting System) - Accidentes de tráfico 2015-2016

> 📂 **Repositorio:** [github.com/Triluxxx/big_data](https://github.com/Triluxxx/big_data)  
> 📖 **Documentos complementarios:** [README.md](README.md) | [CASO_DE_ESTUDIO_FARS.md](CASO_DE_ESTUDIO_FARS.md) | [COMANDOS.md](COMANDOS.md)

---

## 1. Infraestructura del Clúster

### 1.1 Topología

| Rol | IP | Hostname | Servicios |
|-----|-----|----------|-----------|
| Master | `192.168.88.60` | `master` | NameNode, ResourceManager |
| Worker 1 | `192.168.88.61` | `worker1` | DataNode, NodeManager |
| Worker 2 | `192.168.88.62` | `worker2` | DataNode, NodeManager |
| Worker 3 | `192.168.88.63` | `worker3` | DataNode, NodeManager |

### 1.2 Versiones de Software

- **Hadoop:** 3.3.6
- **Java (JDK):** Ubicado en `/opt/hadoop/jdk/`
- **Sistema Operativo:** Debian (en todas las máquinas)
- **Autenticación:** Usuario `debian` / contraseña `debian`

### 1.3 Configuración Relevante

**core-site.xml:**
```xml
<property><name>fs.defaultFS</name><value>hdfs://192.168.88.60:9000</value></property>
```

**hdfs-site.xml:**
```xml
<property><name>dfs.replication</name><value>3</value></property>
<property><name>dfs.namenode.name.dir</name><value>/opt/hadoop/datos/namenode</value></property>
<property><name>dfs.datanode.data.dir</name><value>/opt/hadoop/datos/datanode</value></property>
```

**workers:**
```
192.168.88.61
192.168.88.62
192.168.88.63
```

### 1.4 Estado del Clúster al Inicio

- **Capacidad total HDFS:** 57.61 GB
- **Espacio disponible:** 39.66 GB
- **DataNodes vivos:** 3/3
- **YARN NodeManagers:** 3/3 RUNNING
- **Bloques corruptos:** 0
- **Bloques under-replicated:** 24 (preexistentes, no relacionados con esta operación)

---

## 2. Datos a Cargar

### 2.1 Archivos Fuente

Ubicación en máquina local (Kali WSL): `/home/kali/big data/`

| Archivo | Tamaño | Registros |
|---------|--------|-----------|
| `fars-2015-accidents (1).csv` | 4.66 MB (4,883,056 bytes) | 32,166 + header |
| `fars-2016-accidents.csv` | 5.60 MB (5,866,863 bytes) | 34,748 + header |

**Total:** 66,916 registros (sin contar headers)

### 2.2 Esquema de los Datos

Ambos archivos comparten las mismas 55 columnas. La primera columna es `year` (año del accidente). Columnas principales:

```
year, state_code, case_number, total_vehicles, vehicle_forms_submitted,
parked_working_vehicles, pedestrians, total_persons,
persons_in_motor_vehicles, persons_not_in_motor_vehicles,
county_code, city_code, day, month, crash_year, day_of_week,
hour, minute, national_highway_system, rural_urban,
functional_system, road_owner, route_type, trafficway_id,
trafficway_id_2, milepoint, latitude, longitude,
special_jurisdiction, harmful_event, manner_of_collision,
relation_to_junction_1, relation_to_junction_2,
type_of_intersection, work_zone, relation_to_road,
light_condition, weather_condition_1, weather_condition_2,
weather_condition, school_bus, rail_grade_crossing,
notification_hour, notification_minute, arrival_hour,
arrival_minute, hospital_hour, hospital_minute,
crash_factor_1, crash_factor_2, crash_factor_3,
fatalities, drunk_drivers
```

---

## 3. Procedimiento de Carga

### 3.1 Paso 1: Copiar archivos de Kali al Master

Los archivos se encontraban en la máquina local Kali (WSL) y se transfirieron al nodo master mediante SCP con `sshpass` para automatizar la autenticación.

**Comandos ejecutados:**

```bash
sshpass -p 'debian' scp -o StrictHostKeyChecking=no \
  "/home/kali/big data/fars-2015-accidents (1).csv" \
  debian@192.168.88.60:/tmp/fars_2015.csv

sshpass -p 'debian' scp -o StrictHostKeyChecking=no \
  "/home/kali/big data/fars-2016-accidents.csv" \
  debian@192.168.88.60:/tmp/fars_2016.csv
```

**Nota:** Se renombraron los archivos en destino para eliminar espacios y paréntesis del nombre original (`fars-2015-accidents (1).csv` → `fars_2015.csv`), evitando posibles problemas con caracteres especiales en HDFS.

**Resultado:** Transferencia exitosa. Sin errores.

### 3.2 Paso 2: Configurar Variables de Entorno en el Master

Antes de ejecutar comandos Hadoop, fue necesario configurar las variables de entorno porque no estaban definidas en el perfil del usuario `debian`.

**Error encontrado:** Los comandos `hadoop` y `hdfs` no se encontraban en el PATH del sistema. Java tampoco estaba en el PATH.

**Solución:** Se localizaron las rutas correctas:
- `hadoop` y `hdfs`: `/opt/hadoop/bin/`
- `JAVA_HOME`: `/opt/hadoop/jdk/`

**Comando de configuración usado en cada sesión:**

```bash
export JAVA_HOME=/opt/hadoop/jdk
export PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
```

### 3.3 Paso 3: Crear Estructura de Directorios en HDFS

Se utilizó **particionado por año** (estilo Hive partitions) como estrategia de organización lógica. Esto permite consultar/procesar datos por año sin leer archivos innecesarios.

**Decisión de diseño:** Se eligió la nomenclatura `anio=YYYY` como nombre de directorio para mantener compatibilidad con el estándar de particiones de Hive/Spark, aunque en este clúster solo está instalado Hadoop puro. Esto deja los datos preparados para futuras herramientas.

**Comandos ejecutados:**

```bash
hdfs dfs -mkdir -p /user/debian/fars/anio=2015
hdfs dfs -mkdir -p /user/debian/fars/anio=2016
```

**Resultado:** Directorios creados exitosamente bajo `/user/debian/fars/`.

### 3.4 Paso 4: Subir Archivos a HDFS

**Comandos ejecutados:**

```bash
hdfs dfs -put /tmp/fars_2015.csv /user/debian/fars/anio=2015/
hdfs dfs -put /tmp/fars_2016.csv /user/debian/fars/anio=2016/
```

**Resultado:** Ambos archivos se cargaron exitosamente.

### 3.5 Paso 5: Verificación

#### 5a. Estructura de directorios

```bash
hdfs dfs -ls -R /user/debian/fars/
```

Salida:
```
drwxr-xr-x   - debian supergroup          0 2026-06-26 00:37 /user/debian/fars/anio=2015
-rw-r--r--   3 debian supergroup    4883056 2026-06-26 00:37 /user/debian/fars/anio=2015/fars_2015.csv
drwxr-xr-x   - debian supergroup          0 2026-06-26 00:37 /user/debian/fars/anio=2016
-rw-r--r--   3 debian supergroup    5866863 2026-06-26 00:37 /user/debian/fars/anio=2016/fars_2016.csv
```

#### 5b. Distribución de Bloques

```bash
hdfs fsck /user/debian/fars/ -files -blocks -locations
```

Resultados:

| Archivo | Tamaño | Bloques | Réplicas | DataNodes |
|---------|--------|---------|----------|-----------|
| `fars_2015.csv` | 4,883,056 B | 1 bloque | 3/3 | worker1, worker2, worker3 |
| `fars_2016.csv` | 5,866,863 B | 1 bloque | 3/3 | worker2, worker3, worker1 |

Ambos archivos son menores a 128 MB (tamaño de bloque por defecto), por lo que cada uno ocupa un solo bloque HDFS. Cada bloque está replicado 3 veces, una en cada DataNode.

**Estado del filesystem:** HEALTHY
- Total bloques validados: 2
- Bloques under-replicated: 0
- Bloques corruptos: 0
- Replicación promedio: 3.0

#### 5c. Conteo de Registros

```bash
hdfs dfs -cat /user/debian/fars/anio=2015/fars_2015.csv | wc -l
hdfs dfs -cat /user/debian/fars/anio=2016/fars_2016.csv | wc -l
```

| Año | Líneas (incluye header) | Registros de datos |
|-----|--------------------------|---------------------|
| 2015 | 32,167 | 32,166 |
| 2016 | 34,749 | 34,748 |
| **Total** | **66,916** | **66,914** |

Los conteos coinciden con los archivos originales. No hubo pérdida de datos.

#### 5d. Muestreo de Contenido

Se verificaron las primeras 3 líneas de cada archivo, confirmando que:
- El header es idéntico en ambos archivos (55 columnas)
- Los datos comienzan con el año correspondiente (2015 y 2016)
- El formato CSV es consistente

### 3.6 Paso 6: Limpieza de Archivos Temporales

Se eliminaron los archivos temporales del directorio `/tmp` del master:

```bash
rm -f /tmp/fars_2015.csv /tmp/fars_2016.csv
```

---

## 4. Estructura Final en HDFS

```
/user/debian/fars/
├── anio=2015/
│   └── fars_2015.csv          (4.66 MB, 3 réplicas, 32,166 registros)
└── anio=2016/
    └── fars_2016.csv          (5.60 MB, 3 réplicas, 34,748 registros)
```

**Espacio total ocupado en HDFS:** ~33.8 MB (10.75 MB × factor de replicación 3, más metadatos)

---

## 5. Comandos Útiles para Consultar los Datos

### Leer todo el dataset (ambos años):
```bash
hdfs dfs -cat /user/debian/fars/anio=*/*.csv
```

### Leer solo un año:
```bash
hdfs dfs -cat /user/debian/fars/anio=2015/fars_2015.csv
```

### Contar registros de un año:
```bash
hdfs dfs -cat /user/debian/fars/anio=2015/fars_2015.csv | wc -l
```

### Ver primeras N líneas:
```bash
hdfs dfs -cat /user/debian/fars/anio=2016/fars_2016.csv | head -10
```

### Copiar un archivo de HDFS a local:
```bash
hdfs dfs -get /user/debian/fars/anio=2015/fars_2015.csv /tmp/
```

---

## 6. Errores Encontrados y Soluciones

### Error 1: Comandos Hadoop no encontrados
- **Síntoma:** `hadoop: command not found`, `hdfs: command not found`
- **Causa:** Las variables de entorno `PATH` y `JAVA_HOME` no estaban configuradas en la sesión del usuario `debian`.
- **Solución:** Localizar las rutas de instalación (`/opt/hadoop/bin/`, `/opt/hadoop/jdk/`) y exportarlas manualmente en cada sesión SSH.

### Error 2: Java no encontrado
- **Síntoma:** `java: command not found` al intentar ejecutar herramientas Hadoop.
- **Causa:** El JDK está instalado en una ruta no estándar (`/opt/hadoop/jdk/`) y no está en el PATH del sistema.
- **Solución:** Establecer `JAVA_HOME=/opt/hadoop/jdk` antes de ejecutar cualquier comando Hadoop.

### Error 3: Nombre de archivo con espacios y paréntesis
- **Síntoma:** El archivo original se llamaba `fars-2015-accidents (1).csv`.
- **Causa:** Los espacios y paréntesis pueden causar problemas en comandos de shell y en HDFS.
- **Solución:** Se renombró a `fars_2015.csv` durante la transferencia SCP, usando underscores en lugar de espacios y eliminando el sufijo `(1)`.

### Error 4: Timeout en `hdfs dfs -cat` sobre archivos remotos
- **Síntoma:** El comando `hdfs dfs -cat` sobre el archivo de 2016 excedió el timeout de 20 segundos durante la verificación inicial.
- **Causa:** La operación de lectura sobre HDFS con salida a stdout puede ser lenta cuando se hace a través de múltiples capas de red (WSL → red → master → DataNodes).
- **Solución:** Se re-ejecutó el comando con un timeout más amplio (30 segundos), completándose exitosamente. Para operaciones de lectura masiva se recomienda usar `hdfs dfs -get` (copia a local) en lugar de `-cat`.

---

## 7. Resumen de la Sesión

| Paso | Acción | Herramienta | Resultado |
|------|--------|-------------|-----------|
| 1 | Descubrimiento del clúster | `ssh`, `ping`, `nmap` | 4 máquinas localizadas, Hadoop 3.3.6 |
| 2 | Diagnóstico de entorno | `find`, `which` | JAVA_HOME y PATH no configurados |
| 3 | Corrección de entorno | `export` | Variables configuradas manualmente |
| 4 | Transferencia de datos | `scp` + `sshpass` | 2 archivos CSV copiados al master |
| 5 | Creación de estructura HDFS | `hdfs dfs -mkdir` | Particionado `anio=YYYY` creado |
| 6 | Carga a HDFS | `hdfs dfs -put` | 10.75 MB de datos cargados |
| 7 | Verificación de integridad | `hdfs fsck`, `hdfs dfs -cat`, `wc -l` | Datos íntegros, 3 réplicas, 0 errores |
| 8 | Limpieza | `rm` | Archivos temporales eliminados |
| 9 | Dashboard | Streamlit + Pandas + Matplotlib | Dashboard interactivo funcionando en puerto 8501 |

---

## 8. Dashboard Interactivo con Streamlit

### 8.1 Herramientas Utilizadas

| Herramienta | Versión | Propósito |
|-------------|---------|-----------|
| Streamlit | 1.57.0 | Framework web para el dashboard |
| Pandas | 3.0.2 | Carga y manipulación de datos |
| Matplotlib | 3.10.9 | Generación de gráficos |
| PyArrow | 24.0.0 | Dependencia de Pandas para CSV |

**Nota:** No se instalaron herramientas adicionales. Se usaron las ya disponibles en el sistema Kali.

### 8.2 Arquitectura de Lectura de Datos

El dashboard se ejecuta en la máquina Kali (WSL) y lee los datos desde HDFS mediante SSH al nodo master:

```
Kali (Streamlit) ──SSH──▶ Master (192.168.88.60)
                              │
                              └── hdfs dfs -cat ──▶ DataNodes (workers 1-3)
```

**Decisión de diseño:** Se evaluó usar la API WebHDFS (puerto 9870) para leer directamente, pero se descartó porque:
- WebHDFS redirige a los DataNodes usando hostnames (`worker1`, `worker2`, `worker3`)
- Estos hostnames no son resolubles desde la máquina Kali (WSL)
- La alternativa de agregar entradas en `/etc/hosts` requeriría modificar la configuración de red

**Solución implementada:** Lectura vía `subprocess` + `sshpass` ejecutando `hdfs dfs -cat` en el master. Los datos se transmiten por stdout y se parsean con `pandas.read_csv()`.

### 8.3 Funcionalidades del Dashboard

El dashboard final (`dashboard_fars_v2.py`) incluye:

| Sección | Visualización | Tipo de Gráfico |
|---------|---------------|-----------------|
| Métricas generales | Total accidentes, fatalidades, vehículos, personas | Tarjetas numéricas |
| Comparación 2015 vs 2016 | Accidentes, fatalidades, vehículos, personas por año | Barras comparativas (2x2) |
| Rural vs Urbano | Distribución porcentual | Gráfico de torta |
| Top 15 Estados | Estados con más fatalidades | Barras horizontales |
| Mapa de calor horario | Accidentes por día de semana × hora | Heatmap |
| Tendencia mensual | Accidentes por mes (2015 y 2016) | Barras |
| Condición de luz | Accidentes por iluminación | Barras |
| Mapa geográfico | Dispersión lat/lon coloreado por fatalidades | Scatter plot |
| Conductores ebrios | Distribución de drunk_drivers por año | Barras |
| Datos crudos | Tabla interactiva con 100 filas de muestra | Dataframe |

### 8.4 Filtros Interactivos

- **Año:** Todos / 2015 / 2016
- **Estado:** Todos los 50 estados + DC

Los filtros se aplican en la barra lateral y afectan a todas las visualizaciones simultáneamente.

### 8.5 Caching

Se utiliza `@st.cache_data` con TTL de 1 hora para:
- `read_hdfs_csv()`: Evita re-leer HDFS en cada interacción
- `load_all_data()`: Combina ambos años una sola vez

### 8.6 Ejecución

```bash
cd "/home/kali/big data"
streamlit run dashboard_fars_v2.py --server.port 8501
```

**URLs de acceso:**
- Local: `http://localhost:8501`
- Red: `http://172.28.21.237:8501`
- Público: vía `npx localtunnel --port 8501`

### 8.7 Códigos de Datos Mapeados

El dashboard incluye mapeos de códigos numéricos a etiquetas legibles:

- **state_code** → Nombre del estado (1=Alabama, ..., 56=Wyoming)
- **rural_urban** → 1=Urbano, 2=Rural (códigos FARS reales)
- **light_condition** → 1=Luz de día, 2=Oscuro sin luz, etc.
- **day_of_week** → 1=Domingo, ..., 7=Sábado
- **month** → 1=Ene, ..., 12=Dic

### 8.8 Errores Encontrados en el Dashboard

#### Error 5: WebHDFS redirect a hostnames no resolubles
- **Síntoma:** `curl -L` a WebHDFS quedaba en timeout porque el redirect apuntaba a `http://worker1:9864/...`
- **Causa:** El NameNode redirige las lecturas a los DataNodes usando sus hostnames, que no existen en el DNS de Kali/WSL
- **Solución:** Se implementó lectura vía SSH en lugar de WebHDFS

#### Error 6: Columnas desplazadas en comandos cut
- **Síntoma:** Al explorar los datos con `cut -d',' -fN`, algunas columnas mostraban valores que no correspondían
- **Causa:** Error humano al mapear números de columna (el índice 0-based vs 1-based, y confusión entre nombres de columnas similares)
- **Solución:** Se usó Pandas para el dashboard, que maneja las columnas por nombre, eliminando el problema

---

## 9. Instalación y Configuración de Apache Spark

### 9.1 Instalación

Se descargó e instaló Apache Spark 3.5.8 en el nodo master para realizar análisis avanzados de correlación y machine learning sobre los datos FARS.

**Comando de descarga:**
```bash
cd /tmp
wget --no-check-certificate \
  https://archive.apache.org/dist/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz
tar xzf spark-3.5.8-bin-hadoop3.tgz
mv spark-3.5.8-bin-hadoop3 /home/debian/spark
```

**Tamaño:** 383 MB comprimido

### 9.2 Errores Encontrados en la Instalación

#### Error 7: Permiso denegado en /opt
- **Síntoma:** `wget` a `/opt/spark.tgz` falló con "Permiso denegado"
- **Causa:** El usuario `debian` no tiene permisos de escritura en `/opt`
- **Solución:** Se descargó en `/tmp` y se instaló en `/home/debian/spark`

#### Error 8: sudo requiere terminal
- **Síntoma:** `sudo mv` falló con "a terminal is required to read the password"
- **Causa:** El usuario `debian` no tiene sudo sin contraseña configurado
- **Solución:** Se instaló Spark en el home del usuario (`/home/debian/spark`) sin requerir sudo

#### Error 9: JAVA_HOME no configurado para Spark
- **Síntoma:** `spark-submit --version` mostraba "JAVA_HOME is not set"
- **Causa:** Spark no hereda automáticamente las variables de entorno
- **Solución:** Se configuró `spark-env.sh` con `export JAVA_HOME=/opt/hadoop/jdk`

### 9.3 Configuración

**spark-env.sh:**
```bash
export JAVA_HOME=/opt/hadoop/jdk
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=192.168.88.60
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=1g
```

**workers:**
```
192.168.88.61
192.168.88.62
192.168.88.63
```

**spark-defaults.conf:**
```
spark.master                     spark://192.168.88.60:7077
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://192.168.88.60:9000/spark-libs
```

### 9.4 Versiones Instaladas

| Componente | Versión |
|------------|---------|
| Apache Spark | 3.5.8 |
| Scala | 2.12.18 |
| Hadoop (cliente) | 3.3.6 |
| Java | OpenJDK 1.8.0_442 (Temurin) |
| Python | 3.13.5 |
| NumPy | 2.5.0 |
| Pandas | 3.0.3 |

---

## 10. Análisis de Correlación con PySpark

### 10.1 Objetivo

Identificar qué factores más incrementan el **número de fatalidades por accidente** usando técnicas estadísticas y machine learning.

**Nota importante:** El dataset FARS solo contiene accidentes con al menos 1 fatalidad. Por tanto, el análisis se enfoca en qué hace que un accidente tenga **más** víctimas mortales, no en si es fatal o no.

### 10.2 Errores Encontrados en PySpark

#### Error 10: Módulo numpy no encontrado
- **Síntoma:** `ModuleNotFoundError: No module named 'numpy'` al ejecutar PySpark
- **Causa:** Python 3.13 en Debian no incluye numpy por defecto; PySpark ML requiere numpy
- **Solución:** Instalado con `pip3 install --break-system-packages numpy pandas`

#### Error 11: Módulo distutils no encontrado
- **Síntoma:** `ModuleNotFoundError: No module named 'distutils'`
- **Causa:** Python 3.13 eliminó `distutils`; Spark 3.5.8 tiene una dependencia residual en `pyspark.ml.image`
- **Solución:** Instalado `setuptools` que proporciona compatibilidad (`pip3 install --break-system-packages setuptools`)

#### Error 12: Chi-cuadrado con valores incorrectos (v1)
- **Síntoma:** Todos los p-values eran 1.0 o 0.0 en el primer análisis
- **Causa:** `ChiSquareTest` de Spark requiere features categóricas; se estaban pasando variables continuas sin indexar
- **Solución:** Se reemplazó el test Chi-cuadrado por análisis de medias por grupo (más interpretable para este caso)

#### Error 13: Random Forest Classifier con accuracy 100% (v1)
- **Síntoma:** El modelo clasificador daba 100% de precisión y 0% de importancia de features
- **Causa:** Se usó `is_fatal` como variable objetivo binaria, pero el 100% de los registros FARS son fatales (is_fatal=1 siempre)
- **Solución:** Se cambió a `RandomForestRegressor` prediciendo el número de fatalidades (0-3+) como variable continua

### 10.3 Script Final (v2)

El script `analisis_correlacion_v2.py` realiza:

1. **Carga desde HDFS** de ambos años (66,914 registros)
2. **Correlación de Pearson** entre 14 variables y número de fatalidades
3. **Análisis por grupos** (media de fatalidades por categoría)
4. **Random Forest Regressor** (50 árboles, maxDepth=10) para medir importancia predictiva
5. **Exportación de resultados** a JSON en HDFS y local

### 10.4 Resultados del Análisis

#### Correlaciones Pearson (Top 5)

| Variable | Pearson r | Interpretación |
|----------|-----------|----------------|
| Personas involucradas | +0.2946 | 🟡 Moderada |
| Vehículos involucrados | +0.1126 | 🟡 Moderada |
| Conductores ebrios | +0.0501 | 🟢 Débil |
| Peatones | -0.0484 | ⚪ Mínima |
| Tipo de colisión | +0.0381 | ⚪ Mínima |

#### Importancia Random Forest (Top 5)

| Variable | Importancia | % |
|----------|-------------|---|
| total_persons | 0.4060 | 40.6% |
| hour | 0.1025 | 10.2% |
| month | 0.0726 | 7.3% |
| manner_of_collision | 0.0693 | 6.9% |
| pedestrians | 0.0631 | 6.3% |

#### Rendimiento del Modelo

- **RMSE:** 0.3438 (error promedio al predecir número de fatalidades)
- **R²:** 0.1417 (el modelo explica el 14.2% de la varianza)

#### Hallazgo Principal: Conductores Ebrios

| Conductores ebrios | Accidentes | Media fatalidades | Incremento |
|---------------------|------------|-------------------|------------|
| 0 | 48,750 | 1.080 | Línea base |
| 1 | 17,642 | 1.108 | +2.6% |
| 2 | 516 | 1.310 | +21.3% |
| 3 | 6 | 1.833 | +69.7% |

### 10.5 Ejecución

```bash
cd /home/debian
export JAVA_HOME=/opt/hadoop/jdk
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
/home/debian/spark/bin/spark-submit \
  --master 'local[2]' \
  --driver-memory 1g \
  analisis_correlacion_v2.py
```

---

## 11. Dashboard v2 - Factores de Fatalidad

### 11.1 Actualización

El dashboard fue actualizado (`dashboard_fars_v2.py`) para incorporar los resultados del análisis Spark y presentarlos como un informe ejecutivo interactivo.

### 11.2 Nuevas Funcionalidades

| Sección | Descripción |
|---------|-------------|
| **Factor #1: Alcohol** | Gráfico de barras + pie chart mostrando el efecto multiplicador de conductores ebrios |
| **Factor #2: Luz** | Barras horizontales con tasa de fatalidad por condición de iluminación |
| **Factor #3: Rural/Urbano** | Comparativa de medias y distribución entre zonas |
| **Factor #4: Hora** | Gráfico dual (barras + línea) con franja de hora pico sombreada |
| **Importancia RF** | Top 10 features del modelo Random Forest |
| **Correlaciones Pearson** | Todas las correlaciones ordenadas con código de colores |
| **Metodología** | Expander con detalles del modelo, infraestructura y limitaciones |

### 11.3 Diseño

- **Layout corporativo:** Colores semánticos (verde=seguro, rojo=peligro, naranja=advertencia)
- **Hallazgos accionables:** Cada gráfico va acompañado de interpretación y recomendación
- **Filtros interactivos:** Año y estado en sidebar
- **Caché:** Datos desde HDFS + resultados Spark cacheados por 1 hora

### 11.4 Ejecución

```bash
cd "/home/kali/big data"
streamlit run dashboard_fars_v2.py --server.port 8501
```

---

## 12. Informe Ejecutivo

Se generó un informe ejecutivo independiente (`informe_ejecutivo_fars.md`) que sintetiza los hallazgos para presentar a una empresa o aseguradora.

**Contenido:**
1. Resumen ejecutivo con hallazgo principal
2. Metodología (infraestructura, técnicas, rendimiento del modelo)
3. Resultados detallados por factor
4. Recomendaciones priorizadas (Alta/Media/Baja)
5. Limitaciones del análisis
6. Próximos pasos sugeridos

---

## 13. Archivos Generados

| Archivo | Ubicación | Descripción |
|---------|-----------|-------------|
| `procedimiento_hadoop_fars.md` | Kali: `/home/kali/big data/` | Esta documentación completa |
| `informe_ejecutivo_fars.md` | Kali: `/home/kali/big data/` | Informe para la empresa |
| `dashboard_fars_v2.py` | Kali: `/home/kali/big data/` | Dashboard Streamlit v2 |
| `analisis_correlacion_v2.py` | Kali: `/home/kali/big data/` | Script PySpark de análisis |
| `resultados_analisis.json` | Kali: `/home/kali/big data/` | Resultados del análisis Spark |
| `resultados_analisis.json` | Master: `/home/debian/` | Copia local en el clúster |
| `resultados_analisis_v2.json` | HDFS: `/user/debian/fars/` | Copia en HDFS |
| `fars_2015.csv` | HDFS: `/user/debian/fars/anio=2015/` | Datos 2015 |
| `fars_2016.csv` | HDFS: `/user/debian/fars/anio=2016/` | Datos 2016 |

---

## 14. Resumen Final de la Sesión

> 📖 **Documentos complementarios:**  
> - [README.md](README.md) — Índice y arquitectura del repositorio  
> - [CASO_DE_ESTUDIO_FARS.md](CASO_DE_ESTUDIO_FARS.md) — Documento formal del caso  
> - [COMANDOS.md](COMANDOS.md) — Todos los comandos en orden del flujo

| Paso | Acción | Herramienta | Resultado |
|------|--------|-------------|-----------|
| 1 | Descubrimiento del clúster | `ssh`, `ping` | 4 máquinas, Hadoop 3.3.6 |
| 2 | Diagnóstico de entorno | `find`, `which` | JAVA_HOME y PATH no configurados |
| 3 | Corrección de entorno | `export` | Variables configuradas |
| 4 | Transferencia de datos | `scp` + `sshpass` | 2 CSVs copiados al master |
| 5 | Creación de estructura HDFS | `hdfs dfs -mkdir` | Particionado `anio=YYYY` |
| 6 | Carga a HDFS | `hdfs dfs -put` | 10.75 MB cargados, réplica x3 |
| 7 | Verificación de integridad | `hdfs fsck` | HEALTHY, 0 errores |
| 8 | Dashboard v1 | Streamlit | Visualización inicial |
| 9 | Instalación Spark 3.5.8 | `wget`, `tar` | Spark en `/home/debian/spark` |
| 10 | Configuración Spark | `spark-env.sh`, `workers` | Modo standalone listo |
| 11 | Análisis correlación v1 | PySpark | Errores: distutils, Chi², RF classifier |
| 12 | Corrección de errores | `pip3 install`, refactor | numpy, setuptools, RF regressor |
| 13 | Análisis correlación v2 | PySpark | R²=0.14, factores identificados |
| 14 | Informe ejecutivo | Markdown | Documento para empresa |
| 15 | Dashboard v2 | Streamlit | Dashboard ejecutivo con hallazgos Spark |
| 16 | Exposición pública | Localtunnel | Túnel público para acceso remoto |
| 17 | Documentación final | Markdown | 3 MDs + README cross-referenciados |
