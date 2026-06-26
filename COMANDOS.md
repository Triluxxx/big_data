# 🚗 FARS Big Data Pipeline — Todos los Comandos

> **Clúster:** 1 master + 3 workers | **Hadoop 3.3.6** | **Spark 3.5.8** | **Streamlit 1.58**
>
> 📂 **Repositorio:** [github.com/Triluxxx/big_data](https://github.com/Triluxxx/big_data)  
> 📖 **Documentos complementarios:** [README.md](README.md) | [CASO_DE_ESTUDIO_FARS.md](CASO_DE_ESTUDIO_FARS.md) | [procedimiento_hadoop_fars.md](procedimiento_hadoop_fars.md)
>
> Orden exacto del flujo de trabajo. Cada comando fue ejecutado y verificado.

---

## 📡 FASE 1: Conectividad y Diagnóstico

> **Objetivo:** Identificar los 4 nodos del clúster, verificar Hadoop y localizar Java.
> **Resultado:** Clúster operativo, 3 DataNodes vivos, 57.61 GB capacidad.

```bash
# Verificar que los 4 nodos responden
ping -c 1 192.168.88.60   # master
ping -c 1 192.168.88.61   # worker1
ping -c 1 192.168.88.62   # worker2
ping -c 1 192.168.88.63   # worker3

# Conectarse al master
sshpass -p 'debian' ssh -o StrictHostKeyChecking=no debian@192.168.88.60

# Diagnóstico: encontrar Hadoop y Java
find / -name 'hadoop' -type f 2>/dev/null
find / -name 'java' -type f 2>/dev/null

# Configurar entorno (necesario en cada sesión)
export JAVA_HOME=/opt/hadoop/jdk
export PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin

# Verificar Hadoop
hadoop version
hdfs dfsadmin -report
yarn node -list

# Verificar archivos de configuración
cat /opt/hadoop/etc/hadoop/workers
cat /opt/hadoop/etc/hadoop/core-site.xml
cat /opt/hadoop/etc/hadoop/hdfs-site.xml
```

---

## 📁 FASE 2: Transferencia de Datos (Kali → Master)

> **Objetivo:** Copiar los datasets FARS 2015-2016 desde Kali al nodo master.
> **Resultado:** 2 archivos CSV transferidos (10.75 MB total).

```bash
# Desde Kali WSL:
sshpass -p 'debian' scp -o StrictHostKeyChecking=no \
  "/home/kali/big data/fars-2015-accidents (1).csv" \
  debian@192.168.88.60:/tmp/fars_2015.csv

sshpass -p 'debian' scp -o StrictHostKeyChecking=no \
  "/home/kali/big data/fars-2016-accidents.csv" \
  debian@192.168.88.60:/tmp/fars_2016.csv
```

---

## 🐘 FASE 3: Carga a HDFS

> **Objetivo:** Subir datos a HDFS con particionado por año (`anio=YYYY/`).
> **Resultado:** 66,914 registros en HDFS, réplica x3, fsck HEALTHY.

```bash
# En el master:
export JAVA_HOME=/opt/hadoop/jdk
export PATH=$PATH:/opt/hadoop/bin

# Crear estructura particionada por año
hdfs dfs -mkdir -p /user/debian/fars/anio=2015
hdfs dfs -mkdir -p /user/debian/fars/anio=2016

# Subir archivos a HDFS
hdfs dfs -put /tmp/fars_2015.csv /user/debian/fars/anio=2015/
hdfs dfs -put /tmp/fars_2016.csv /user/debian/fars/anio=2016/

# Verificar estructura
hdfs dfs -ls -R /user/debian/fars/

# Verificar tamaño
hdfs dfs -du -h /user/debian/fars/

# Verificar integridad y distribución de bloques
hdfs fsck /user/debian/fars/ -files -blocks -locations

# Verificar contenido
hdfs dfs -cat /user/debian/fars/anio=2015/fars_2015.csv | head -3
hdfs dfs -cat /user/debian/fars/anio=2016/fars_2016.csv | head -3

# Conteo de registros
hdfs dfs -cat /user/debian/fars/anio=2015/fars_2015.csv | wc -l
hdfs dfs -cat /user/debian/fars/anio=2016/fars_2016.csv | wc -l

# Limpiar temporales
rm -f /tmp/fars_2015.csv /tmp/fars_2016.csv
```

---

## ⚡ FASE 4: Instalación de Apache Spark

> **Objetivo:** Instalar Spark 3.5.8 para machine learning sobre los datos en HDFS.
> **Resultado:** Spark funcional en `/home/debian/spark`, dependencias Python instaladas.

```bash
# En el master:
cd /tmp

# Descargar Spark 3.5.8 (compatible con Hadoop 3.x)
wget --no-check-certificate \
  https://archive.apache.org/dist/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz

# Extraer
tar xzf spark-3.5.8-bin-hadoop3.tgz
mv spark-3.5.8-bin-hadoop3 /home/debian/spark
rm spark-3.5.8-bin-hadoop3.tgz

# Configurar spark-env.sh
cp /home/debian/spark/conf/spark-env.sh.template /home/debian/spark/conf/spark-env.sh
cat >> /home/debian/spark/conf/spark-env.sh << 'EOF'
export JAVA_HOME=/opt/hadoop/jdk
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=192.168.88.60
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=1g
EOF

# Configurar workers
echo '192.168.88.61' > /home/debian/spark/conf/workers
echo '192.168.88.62' >> /home/debian/spark/conf/workers
echo '192.168.88.63' >> /home/debian/spark/conf/workers

# Configurar spark-defaults.conf
cp /home/debian/spark/conf/spark-defaults.conf.template /home/debian/spark/conf/spark-defaults.conf
cat >> /home/debian/spark/conf/spark-defaults.conf << 'EOF'
spark.master                     spark://192.168.88.60:7077
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://192.168.88.60:9000/spark-libs
spark.history.fs.logDirectory    hdfs://192.168.88.60:9000/spark-libs
EOF

# Instalar dependencias Python para PySpark
pip3 install --break-system-packages numpy pandas setuptools

# Verificar Spark
export JAVA_HOME=/opt/hadoop/jdk
/home/debian/spark/bin/spark-submit --version
```

---

## 🔬 FASE 5: Análisis de Correlación con PySpark

> **Objetivo:** Identificar factores que más incrementan el número de fatalidades.
> **Técnicas:** Correlación Pearson + Random Forest Regressor (50 árboles).
> **Resultado:** R²=0.14, top 5 factores identificados, JSON exportado.

```bash
# Copiar script de análisis al master (desde Kali)
sshpass -p 'debian' scp -o StrictHostKeyChecking=no \
  "/home/kali/big data/analisis_correlacion_v2.py" \
  debian@192.168.88.60:/home/debian/analisis_correlacion_v2.py

# Ejecutar análisis en el master
sshpass -p 'debian' ssh -o StrictHostKeyChecking=no debian@192.168.88.60 \
"export JAVA_HOME=/opt/hadoop/jdk && \
 export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && \
 export PATH=\$PATH:/opt/hadoop/bin && \
 cd /home/debian && \
 /home/debian/spark/bin/spark-submit \
   --master 'local[2]' \
   --driver-memory 1g \
   analisis_correlacion_v2.py"

# Copiar resultados a Kali
sshpass -p 'debian' scp -o StrictHostKeyChecking=no \
  debian@192.168.88.60:/home/debian/resultados_analisis.json \
  "/home/kali/big data/resultados_analisis.json"
```

---

## 📊 FASE 6: Dashboard Streamlit

> **Objetivo:** Crear dashboard ejecutivo interactivo con los hallazgos del análisis.
> **Resultado:** Dashboard con 4 factores clave, gráficos, filtros, métricas.

```bash
# En Kali:
cd "/home/kali/big data"

# Ejecutar dashboard v2 (con resultados Spark)
streamlit run dashboard_fars_v2.py --server.port 8501 --server.headless true

# URLs de acceso:
# Local:    http://localhost:8501
# Red:      http://172.28.21.237:8501
```

---

## 🌐 FASE 7: Exposición Pública (Túnel)

> **Objetivo:** Exponer el dashboard para acceso remoto (cada miembro en su casa).
> **Resultado:** Dashboard accesible vía URL pública temporal.

```bash
# Opción A: localtunnel (gratis, sin registro)
npx localtunnel --port 8501
# URL generada en consola

# Opción B: ngrok (requiere cuenta gratuita)
ngrok http 8501
# URL generada en consola

# Opción C: cloudflared (requiere cuenta Cloudflare)
cloudflared tunnel --url http://localhost:8501
```

---

## 🧹 FASE 8: Comandos Útiles Adicionales

```bash
# ===== HDFS =====
# Listar directorio
hdfs dfs -ls /user/debian/

# Ver uso de espacio
hdfs dfs -df -h

# Borrar archivo/directorio
hdfs dfs -rm -r /user/debian/fars/resultados_analisis.json

# Copiar de HDFS a local
hdfs dfs -get /user/debian/fars/anio=2015/fars_2015.csv /tmp/

# ===== Spark =====
# Iniciar master (modo standalone)
/home/debian/spark/sbin/start-master.sh

# Iniciar worker
/home/debian/spark/sbin/start-worker.sh spark://192.168.88.60:7077

# PySpark shell interactivo
/home/debian/spark/bin/pyspark --master 'local[2]'

# ===== Streamlit =====
# Matar proceso
pkill -f streamlit

# Ver puerto en uso
ss -tlnp | grep 8501

# ===== SSH =====
# Verificar conectividad a workers desde master
ssh debian@192.168.88.61 "hostname"
ssh debian@192.168.88.62 "hostname"
ssh debian@192.168.88.63 "hostname"
```

---

## 📋 Resumen de Errores y Soluciones

| # | Error | Causa | Solución |
|---|-------|-------|----------|
| 1 | `hadoop: command not found` | PATH no configurado | `export PATH=$PATH:/opt/hadoop/bin` |
| 2 | `java: command not found` | JAVA_HOME no definido | `export JAVA_HOME=/opt/hadoop/jdk` |
| 3 | `Permission denied` en `/opt` | Usuario sin permisos | Instalar en `/home/debian/` |
| 4 | `sudo: a terminal is required` | sudo sin contraseña | Evitar sudo, usar home dir |
| 5 | `ModuleNotFoundError: numpy` | Python 3.13 no trae numpy | `pip3 install --break-system-packages numpy` |
| 6 | `ModuleNotFoundError: distutils` | Python 3.13 eliminó distutils | `pip3 install --break-system-packages setuptools` |
| 7 | Chi-cuadrado p-values = 1.0 | Variables continuas en test categórico | Reemplazar por análisis de medias |
| 8 | RF Classifier 100% accuracy | Todos los registros son fatales | Cambiar a RF Regressor |
| 9 | WebHDFS redirect a hostnames | DataNodes usan hostnames no resolubles | Leer vía SSH en vez de HTTP |
| 10 | `ERR_NGROK_3004` | WebSockets + plan gratuito ngrok | Usar localtunnel |
| 11 | `ERR_NGROK_8013` | TCP requiere tarjeta crédito | Usar HTTP tunnel |
| 12 | Gráfico Rural/Urbano vacío | Códigos FARS: 1=Urbano, 2=Rural (no 0,1) | Corregir mapeo `{1:"Urbana", 2:"Rural"}` |
| 13 | Estados mostraban códigos numéricos | Filtro sin nombres | Agregar `STATE_NAMES` mapping |

---

## 🗺️ Estructura Final del Proyecto

```
Kali (WSL): /home/kali/big data/
├── fars-2015-accidents (1).csv          # Datos originales 2015
├── fars-2016-accidents.csv               # Datos originales 2016
├── analisis_correlacion_v2.py            # Script PySpark
├── dashboard_fars_v2.py                  # Dashboard Streamlit
├── dashboard_fars_master.py              # Dashboard para ejecutar en master
├── resultados_analisis.json              # Resultados Spark
├── CASO_DE_ESTUDIO_FARS.md               # Documento caso de estudio
├── informe_ejecutivo_fars.md             # Informe para empresa
├── procedimiento_hadoop_fars.md          # Documentación técnica
└── COMANDOS.md                           # Este archivo

Master (192.168.88.60):
/home/debian/
├── spark/                                # Apache Spark 3.5.8
├── analisis_correlacion_v2.py            # Script de análisis
├── resultados_analisis.json              # Resultados
└── dashboard_fars_master.py              # Dashboard

HDFS: /user/debian/fars/
├── anio=2015/fars_2015.csv               # 32,166 registros
├── anio=2016/fars_2016.csv               # 34,748 registros
├── resultados_analisis.json/             # Resultados v1
└── resultados_analisis_v2.json/          # Resultados v2
```

---

*Todos los comandos fueron ejecutados y verificados el 26 de junio de 2026.*
