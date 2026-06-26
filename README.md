# 🚗 Big Data FARS — Pipeline Completo Hadoop + Spark + Streamlit

> **Repositorio:** [github.com/Triluxxx/big_data](https://github.com/Triluxxx/big_data)

---

## 📂 Estructura del Repositorio

| Archivo | Contenido | ¿Para quién? |
|---------|-----------|---------------|
| **[CASO_DE_ESTUDIO_FARS.md](CASO_DE_ESTUDIO_FARS.md)** | 📄 Documento formal del caso: pipeline, arquitectura, resultados, recomendaciones | Profesor, empresa, informe |
| **[COMANDOS.md](COMANDOS.md)** | ⚡ Todos los comandos en orden del flujo de trabajo + 13 errores documentados | Replicación, referencia técnica |
| **[procedimiento_hadoop_fars.md](procedimiento_hadoop_fars.md)** | 🐘 Documentación técnica exhaustiva del clúster Hadoop, Spark, HDFS | Documentación del proyecto |

### 📂 Archivos de Código

| Archivo | Descripción |
|---------|-------------|
| `dashboard_fars_v4.py` | Dashboard Streamlit + Plotly (gráficos interactivos) |
| `resultados_analisis.json` | Resultados del modelo en formato JSON |

### 📂 Scripts Modulares (`scripts/`)

| Script | Fase | Descripción |
|--------|------|-------------|
| `01_ingesta.py` | 1 | Diagnóstico de conectividad y estado del clúster |
| `02_etl.py` | 2 | Transferencia de datos y carga a HDFS con particionado |
| `03_spark_ml.py` | 3 | Análisis ML: Pearson + Random Forest Regressor |
| `04_dashboard.py` | 4 | Iniciar dashboard Streamlit + Plotly |
| `pipeline_completo.py` | Todas | Ejecuta fases 1-3 (o 1-4 con --all) |

### 📂 Datos

| Archivo | Registros | Tamaño |
|---------|-----------|--------|
| `fars-2015-accidents (1).csv` | 32,166 | 4.66 MB |
| `fars-2016-accidents.csv` | 34,748 | 5.60 MB |

---

## 🏗️ Arquitectura del Proyecto

```mermaid
graph TB
    subgraph "Clúster Hadoop (4 nodos)"
        MASTER["Master<br/>192.168.88.60<br/>NameNode + Spark"]
        W1["Worker 1<br/>192.168.88.61"]
        W2["Worker 2<br/>192.168.88.62"]
        W3["Worker 3<br/>192.168.88.63"]
    end

    subgraph "Kali Linux (WSL)"
        DATA["📁 Datos FARS CSV"]
        DASH["📊 Streamlit Dashboard"]
    end

    DATA -->|"1. SCP"| MASTER
    MASTER -->|"2. HDFS Put"| W1
    MASTER -->|"2. HDFS Put"| W2
    MASTER -->|"2. HDFS Put"| W3
    MASTER -->|"3. Spark ML"| MASTER
    MASTER -->|"4. JSON"| DASH
    DASH -->|"5. Dashboard"| USERS["👥 Usuarios"]
```

---

## 🚀 Flujo de Trabajo (7 Fases)

| Fase | Descripción | Script |
|------|-------------|--------|
| 1 | Descubrimiento y diagnóstico del clúster | `scripts/01_ingesta.py` |
| 2 | Transferencia de datos Kali → Master → HDFS | `scripts/02_etl.py` |
| 3 | Análisis ML con PySpark (Pearson + Random Forest) | `scripts/03_spark_ml.py` |
| 4 | Dashboard ejecutivo con Streamlit + Plotly | `scripts/04_dashboard.py` |
| 🚀 | **Pipeline completo (todo automático)** | `scripts/pipeline_completo.py` |

---

## 📊 Resultados Clave

> **El alcohol tiene un efecto multiplicador:** un accidente con 3 conductores ebrios es **70% más letal** que uno sin alcohol.

| Factor | Impacto | Recomendación |
|--------|---------|---------------|
| 🍺 Conductores ebrios | 3 ebrios = +70% fatalidades | Alcolock en flotas |
| 💡 Oscuridad sin luz | 27.9% de accidentes | Auditoría de alumbrado |
| 🏙️ Zona urbana | +4.7% tasa vs rural | Intersecciones seguras |
| 🕐 Hora pico 17-21h | Mayor concentración | Operativos focalizados |

---

## 🔧 Stack Tecnológico

| Capa | Tecnología | Versión |
|------|-----------|---------|
| Almacenamiento | Hadoop HDFS | 3.3.6 |
| Procesamiento | Apache Spark | 3.5.8 |
| ML | Spark MLlib (Random Forest) | 3.5.8 |
| Lenguaje | Python (PySpark) | 3.13 |
| Visualización | Streamlit + **Plotly** | 1.58 |
| Gráficos | Plotly (interactivos) | 5.x |
| Datos | Pandas | 3.0 |
| Túnel | Localtunnel | - |

---

## 🔗 Dashboard

```bash
# Iniciar dashboard interactivo (Plotly)
python3 scripts/04_dashboard.py

# O directamente:
streamlit run dashboard_fars_v4.py --server.port 8501

# Exponer públicamente:
npx localtunnel --port 8501
```

---

## 📋 Errores Documentados (13)

Todos los errores encontrados durante el desarrollo están documentados con síntoma, causa y solución en:

- **[COMANDOS.md](COMANDOS.md#-resumen-de-errores-y-soluciones)** — Tabla resumen de los 13 errores
- **[procedimiento_hadoop_fars.md](procedimiento_hadoop_fars.md#6-errores-encontrados-y-soluciones)** — Documentación detallada de cada error

---

## 📝 Licencia

Proyecto académico — Big Data 2026.

---

*Para detalles completos, consultar [CASO_DE_ESTUDIO_FARS.md](CASO_DE_ESTUDIO_FARS.md).*
