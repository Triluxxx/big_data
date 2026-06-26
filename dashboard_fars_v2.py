"""
Dashboard FARS - Factores de Fatalidad (v2 - con resultados Spark)
Basado en análisis de correlación Pearson + Random Forest
"""
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import io
import json
import os

# ============================================================
# CONFIGURACIÓN
# ============================================================
st.set_page_config(
    page_title="FARS - Factores de Fatalidad | Informe Ejecutivo",
    page_icon="🚗",
    layout="wide"
)

HDFS_MASTER = "192.168.88.60"
HDFS_USER = "debian"
SSH_PASS = "debian"
HDFS_BASE = "/user/debian/fars"

# ============================================================
# COLORES CORPORATIVOS
# ============================================================
COLOR_PRIMARY = "#1a3a5c"
COLOR_DANGER = "#c0392b"
COLOR_WARNING = "#e67e22"
COLOR_SUCCESS = "#27ae60"
COLOR_INFO = "#2980b9"
COLOR_MUTED = "#7f8c8d"

# ============================================================
# CACHÉ DE DATOS
# ============================================================

@st.cache_data(ttl=3600, show_spinner="Cargando datos desde HDFS...")
def load_data_from_hdfs():
    """Carga ambos años desde HDFS vía SSH."""
    cmd_2015 = [
        "sshpass", "-p", SSH_PASS, "ssh", "-o", "StrictHostKeyChecking=no",
        f"{HDFS_USER}@{HDFS_MASTER}",
        f"export JAVA_HOME=/opt/hadoop/jdk && export PATH=$PATH:/opt/hadoop/bin && hdfs dfs -cat {HDFS_BASE}/anio=2015/fars_2015.csv"
    ]
    cmd_2016 = [
        "sshpass", "-p", SSH_PASS, "ssh", "-o", "StrictHostKeyChecking=no",
        f"{HDFS_USER}@{HDFS_MASTER}",
        f"export JAVA_HOME=/opt/hadoop/jdk && export PATH=$PATH:/opt/hadoop/bin && hdfs dfs -cat {HDFS_BASE}/anio=2016/fars_2016.csv"
    ]

    try:
        r1 = subprocess.run(cmd_2015, capture_output=True, text=True, timeout=60)
        r2 = subprocess.run(cmd_2016, capture_output=True, text=True, timeout=60)
        df1 = pd.read_csv(io.StringIO(r1.stdout))
        df2 = pd.read_csv(io.StringIO(r2.stdout))
        return pd.concat([df1, df2], ignore_index=True)
    except Exception as e:
        st.error(f"Error cargando HDFS: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner="Cargando resultados del análisis Spark...")
def load_spark_results():
    """Carga los resultados JSON del análisis Spark desde el master."""
    cmd = [
        "sshpass", "-p", SSH_PASS, "ssh", "-o", "StrictHostKeyChecking=no",
        f"{HDFS_USER}@{HDFS_MASTER}",
        "cat /home/debian/resultados_analisis.json"
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return json.loads(r.stdout)
    except:
        return None

# ============================================================
# GRÁFICOS PARA EL DASHBOARD
# ============================================================

def plot_drunk_drivers_impact(spark_results):
    """Gráfico principal: Impacto de conductores ebrios en fatalidades."""
    drunk_data = None
    if spark_results and "detalle_por_grupo" in spark_results:
        drunk_data = spark_results["detalle_por_grupo"].get("drunk_drivers", [])

    if not drunk_data:
        # Fallback con datos crudos
        return None

    valores = [d["valor"] for d in drunk_data]
    medias = [d["media_fatalidades"] for d in drunk_data]
    totales = [d["n"] for d in drunk_data]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Gráfico 1: Media de fatalidades
    colors = [COLOR_SUCCESS, COLOR_WARNING, COLOR_DANGER, "#8e0000"]
    bars = ax1.bar([f"{v} ebrio(s)" for v in valores], medias,
                   color=colors[:len(valores)], edgecolor="white", linewidth=1.5)
    ax1.set_ylabel("Media de Fatalidades por Accidente", fontweight="bold")
    ax1.set_title("Impacto del Alcohol en la Letalidad", fontweight="bold", fontsize=13)
    ax1.set_ylim(0, max(medias) * 1.2)

    # Añadir etiquetas y porcentaje de incremento
    base = medias[0] if medias else 1
    for bar, val in zip(bars, medias):
        pct = ((val - base) / base) * 100
        label = f"{val:.3f}"
        if pct > 0:
            label += f"\n(+{pct:.0f}%)"
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
                label, ha="center", fontweight="bold", fontsize=10)

    # Gráfico 2: Distribución de accidentes
    ax2.pie(totales, labels=[f"{v} ebrio(s)\n({t:,} acc.)" for v, t in zip(valores, totales)],
            colors=colors[:len(valores)], autopct="%1.1f%%",
            startangle=90, explode=[0, 0, 0.1, 0.2])
    ax2.set_title("Distribución de Accidentes", fontweight="bold", fontsize=13)

    plt.tight_layout()
    return fig


def plot_light_condition_impact(spark_results):
    """Condición de luz vs fatalidades."""
    light_data = None
    if spark_results and "detalle_por_grupo" in spark_results:
        light_data = spark_results["detalle_por_grupo"].get("light_condition", [])

    if not light_data:
        return None

    light_labels = {1: "Luz de día", 2: "Oscuro\nsin luz", 3: "Oscuro\ncon luz",
                    4: "Amanecer", 5: "Atardecer", 6: "Oscuro\nluz desc.", 7: "Oscuro"}

    data = sorted(light_data, key=lambda x: x["media_fatalidades"], reverse=True)
    labels = [light_labels.get(d["valor"], f"Cód {d['valor']}") for d in data]
    medias = [d["media_fatalidades"] for d in data]
    counts = [d["n"] for d in data]

    fig, ax = plt.subplots(figsize=(10, 5))

    # Barras horizontales
    colors = plt.cm.RdYlGn_r([(m - min(medias)) / (max(medias) - min(medias)) * 0.6 + 0.2 for m in medias])
    bars = ax.barh(range(len(labels)), medias, color=colors, edgecolor="white")

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Media de Fatalidades por Accidente", fontweight="bold")
    ax.set_title("Tasa de Fatalidad por Condición de Luz", fontweight="bold", fontsize=13)
    ax.invert_yaxis()

    for bar, val, cnt in zip(bars, medias, counts):
        ax.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height()/2,
                f"{val:.3f}  ({cnt:,} acc.)", va="center", fontsize=9)

    plt.tight_layout()
    return fig


def plot_rural_urban_comparison(spark_results):
    """Comparación Rural vs Urbano."""
    ru_data = None
    if spark_results and "detalle_por_grupo" in spark_results:
        ru_data = spark_results["detalle_por_grupo"].get("rural_urban", [])

    if not ru_data:
        return None

    ru_labels = {1: "Urbana", 2: "Rural"}
    data = [d for d in ru_data if d["valor"] in [1, 2]]

    if len(data) < 2:
        return None

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))

    labels = [ru_labels.get(d["valor"], "?") for d in data]
    medias = [d["media_fatalidades"] for d in data]
    counts = [d["n"] for d in data]
    totals = [d["total_fatalidades"] for d in data]

    # Barras comparativas
    colors = [COLOR_INFO, COLOR_WARNING]
    ax1.bar(labels, medias, color=colors, edgecolor="white", linewidth=2)
    ax1.set_ylabel("Media de Fatalidades", fontweight="bold")
    ax1.set_title("Tasa de Fatalidad: Rural vs Urbano", fontweight="bold")

    for i, (m, c) in enumerate(zip(medias, counts)):
        ax1.text(i, m + 0.002, f"{m:.3f}\n({c:,} acc.)", ha="center", fontweight="bold")

    # Torta de distribución
    ax2.pie(counts, labels=[f"{l}\n({c:,} acc.)" for l, c in zip(labels, counts)],
            colors=colors, autopct="%1.1f%%", startangle=90, explode=[0, 0.05])
    ax2.set_title("Distribución de Accidentes", fontweight="bold")

    plt.tight_layout()
    return fig


def plot_hourly_pattern(spark_results):
    """Patrón horario de accidentes fatales."""
    hour_data = None
    if spark_results and "detalle_por_grupo" in spark_results:
        hour_data = spark_results["detalle_por_grupo"].get("hour", [])

    if not hour_data:
        return None

    hours = [int(d["valor"]) for d in hour_data]
    counts = [d["n"] for d in hour_data]
    medias = [d["media_fatalidades"] for d in hour_data]

    # Ordenar por hora
    sorted_data = sorted(zip(hours, counts, medias), key=lambda x: x[0])
    hours, counts, medias = zip(*sorted_data)

    fig, ax1 = plt.subplots(figsize=(12, 5))

    # Barras: cantidad de accidentes
    ax1.bar(hours, counts, color=COLOR_INFO, alpha=0.6, label="N° Accidentes")
    ax1.set_xlabel("Hora del Día", fontweight="bold")
    ax1.set_ylabel("N° Accidentes Fatales", fontweight="bold", color=COLOR_INFO)
    ax1.tick_params(axis='y', labelcolor=COLOR_INFO)

    # Línea: media de fatalidades
    ax2 = ax1.twinx()
    ax2.plot(hours, medias, color=COLOR_DANGER, linewidth=2.5, marker='o', markersize=6, label="Media Fatalidades")
    ax2.set_ylabel("Media de Fatalidades por Accidente", fontweight="bold", color=COLOR_DANGER)
    ax2.tick_params(axis='y', labelcolor=COLOR_DANGER)

    # Sombrear hora pico 17-21
    ax1.axvspan(17, 21, alpha=0.15, color=COLOR_WARNING)
    ax1.text(19, max(counts) * 0.95, "HORA PICO\n17:00-21:00", ha="center",
             fontweight="bold", color=COLOR_WARNING, fontsize=10,
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))

    ax1.set_title("Patrón Horario de Accidentes Fatales", fontweight="bold", fontsize=13)
    ax1.set_xticks(range(0, 24, 2))
    ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 2)])

    # Leyenda combinada
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.tight_layout()
    return fig


def plot_feature_importance(spark_results):
    """Importancia de features del Random Forest."""
    if not spark_results or "importancia_random_forest" not in spark_results:
        return None

    rf_data = spark_results["importancia_random_forest"][:10]
    names = [d["variable"] for d in rf_data]
    imps = [d["importancia"] for d in rf_data]

    # Nombres legibles
    name_map = {
        "total_persons": "Personas Involucradas",
        "hour": "Hora del Día",
        "month": "Mes",
        "manner_of_collision": "Tipo de Colisión",
        "pedestrians": "Peatones",
        "total_vehicles": "Vehículos",
        "day_of_week": "Día de la Semana",
        "rural_urban": "Zona Rural/Urbana",
        "light_condition": "Condición de Luz",
        "drunk_drivers": "Conductores Ebrios",
    }
    labels = [name_map.get(n, n) for n in names]

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = plt.cm.Blues([(i + 0.3) for i in np.linspace(0.3, 0.9, len(labels))])
    bars = ax.barh(range(len(labels)), imps, color=colors, edgecolor="white")

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Importancia (Random Forest)", fontweight="bold")
    ax.set_title("¿Qué Factores Predicen Más Fatalidades?\n(Modelo Random Forest)", fontweight="bold", fontsize=13)
    ax.invert_yaxis()

    for bar, imp in zip(bars, imps):
        ax.text(bar.get_width() + 0.005, bar.get_y() + bar.get_height()/2,
                f"{imp*100:.1f}%", va="center", fontweight="bold", fontsize=10)

    plt.tight_layout()
    return fig


def plot_pearson_correlation(spark_results):
    """Correlaciones de Pearson."""
    if not spark_results or "correlaciones_pearson" not in spark_results:
        return None

    pearson = spark_results["correlaciones_pearson"]
    names = [d["label"] for d in pearson]
    values = [d["abs_r"] for d in pearson]
    raw_values = [d["pearson_r"] for d in pearson]

    fig, ax = plt.subplots(figsize=(10, 5))

    colors = []
    for v in raw_values:
        if abs(v) >= 0.2: colors.append(COLOR_DANGER)
        elif abs(v) >= 0.1: colors.append(COLOR_WARNING)
        elif abs(v) >= 0.05: colors.append(COLOR_INFO)
        else: colors.append(COLOR_MUTED)

    bars = ax.barh(range(len(names)), values, color=colors, edgecolor="white")

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names)
    ax.set_xlabel("Correlación Absoluta (|Pearson r|)", fontweight="bold")
    ax.set_title("Correlación Lineal con Número de Fatalidades", fontweight="bold", fontsize=13)
    ax.invert_yaxis()

    # Leyenda de intensidad
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLOR_DANGER, label="Fuerte (|r| ≥ 0.2)"),
        Patch(facecolor=COLOR_WARNING, label="Moderada (|r| ≥ 0.1)"),
        Patch(facecolor=COLOR_INFO, label="Débil (|r| ≥ 0.05)"),
        Patch(facecolor=COLOR_MUTED, label="Mínima (|r| < 0.05)"),
    ]
    ax.legend(handles=legend_elements, loc="lower right")

    for bar, val, raw in zip(bars, values, raw_values):
        ax.text(bar.get_width() + 0.002, bar.get_y() + bar.get_height()/2,
                f"{raw:+.4f}", va="center", fontsize=9)

    plt.tight_layout()
    return fig


# ============================================================
# INTERFAZ PRINCIPAL
# ============================================================

# Sidebar
st.sidebar.image("https://img.icons8.com/fluency/96/car-crash.png", width=60)
st.sidebar.title("FARS Dashboard")
st.sidebar.markdown("---")
st.sidebar.markdown("### 📋 Filtros")

# Cargar datos
df = load_data_from_hdfs()
spark_results = load_spark_results()

if df is None:
    st.error("❌ No se pudieron cargar los datos desde HDFS.")
    st.stop()

# Filtros
selected_year = st.sidebar.selectbox("Año", ["Todos", 2015, 2016])
STATE_NAMES = {
    1: "Alabama", 2: "Alaska", 4: "Arizona", 5: "Arkansas", 6: "California",
    8: "Colorado", 9: "Connecticut", 10: "Delaware", 11: "DC", 12: "Florida",
    13: "Georgia", 15: "Hawaii", 16: "Idaho", 17: "Illinois", 18: "Indiana",
    19: "Iowa", 20: "Kansas", 21: "Kentucky", 22: "Louisiana", 23: "Maine",
    24: "Maryland", 25: "Massachusetts", 26: "Michigan", 27: "Minnesota",
    28: "Mississippi", 29: "Missouri", 30: "Montana", 31: "Nebraska",
    32: "Nevada", 33: "New Hampshire", 34: "New Jersey", 35: "New Mexico",
    36: "New York", 37: "North Carolina", 38: "North Dakota", 39: "Ohio",
    40: "Oklahoma", 41: "Oregon", 42: "Pennsylvania", 44: "Rhode Island",
    45: "South Carolina", 46: "South Dakota", 47: "Tennessee", 48: "Texas",
    49: "Utah", 50: "Vermont", 51: "Virginia", 53: "Washington",
    54: "West Virginia", 55: "Wisconsin", 56: "Wyoming"
}

state_codes = sorted(df["state_code"].dropna().unique())
state_options = ["Todos"] + [f"{STATE_NAMES.get(int(s), f'St-{int(s)}')} ({int(s)})" for s in state_codes]
selected_state_label = st.sidebar.selectbox("Estado", state_options)

filtered_df = df.copy()
if selected_year != "Todos":
    filtered_df = filtered_df[filtered_df["year"] == selected_year]
if selected_state_label != "Todos":
    # Extraer código de estado del label "Nombre (XX)"
    selected_code = int(selected_state_label.split("(")[1].rstrip(")"))
    filtered_df = filtered_df[filtered_df["state_code"] == selected_code]

# ============================================================
# HEADER
# ============================================================
st.title("🚗 Factores de Fatalidad en Accidentes de Tráfico")
st.markdown("### Análisis basado en FARS 2015-2016 · Procesado con Apache Spark + Hadoop")
st.markdown("---")

# ============================================================
# MÉTRICAS PRINCIPALES
# ============================================================
st.subheader("📊 Métricas del Análisis")

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Accidentes", f"{len(filtered_df):,}")
with col2:
    st.metric("Total Fatalidades", f"{int(filtered_df['fatalities'].sum()):,}")
with col3:
    st.metric("Media Fatalidades/Acc", f"{filtered_df['fatalities'].mean():.3f}")
with col4:
    st.metric("Acc. con Ebrios", f"{int((filtered_df['drunk_drivers'] > 0).sum()):,}",
              f"{(filtered_df['drunk_drivers'] > 0).mean()*100:.1f}%")
with col5:
    r2_val = spark_results["modelo"]["r2"] if spark_results else 0
    st.metric("R² del Modelo", f"{r2_val:.3f}",
              "Random Forest")

st.markdown("---")

# ============================================================
# SECCIÓN 1: EL FACTOR MÁS ACCIONABLE - ALCOHOL
# ============================================================
st.subheader("🍺 Factor Clave #1: Conductores Ebrios")

if spark_results:
    col_left, col_right = st.columns([3, 2])
    with col_left:
        fig = plot_drunk_drivers_impact(spark_results)
        if fig:
            st.pyplot(fig)
    with col_right:
        st.markdown("""
        ### 💡 Hallazgo Principal
        
        El alcohol tiene un **efecto multiplicador** sobre la letalidad:
        
        - **0 ebrios:** 1.080 fatalidades/accidente
        - **1 ebrio:** 1.108 (+2.6%)
        - **2 ebrios:** 1.310 (+21.3%)
        - **3 ebrios:** 1.833 (+69.7%)
        
        > Un accidente con 3 conductores ebrios es **70% más letal** que uno sin alcohol.
        
        **Recomendación:** Invertir en sistemas de detección de alcohol (alcolock) para flotas corporativas.
        """)
else:
    st.info("Cargando resultados del análisis Spark...")

st.markdown("---")

# ============================================================
# SECCIÓN 2: CONDICIÓN DE LUZ
# ============================================================
st.subheader("💡 Factor Clave #2: Condición de Luz")

col_left, col_right = st.columns([3, 2])
with col_left:
    if spark_results:
        fig = plot_light_condition_impact(spark_results)
        if fig:
            st.pyplot(fig)
with col_right:
    st.markdown("""
    ### 💡 Hallazgo
    
    La **oscuridad sin iluminación** presenta la mayor tasa de fatalidad (1.100), por encima de la luz de día (1.091).
    
    - **18,664 accidentes** (27.9%) ocurrieron en oscuridad sin luz
    - La diferencia parece pequeña pero representa cientos de vidas al año
    
    **Recomendación:** Auditoría de iluminación en tramos de alta siniestralidad nocturna.
    """)

st.markdown("---")

# ============================================================
# SECCIÓN 3: RURAL VS URBANO
# ============================================================
st.subheader("🏙️ Factor Clave #3: Zona Rural vs Urbana")

col_left, col_right = st.columns([3, 2])
with col_left:
    if spark_results:
        fig = plot_rural_urban_comparison(spark_results)
        if fig:
            st.pyplot(fig)
with col_right:
    st.markdown("""
    ### 🏙️ Hallazgo
    
    Contrario a la intuición, los accidentes **urbanos** tienen mayor tasa de fatalidad:
    
    - **Urbana:** 1.116 fatalidades/accidente
    - **Rural:** 1.066 fatalidades/accidente
    - Diferencia: **+4.7%** en zonas urbanas
    
    **Posible explicación:** Mayor densidad de ocupantes por vehículo en ciudad.
    
    **Recomendación:** Reforzar seguridad vial en intersecciones urbanas de alto tráfico.
    """)

st.markdown("---")

# ============================================================
# SECCIÓN 4: PATRÓN HORARIO
# ============================================================
st.subheader("🕐 Factor Clave #4: Hora del Día")

if spark_results:
    fig = plot_hourly_pattern(spark_results)
    if fig:
        st.pyplot(fig)

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("""
    ### ⏰ Hora Pico
    **17:00 - 21:00** concentra la mayor cantidad de accidentes fatales.
    
    Coincide con:
    - Fin de jornada laboral
    - Mayor volumen de tráfico
    - Transición a oscuridad
    """)
with col2:
    st.markdown("""
    ### 🌅 Amanecer (04:00-06:00)
    Segundo pico de fatalidades.
    
    Factores:
    - Fatiga del conductor
    - Baja visibilidad
    - Fin de fiestas/ocio nocturno
    """)
with col3:
    st.markdown("""
    ### 📊 Recomendación
    **Operativos de control:**
    - 17:00-21:00: alcohol y velocidad
    - 04:00-06:00: fatiga y alcohol
    
    **Campañas de comunicación** en horarios de mayor riesgo.
    """)

st.markdown("---")

# ============================================================
# SECCIÓN 5: IMPORTANCIA DE FEATURES (RANDOM FOREST)
# ============================================================
st.subheader("🧠 ¿Qué Factores Predicen Más Fatalidades? (Modelo Machine Learning)")

col_left, col_right = st.columns([1, 1])
with col_left:
    if spark_results:
        fig = plot_feature_importance(spark_results)
        if fig:
            st.pyplot(fig)
with col_right:
    if spark_results:
        fig = plot_pearson_correlation(spark_results)
        if fig:
            st.pyplot(fig)

st.markdown("---")

# ============================================================
# SECCIÓN 6: DATOS CRUDOS Y METODOLOGÍA
# ============================================================
with st.expander("📋 Metodología y Datos del Análisis"):
    st.markdown(f"""
    ### Infraestructura
    - **Clúster Hadoop 3.3.6:** 1 master + 3 workers (5 CPUs, 8 GB RAM)
    - **Apache Spark 3.5.8:** Random Forest Regressor (50 árboles, maxDepth=10)
    - **HDFS:** Datos particionados por año (`anio=2015/`, `anio=2016/`)
    
    ### Técnicas Aplicadas
    1. **Correlación de Pearson:** Relación lineal entre variables y número de fatalidades
    2. **Random Forest Regressor:** Modelo predictivo con medición de importancia de features
    3. **Análisis por grupos:** Comparación de medias entre categorías
    
    ### Rendimiento del Modelo
    - **RMSE:** {spark_results['modelo']['rmse']:.4f} (error promedio en predicción)
    - **R²:** {spark_results['modelo']['r2']:.4f} (varianza explicada: {spark_results['modelo']['r2']*100:.1f}%)
    
    ### Limitaciones
    - FARS solo contiene accidentes con al menos 1 fatalidad
    - El modelo explica el 14.2% de la varianza; hay factores no capturados (velocidad, cinturón, edad)
    - Datos limitados a EE.UU. 2015-2016
    """)

with st.expander("📋 Ver Datos Crudos (muestra 500 filas)"):
    st.dataframe(filtered_df.head(500), use_container_width=True)

# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.markdown(
    f"""
    <div style="text-align: center; color: #7f8c8d; font-size: 0.85em;">
    <strong>FARS Dashboard v2.0</strong> — Análisis de Factores de Fatalidad<br>
    Datos: NHTSA FARS 2015-2016 · Procesado con Apache Spark 3.5.8 + Hadoop 3.3.6<br>
    HDFS: <code>{HDFS_BASE}/anio=2015/</code> · <code>{HDFS_BASE}/anio=2016/</code><br>
    Clúster: master (192.168.88.60) + worker1, worker2, worker3
    </div>
    """,
    unsafe_allow_html=True
)
