"""
Análisis de Correlación FARS - Factores que aumentan el NÚMERO de fatalidades
Ejecutar: spark-submit analisis_correlacion.py
"""
import sys, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, round as spark_round, sum as spark_sum
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("FARS-Correlation-Analysis-v2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("  ANÁLISIS FARS - FACTORES QUE AUMENTAN NÚMERO DE FATALIDADES")
print("=" * 65)

# ============================================================
# 1. CARGAR DATOS
# ============================================================
print("\n[1/5] Cargando datos desde HDFS...")
df_2015 = spark.read.option("header","true").option("inferSchema","true") \
    .csv("hdfs://192.168.88.60:9000/user/debian/fars/anio=2015/fars_2015.csv")
df_2016 = spark.read.option("header","true").option("inferSchema","true") \
    .csv("hdfs://192.168.88.60:9000/user/debian/fars/anio=2016/fars_2016.csv")
df = df_2015.union(df_2016)
print(f"   Registros: {df.count():,} | Columnas: {len(df.columns)}")

# ============================================================
# 2. LIMPIEZA
# ============================================================
print("\n[2/5] Preparando datos...")
numeric_cols = [
    "year","state_code","total_vehicles","total_persons",
    "pedestrians","persons_in_motor_vehicles","persons_not_in_motor_vehicles",
    "month","day_of_week","hour","minute",
    "national_highway_system","rural_urban","functional_system",
    "light_condition","weather_condition_1","weather_condition_2",
    "manner_of_collision","type_of_intersection","work_zone",
    "school_bus","rail_grade_crossing",
    "fatalities","drunk_drivers"
]
for c in numeric_cols:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast(DoubleType()))

df_clean = df.dropna(subset=["fatalities","drunk_drivers","total_vehicles",
                               "light_condition","rural_urban","hour","day_of_week"])
print(f"   Registros limpios: {df_clean.count():,}")

# ============================================================
# 3. CORRELACIÓN PEARSON + ANÁLISIS POR GRUPOS
# ============================================================
print("\n[3/5] Análisis de factores vs número de fatalidades...")

analysis_vars = [
    ("drunk_drivers", "Conductores Ebrios"),
    ("total_vehicles", "Vehículos Involucrados"),
    ("total_persons", "Personas Involucradas"),
    ("pedestrians", "Peatones"),
    ("light_condition", "Condición de Luz"),
    ("rural_urban", "Zona Rural/Urbana"),
    ("hour", "Hora del Día"),
    ("day_of_week", "Día de la Semana"),
    ("month", "Mes"),
    ("national_highway_system", "Sistema Nacional de Autopistas"),
    ("work_zone", "Zona de Trabajo"),
    ("school_bus", "Autobús Escolar"),
    ("manner_of_collision", "Tipo de Colisión"),
    ("type_of_intersection", "Tipo de Intersección"),
]

results = []
for var, label in analysis_vars:
    if var not in df_clean.columns:
        continue
    
    # Correlación Pearson
    pearson_r = df_clean.stat.corr(var, "fatalities")
    
    # Estadísticas por grupo (para variables categóricas con pocos valores)
    n_unique = df_clean.select(var).distinct().count()
    
    group_stats = None
    if n_unique <= 20:
        stats = df_clean.groupBy(var).agg(
            count("*").alias("n"),
            spark_round(mean("fatalities"), 4).alias("media_fatalidades"),
            spark_round(stddev("fatalities"), 2).alias("std_fatalidades"),
            spark_sum("fatalities").alias("total_fatalidades")
        ).orderBy("media_fatalidades", ascending=False).collect()
        group_stats = [{
            "valor": int(r[var]) if r[var] is not None else None,
            "n": r["n"],
            "media_fatalidades": float(r["media_fatalidades"]),
            "std_fatalidades": float(r["std_fatalidades"]) if r["std_fatalidades"] else 0,
            "total_fatalidades": int(r["total_fatalidades"])
        } for r in stats]
    
    results.append({
        "variable": var,
        "label": label,
        "pearson_r": round(pearson_r, 4) if pearson_r else 0,
        "abs_pearson_r": round(abs(pearson_r), 4) if pearson_r else 0,
        "n_unique": n_unique,
        "group_stats": group_stats
    })

# Ordenar por valor absoluto de correlación
results.sort(key=lambda x: x["abs_pearson_r"], reverse=True)

print(f"\n   {'Variable':<30} {'Pearson r':>10} {'|r|':>10} {'Interpretación'}")
print(f"   {'-'*70}")
for r in results:
    strength = ""
    ar = r["abs_pearson_r"]
    if ar >= 0.3: strength = "🔴 FUERTE"
    elif ar >= 0.1: strength = "🟡 MODERADA"
    elif ar >= 0.05: strength = "🟢 DÉBIL"
    else: strength = "⚪ MÍNIMA"
    print(f"   {r['label']:<30} {r['pearson_r']:>10.4f} {ar:>10.4f} {strength}")

# ============================================================
# 4. RANDOM FOREST REGRESSION
# ============================================================
print("\n[4/5] Random Forest Regressor - Importancia de Features...")

feature_cols = [r["variable"] for r in results if r["variable"] != "fatalities"]
ml_data = df_clean.select(feature_cols + ["fatalities"]).dropna()

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
ml_data = assembler.transform(ml_data)

train, test = ml_data.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestRegressor(
    featuresCol="features", labelCol="fatalities",
    numTrees=50, maxDepth=10, seed=42
)
model = rf.fit(train)
predictions = model.transform(test)

# Evaluar
evaluator_rmse = RegressionEvaluator(labelCol="fatalities", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="fatalities", metricName="r2")
rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print(f"   RMSE: {rmse:.4f} | R²: {r2:.4f}")

# Importancia
importances = model.featureImportances.toArray()
rf_importance = list(zip(feature_cols, importances))
rf_importance.sort(key=lambda x: x[1], reverse=True)

print(f"\n   Importancia Random Forest (prediciendo número de fatalidades):")
print(f"   {'Variable':<30} {'Importancia':>12} {'%':>8}")
print(f"   {'-'*50}")
for name, imp in rf_importance[:10]:
    bar = "█" * int(imp * 100)
    print(f"   {name:<30} {imp:>12.6f} {imp*100:>7.1f}% {bar}")

# ============================================================
# 5. HALLAZGOS CLAVE (análisis detallado por grupo)
# ============================================================
print("\n[5/5] HALLAZGOS DETALLADOS POR FACTOR")
print("=" * 65)

# --- DRUNK DRIVERS ---
print("\n📊 CONDUCTORES EBRIOS (drunk_drivers):")
for r in results:
    if r["variable"] == "drunk_drivers" and r["group_stats"]:
        for g in r["group_stats"]:
            print(f"   {g['valor']} ebrio(s): {g['n']:,} accidentes | "
                  f"media fatalidades: {g['media_fatalidades']:.3f} | "
                  f"total: {g['total_fatalidades']:,}")

# --- LIGHT CONDITION ---
print("\n📊 CONDICIÓN DE LUZ (light_condition):")
light_labels = {1:"Luz de día", 2:"Oscuro - sin luz", 3:"Oscuro - con luz",
                4:"Amanecer", 5:"Atardecer", 6:"Oscuro - luz desc.", 7:"Oscuro"}
for r in results:
    if r["variable"] == "light_condition" and r["group_stats"]:
        for g in sorted(r["group_stats"], key=lambda x: x["media_fatalidades"], reverse=True)[:7]:
            label = light_labels.get(g["valor"], f"Cód {g['valor']}")
            print(f"   {label:<20} | {g['n']:>6,} acc | media: {g['media_fatalidades']:.3f} | total: {g['total_fatalidades']:,}")

# --- RURAL vs URBAN ---
print("\n📊 ZONA RURAL vs URBANA (rural_urban):")
ru_labels = {0:"Rural", 1:"Urbana"}
for r in results:
    if r["variable"] == "rural_urban" and r["group_stats"]:
        for g in r["group_stats"]:
            label = ru_labels.get(g["valor"], f"Cód {g['valor']}")
            print(f"   {label:<10} | {g['n']:>6,} acc | media: {g['media_fatalidades']:.3f} | total: {g['total_fatalidades']:,}")

# --- HOUR ---
print("\n📊 HORA DEL DÍA - Top 5 horas con mayor tasa de fatalidad:")
for r in results:
    if r["variable"] == "hour" and r["group_stats"]:
        sorted_hours = sorted(r["group_stats"], key=lambda x: x["media_fatalidades"], reverse=True)[:5]
        for g in sorted_hours:
            print(f"   {int(g['valor']):02d}:00 | {g['n']:>6,} acc | media: {g['media_fatalidades']:.3f} | total: {g['total_fatalidades']:,}")

# --- TOTAL VEHICLES ---
print("\n📊 VEHÍCULOS INVOLUCRADOS (total_vehicles):")
for r in results:
    if r["variable"] == "total_vehicles" and r["group_stats"]:
        for g in r["group_stats"][:8]:
            print(f"   {g['valor']} vehículo(s): {g['n']:>6,} acc | media: {g['media_fatalidades']:.3f} | total: {g['total_fatalidades']:,}")

# --- TOTAL PERSONS ---
print("\n📊 PERSONAS INVOLUCRADAS (total_persons):")
for r in results:
    if r["variable"] == "total_persons" and r["group_stats"]:
        for g in r["group_stats"][:8]:
            print(f"   {g['valor']} persona(s): {g['n']:>6,} acc | media: {g['media_fatalidades']:.3f} | total: {g['total_fatalidades']:,}")

# ============================================================
# 6. GUARDAR RESULTADOS
# ============================================================
print("\n" + "=" * 65)
print("GUARDANDO RESULTADOS...")

# Estadísticas globales
total_acc = df_clean.count()
total_fat = df_clean.agg(spark_sum("fatalities")).collect()[0][0]
avg_fat = df_clean.agg(mean("fatalities")).collect()[0][0]

output = {
    "resumen": {
        "total_accidentes": total_acc,
        "total_fatalidades": int(total_fat),
        "media_fatalidades": round(float(avg_fat), 4),
        "nota": "FARS solo contiene accidentes con al menos 1 fatalidad. El análisis mide qué factores aumentan el NÚMERO de fatalidades por accidente."
    },
    "modelo": {
        "tipo": "RandomForestRegressor",
        "rmse": round(float(rmse), 4),
        "r2": round(float(r2), 4),
        "interpretacion": f"El modelo explica {round(float(r2)*100,1)}% de la varianza en número de fatalidades"
    },
    "correlaciones_pearson": [
        {"variable": r["variable"], "label": r["label"],
         "pearson_r": r["pearson_r"], "abs_r": r["abs_pearson_r"]}
        for r in results
    ],
    "importancia_random_forest": [
        {"variable": name, "importancia": round(float(imp), 6)}
        for name, imp in rf_importance
    ],
    "top_factores": [
        {"rank": i+1, "variable": r["variable"], "label": r["label"],
         "pearson_r": r["pearson_r"],
         "rf_importance": next((round(float(imp),6) for n, imp in rf_importance if n==r["variable"]), 0)}
        for i, r in enumerate(results[:8])
    ],
    "detalle_por_grupo": {
        r["variable"]: r["group_stats"]
        for r in results if r["group_stats"]
    }
}

json_str = json.dumps(output, indent=2, ensure_ascii=False)
spark.sparkContext.parallelize([json_str]).saveAsTextFile(
    "hdfs://192.168.88.60:9000/user/debian/fars/resultados_analisis_v2.json"
)

# También guardar localmente para el dashboard
with open("/home/debian/resultados_analisis.json", "w") as f:
    f.write(json_str)

print("✅ Resultados guardados en:")
print("   HDFS: /user/debian/fars/resultados_analisis_v2.json")
print("   Local: /home/debian/resultados_analisis.json")
print("=" * 65)

spark.stop()
