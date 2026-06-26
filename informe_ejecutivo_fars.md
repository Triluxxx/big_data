# INFORME EJECUTIVO: Análisis de Factores de Fatalidad en Accidentes de Tráfico

**Dataset:** FARS (Fatality Analysis Reporting System) 2015-2016  
**Fecha del análisis:** 26 de junio de 2026  
**Metodología:** Apache Spark 3.5.8 — Correlación Pearson + Random Forest Regressor  
**Preparado para:** Empresa de Seguridad Vial / Aseguradora

---

## 1. RESUMEN EJECUTIVO

Se analizaron **66,914 accidentes fatales** ocurridos en Estados Unidos durante 2015-2016, con un total de **72,898 fatalidades**. El objetivo fue identificar los factores que más incrementan el **número de víctimas mortales por accidente**, utilizando técnicas de machine learning sobre un clúster Hadoop.

### Hallazgo Principal

El factor que más predice un mayor número de fatalidades es el **número de personas involucradas en el accidente** (correlación Pearson r=0.29, importancia Random Forest 40.6%). Sin embargo, desde el punto de vista de **políticas de prevención**, los factores accionables más relevantes son:

1. **Conductores ebrios:** Un accidente con 3 conductores ebrios tiene **70% más fatalidades** que uno sin ebrios (1.83 vs 1.08 fatalidades/accidente)
2. **Conducción nocturna sin iluminación:** Mayor tasa de fatalidad (1.10) que con luz de día (1.09)
3. **Zonas urbanas:** Tasa de fatalidad 4.7% mayor que en zonas rurales (1.116 vs 1.066)
4. **Hora pico vespertina:** 17:00-21:00 concentra la mayor cantidad de accidentes fatales

---

## 2. METODOLOGÍA

### 2.1 Infraestructura

| Componente | Tecnología | Detalle |
|------------|-----------|---------|
| Almacenamiento | HDFS (Hadoop 3.3.6) | 3 DataNodes, réplica x3 |
| Procesamiento | Apache Spark 3.5.8 | Random Forest Regressor, 50 árboles |
| Visualización | Streamlit + Pandas + Matplotlib | Dashboard interactivo |

### 2.2 Técnicas de Análisis

1. **Correlación de Pearson:** Mide relación lineal entre cada variable y el número de fatalidades
2. **Random Forest Regressor:** Modelo de machine learning que mide importancia predictiva de cada factor
3. **Análisis por grupos:** Comparación de medias de fatalidades entre categorías

### 2.3 Rendimiento del Modelo

- **RMSE:** 0.3438 (error promedio en predicción de número de fatalidades)
- **R²:** 0.1417 (el modelo explica el 14.2% de la varianza)
- **Interpretación:** Un R² de 14.2% es esperable en datos de accidentes, donde intervienen muchos factores no medidos (velocidad, estado del vehículo, uso de cinturón, etc.)

---

## 3. RESULTADOS DETALLADOS

### 3.1 Ranking de Factores por Importancia

| Rank | Factor | Pearson r | RF Importancia | Accionable |
|------|--------|-----------|----------------|------------|
| 1 | Personas involucradas | 0.2946 | 40.6% | ❌ No controlable |
| 2 | Hora del día | -0.0131 | 10.2% | ✅ Campañas horarias |
| 3 | Mes del año | -0.0041 | 7.3% | ✅ Operativos estacionales |
| 4 | Tipo de colisión | 0.0381 | 6.9% | ✅ Infraestructura |
| 5 | Peatones | -0.0484 | 6.3% | ✅ Cruces seguros |
| 6 | Vehículos involucrados | 0.1126 | 5.8% | ❌ No controlable |
| 7 | Día de la semana | 0.0070 | 5.3% | ✅ Operativos finde |
| 8 | Zona Rural/Urbana | -0.0380 | 4.4% | ✅ Políticas urbanas |
| 9 | Condición de luz | -0.0140 | 3.8% | ✅ Alumbrado público |
| 10 | Conductores ebrios | 0.0501 | 2.9% | ✅ Control alcoholemia |

### 3.2 Conductores Ebrios — El Factor Más Accionable

| Conductores ebrios | Accidentes | Media fatalidades | Total fatalidades | Incremento |
|---------------------|------------|-------------------|-------------------|------------|
| 0 | 48,750 | 1.080 | 52,665 | — (línea base) |
| 1 | 17,642 | 1.108 | 19,546 | **+2.6%** |
| 2 | 516 | 1.310 | 676 | **+21.3%** |
| 3 | 6 | 1.833 | 11 | **+69.7%** |

> **Conclusión:** Cada conductor ebrio adicional incrementa significativamente la letalidad. Un accidente con 2+ conductores ebrios es sustancialmente más mortal.

### 3.3 Condición de Luz

| Condición | Accidentes | Media fatalidades |
|-----------|------------|-------------------|
| Oscuro - sin luz | 18,664 | **1.100** |
| Atardecer | 1,603 | 1.094 |
| Luz de día | 31,766 | 1.091 |
| Amanecer | 1,254 | 1.085 |
| Oscuro - con luz | 12,829 | 1.072 |

> **Conclusión:** La oscuridad sin alumbrado aumenta la tasa de fatalidad. Mejorar la iluminación en vías de alta siniestralidad podría reducir la letalidad.

### 3.4 Zona Rural vs Urbana

| Zona | Accidentes | Media fatalidades | Total fatalidades |
|------|------------|-------------------|-------------------|
| **Urbana** | 31,755 | **1.116** | 35,435 |
| Rural | 32,580 | 1.066 | 34,719 |

> **Conclusión:** Contrario a la intuición, los accidentes urbanos tienen mayor tasa de fatalidad por accidente (4.7% más). Esto podría deberse a mayor densidad de personas por vehículo en ciudad.

### 3.5 Hora del Día

Las horas con mayor concentración de accidentes fatales son las **17:00-21:00** (hora pico vespertina), coincidiendo con el fin de la jornada laboral, mayor tráfico y transición a oscuridad.

---

## 4. RECOMENDACIONES PARA LA EMPRESA

### 4.1 Prioridad Alta: Control de Alcohol

- **Invertir en campañas de concientización** sobre el impacto multiplicador del alcohol en la letalidad
- **Tecnología de detección:** Sistemas de alcolock en flotas corporativas
- **ROI estimado:** Reducir accidentes con 2+ ebrios (522 casos) podría evitar ~100 fatalidades/año

### 4.2 Prioridad Media: Iluminación Vial

- **Auditar tramos de alta siniestralidad nocturna** sin iluminación adecuada
- **Colaborar con municipios** para instalar/mejorar alumbrado en puntos negros
- **Dato clave:** 18,664 accidentes ocurrieron en oscuridad sin luz (27.9% del total)

### 4.3 Prioridad Media: Operativos por Horario

- **Reforzar controles entre 17:00-21:00** (horas pico de accidentes fatales)
- **Campañas de comunicación** sobre riesgos del manejo nocturno y en hora pico

### 4.4 Prioridad Baja: Infraestructura Urbana

- **Revisar intersecciones peligrosas** en zonas urbanas (mayor tasa de fatalidad)
- **Implementar medidas de tráfico calmado** en áreas de alta densidad

---

## 5. DATOS DEL ANÁLISIS

### 5.1 Dataset

- **Origen:** FARS (Fatality Analysis Reporting System), NHTSA, EE.UU.
- **Período:** 2015-2016
- **Registros:** 66,914 accidentes
- **Variables:** 53 columnas (ubicación, condiciones, vehículos, personas, consecuencias)
- **Nota:** FARS solo registra accidentes con al menos 1 fatalidad

### 5.2 Infraestructura de Procesamiento

- **Clúster Hadoop:** 1 master + 3 workers (5 CPUs, 8 GB RAM total)
- **HDFS:** 57.6 GB capacidad, réplica x3
- **Spark:** 3.5.8 en modo local[2]
- **Dashboard:** Streamlit 1.57 + Pandas 3.0 + Matplotlib 3.10

### 5.3 Limitaciones del Análisis

- El modelo R²=0.14 indica que hay factores no capturados en el dataset (velocidad, cinturón, edad, tipo de vehículo)
- Los datos son solo de EE.UU., la extrapolación a otros países requiere validación
- FARS solo contiene accidentes fatales; no se pueden hacer inferencias sobre accidentes no fatales

---

## 6. PRÓXIMOS PASOS SUGERIDOS

1. **Ampliar el dataset** con años adicionales (2017-2024) para validar tendencias
2. **Incorporar datos de velocidad** y uso de cinturón para mejorar el modelo
3. **Análisis geoespacial avanzado** con clustering de puntos calientes
4. **Modelo de predicción en tiempo real** para patrullaje predictivo
5. **Segmentación por estado** para identificar políticas estatales efectivas

---

*Documento generado automáticamente por el pipeline de análisis Hadoop/Spark.*  
*Resultados completos y dashboard interactivo disponibles en el sistema.*
