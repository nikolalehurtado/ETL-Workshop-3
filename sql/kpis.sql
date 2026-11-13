-- Workshop-3: consultas de los KPIs del dashboard.
-- Todas leen de la base de datos de predicciones (no de archivos CSV).

-- KPI 1. Error promedio de prediccion
--   mae  = error absoluto medio: en promedio, cuantos puntos se equivoca el modelo
--   bias = error medio con signo: positivo si el modelo tiende a quedarse corto
--   rmse = raiz del error cuadratico medio: castiga mas los errores grandes
SELECT
    COUNT(*)                                        AS total_predictions,
    ROUND(AVG(ABS(prediction_error)), 4)            AS mae,
    ROUND(AVG(prediction_error), 4)                 AS bias,
    ROUND(SQRT(AVG(prediction_error * prediction_error)), 4) AS rmse
FROM fact_predictions;

-- KPI 2. Predicciones por pais
SELECT
    c.country_name,
    COUNT(*)                                AS predictions,
    ROUND(AVG(f.actual_score), 3)           AS avg_actual_score,
    ROUND(AVG(f.predicted_score), 3)        AS avg_predicted_score,
    ROUND(AVG(ABS(f.prediction_error)), 3)  AS mae
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
GROUP BY c.country_name
ORDER BY mae DESC;

-- KPI 3. Puntaje predicho vs. real (una fila por prediccion)
SELECT
    r.event_id,
    c.country_name,
    d.year,
    f.actual_score,
    f.predicted_score,
    f.prediction_error
FROM fact_predictions f
JOIN dim_country c          ON c.country_id = f.country_id
JOIN dim_date d             ON d.date_id = f.date_id
JOIN raw_happiness_events r ON r.raw_event_id = f.raw_event_id
ORDER BY f.prediction_id;

-- KPI 4. Tendencia de las predicciones en el tiempo (por anio de la observacion)
SELECT
    d.year,
    COUNT(*)                                AS predictions,
    ROUND(AVG(f.actual_score), 3)           AS avg_actual_score,
    ROUND(AVG(f.predicted_score), 3)        AS avg_predicted_score,
    ROUND(AVG(ABS(f.prediction_error)), 3)  AS mae
FROM fact_predictions f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year
ORDER BY d.year;

-- Extra. Calidad del pipeline: eventos recibidos por estado de procesamiento
SELECT
    processing_status,
    COUNT(*) AS events
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY processing_status;