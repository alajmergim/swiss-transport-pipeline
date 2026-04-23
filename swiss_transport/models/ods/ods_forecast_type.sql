WITH all_statuses AS (
    SELECT DEPARTURE_FORECAST_STATUS AS raw_status FROM {{ ref('ods_daily_transport') }}
    UNION 
    SELECT ARRIVAL_FORECAST_STATUS FROM {{ ref('ods_daily_transport') }}
)

SELECT
    -- Generate a stable ID based on the text hash so it never changes
    CASE 
        WHEN raw_status IS NULL THEN -1 
        ELSE hash(raw_status) 
    END AS FORECAST_TYPE_ID,
    
    raw_status AS FORECAST_TYPE_DE,

    CASE UPPER(raw_status)
        WHEN 'REAL'        THEN 'RÉEL'
        WHEN 'GESCHAETZT' THEN 'ESTIMÉ'
        WHEN 'PROGNOSE'   THEN 'PRÉVISION'
        WHEN 'UNBEKANNT'  THEN 'INCONNU'
        ELSE 'NON DÉFINI'
    END AS FORECAST_TYPE_FR,

    CASE UPPER(raw_status)
        WHEN 'REAL'       THEN 'REAL'
        WHEN 'GESCHAETZT' THEN 'ESTIMATED'
        WHEN 'PROGNOSE'   THEN 'FORECAST'
        WHEN 'UNBEKANNT'  THEN 'UNKNOWN'
        ELSE 'UNDEFINED'
    END AS FORECAST_TYPE_EN

FROM all_statuses