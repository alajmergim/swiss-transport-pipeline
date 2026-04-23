SELECT
    DISTINCT 
    STOP_UIC,
    STOP_NAME
FROM {{ref('ods_daily_transport')}}