{{
    config(
        materialized='incremental',
        unique_key='_surrogate_key',
        incremental_strategy='delete+insert'
    )
}}

WITH source AS (
    SELECT
        *,
        filename AS source_file
    FROM read_csv(
        '/Users/consultez/Desktop/swiss-transport-pipeline/swiss_transport/raw_data/*.csv',
        delim = ';',
        header = true,
        filename = true,
        auto_detect = true,
        union_by_name = true,
        normalize_names = true,
        all_varchar = true -- I will handle column type later
    )
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['BETRIEBSTAG', 'source_file']) }} AS _surrogate_key,
    *,
    CURRENT_TIMESTAMP AS _loaded_at
FROM source

{% if is_incremental() %}
    WHERE BETRIEBSTAG >= (SELECT MAX(BETRIEBSTAG) FROM {{ this }})
{% endif %}