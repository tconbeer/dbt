{{
    config(
        materialized='incremental',
        on_schema_change='ignore'
    )
}}

WITH incr_data AS (SELECT * FROM {{ ref('model_a') }} )

SELECT * FROM incr_data

{% if is_incremental()  %}

WHERE id NOT IN (SELECT id from {{ this}} )

{% endif %}