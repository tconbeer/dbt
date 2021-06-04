{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id, field1, field2 FROM source_data

{% else %}

SELECT id, field1, field3 FROm source_data

{% endif %}