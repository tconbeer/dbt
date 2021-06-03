select * from {{ ref('incremental') }} where false
