{{
 config(
 materialized = 'incremental',
 on_schema_change='fail'
 )
}}
WITH src_reviews AS (
 SELECT 
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_id,
    * 
    FROM {{ ref('src_reviews') }}
)
SELECT *, CURRENT_TIMESTAMP() as load_date FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %}
    {% if var('start_date', false) and var('end_date', false) %}
        {{ log('Loading ' ~ this ~ ' incrementally ( start_date: ' ~ var("start_date") ~ ' end_date: ' ~ var("end_date") , info=True) }}
        AND review_date >= '{{ var("start_date") }}'
        AND review_date < '{{ var("end_date") }}'
    {% else %}
        AND review_date > (select max(review_date) from {{ this }})
        {{ log('Loading ' ~ this ~ ' incrementally ( all missing values )' , info=True) }}
    {% endif %}
{% endif %}
