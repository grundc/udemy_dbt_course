
{% macro learn_variables() %}

    {# this is how JINJA variables work #}
    {% set my_name = "Christoph" %}
    {{ log("Hello " ~ my_name, info=True) }}

    {# this is how DBT variables work #}
    {{ log("Hello " ~ var("user_name", "DEFAULT VALUE") ~ "!", info=True)}}

{# dbt run-operation learn_variables #}
{# dbt run-operation learn_variables --vars '{"user_name": "Christoph"}' #}

{% endmacro %}


