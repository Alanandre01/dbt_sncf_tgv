-- Retourne NULL si le cast échoue, évite les erreurs de pipeline
{% macro safe_cast(column, type) %}
    TRY_CAST({{ column }} AS {{ type }})
{% endmacro %}
