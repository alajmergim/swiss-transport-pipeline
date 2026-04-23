-- macros/to_date_ch.sql
{% macro to_date_ch(column_name) %}
    strptime({{ column_name }}, '%d.%m.%Y')::DATE
{% endmacro %}


-- macros/to_date_ch.sql
{% macro to_timestamp_ch(column_name) %}
    CASE 
        -- If it has seconds (length is 19: DD.MM.YYYY HH:MM:SS)
        WHEN LENGTH({{ column_name }}) = 19 
            THEN strptime({{ column_name }}, '%d.%m.%Y %H:%M:%S')::TIMESTAMP
        -- If it only has minutes (length is 16: DD.MM.YYYY HH:MM)
        WHEN LENGTH({{ column_name }}) = 16 
            THEN strptime({{ column_name }}, '%d.%m.%Y %H:%M')::TIMESTAMP
        ELSE NULL 
    END
{% endmacro %}