{% macro convert_to_timestamp(date_col, time_col) %}

(
    case
        when {{ time_col }} is null
          or trim({{ time_col }}) = ''
          or {{ time_col }} in ('0','00','000','0000')
        then null
        when {{ time_col }} = '2400'
        then dateadd(
                'day', 1,
                to_timestamp_ntz({{ date_col }})
             )
        when length({{ time_col }}) < 4
        then
            try_to_timestamp_ntz(
                {{ date_col }} || ' ' ||
                substr(lpad({{ time_col }}, 4, '0'), 1, 2)
                || ':' ||
                substr(lpad({{ time_col }}, 4, '0'), 3, 2)
            )

        else
            try_to_timestamp_ntz(
                {{ date_col }} || ' ' ||
                substr({{ time_col }}, 1, 2)
                || ':' ||
                substr({{ time_col }}, 3, 2)
            )
    end
)

{% endmacro %}
