{% test total_state_cases_match_national(model, column_name) %}

with state_totals as (
    select 
        date,
        sum(cases_total) as total_state_cases
    from {{ model }}
    group by date
),
national_totals as (
    select 
        date,
        cases_total as national_cases
    from {{ ref('daily_cases') }}
)

select 
    st.date,
    st.total_state_cases,
    nt.national_cases
from state_totals st
join national_totals nt on st.date = nt.date
where abs(st.total_state_cases - nt.national_cases) > 1000  -- Allow small discrepancy for data collection differences

{% endtest %}