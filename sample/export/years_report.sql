select
    s.Year,
    s.Industry_name_NZSIOC,
    y.description as year_description,
    s.Industry_aggregation_NZSIOC,
    s.Units,
    s.Value
from
    annual_enterprise_survey s
join
    years y on s.Year = y.year