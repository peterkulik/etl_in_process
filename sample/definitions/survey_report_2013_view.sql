create view survey_report_2013_view as
select
    Year,
    Industry_aggregation_NZSIOC,
    Units,
    Value
from
    annual_enterprise_survey
where
    year = 2019
    and Industry_aggregation_NZSIOC not in ('Level 1', 'Level 3')