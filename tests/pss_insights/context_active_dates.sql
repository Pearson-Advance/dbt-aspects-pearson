{{ config(tags=["pss_context"]) }}

with
    cases as (
        select *
        from
        values
            (
                'class_start Nullable(Date), class_end Nullable(Date), reporting_date Date, expected Nullable(UInt8)',
                ('2026-11-10', '2026-11-12', '2026-11-09', 0),
                ('2026-11-10', '2026-11-12', '2026-11-10', 1),
                ('2026-11-10', '2026-11-12', '2026-11-11', 1),
                ('2026-11-10', '2026-11-12', '2026-11-12', 1),
                ('2026-11-10', '2026-11-12', '2026-11-13', 0),
                ('2026-11-10', null, '2026-11-10', null),
                (null, '2026-11-12', '2026-11-11', null),
                ('2026-11-12', '2026-11-10', '2026-11-11', null)
            )
    )
select *
from cases
where
    ifNull({{ pss_context_active_on("class_start", "class_end", "reporting_date") }}, 2)
    != ifNull(expected, 2)
