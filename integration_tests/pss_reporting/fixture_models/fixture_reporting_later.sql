{{ pss_class_delivery_by_type(
    ref("fixture_reporting_scope"),
    "toDateTime64('2026-11-02 17:00:00', 6, 'UTC')",
    "toDateTime64('2026-11-02 18:00:00', 6, 'UTC')"
) }}
