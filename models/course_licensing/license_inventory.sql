{{ 
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("ReplacingMergeTree()"),
        primary_key="(license_id)",
        order_by="(license_id)",
    ) 
}}

-- Step 1: Get the latest licenses for an institution
WITH latest_licenses AS (
    SELECT
        id AS license_id,  -- License identifier
        license_name,  -- Name of the license
        institution_id,  -- Institution identifier
        MAX(time_last_dumped) AS latest_time_last_dumped  -- Most recent time for license data dump
    FROM {{ source("event_sink", "course_licensing_license") }}
    GROUP BY id, license_name, institution_id  -- Group by license id, name and institution_id to get unique licenses
),

-- Step 2: Find the most recent record for each license
latest_license_records AS (
    SELECT
        license_id,  -- License identifier
        MAX(latest_time_last_dumped) AS latest_time_last_dumped  -- Latest dump time for each license
    FROM latest_licenses
    GROUP BY license_id  -- Group by license to get latest record for each
),

-- Step 3: Map licenses to CCX class IDs
ccx_classes_by_license AS (
    SELECT
        license_id,  -- License identifier
        ccx_id AS class_id  -- Corresponding CCX class ID
    FROM {{ source("event_sink", "course_licensing_institution_ccx") }}
),

-- Step 4: Count active or expired enrollments per license
enrollments_by_license AS (
    SELECT
        enrollment.license_id,  -- License identifier
        COUNT(*) AS enrolled  -- Count of enrollments
    FROM {{ source("event_sink", "course_licensing_licensed_enrollment") }} enrollment
    JOIN (
        SELECT id, MAX(time_last_dumped) AS latest_time_last_dumped  -- Latest enrollment records
        FROM {{ source("event_sink", "course_licensing_licensed_enrollment") }}
        GROUP BY id  -- Group by enrollment id to get the latest data
    ) latest_enrollment_records
    ON enrollment.id = latest_enrollment_records.id
    AND enrollment.time_last_dumped = latest_enrollment_records.latest_time_last_dumped  -- Only select latest records
    WHERE enrollment.is_active = 'True' OR enrollment.expired = 'True'  -- Include active or expired enrollments
    GROUP BY enrollment.license_id  -- Group by license to count enrollments per license
),

-- Step 5: Calculate pending enrollments
allowed_enrollments AS (
    SELECT
        allowed.course_id,  -- CCX course identifier
        COUNT(DISTINCT allowed.email) AS pending  -- Count of unique pending enrollments
    FROM {{ source("event_sink", "openedx_course_enrollment_allowed") }} allowed
    WHERE allowed.course_id IN (  -- Only consider courses associated with the licenses
        SELECT class_id FROM ccx_classes_by_license
    )
    AND lower(allowed.email) NOT IN (  -- Exclude students who are already enrolled
        SELECT lower(student_email)
        FROM {{ source("event_sink", "course_licensing_licensed_enrollment") }}
    )
    GROUP BY allowed.course_id  -- Group by course ID to count pending enrollments per course
),

-- Step 6: Summarize pending enrollments by license
allowed_enrollments_by_license AS (
    SELECT
        ccx.license_id,             -- License identifier
        SUM(allowed.pending) AS pending -- Total pending enrollments for this license
    FROM ccx_classes_by_license ccx
    LEFT JOIN allowed_enrollments allowed
    ON ccx.class_id = allowed.course_id -- Join on course ID to match licenses with pending enrollments
    GROUP BY ccx.license_id         -- Group by license to sum pending enrollments per license
)

-- Step 7: Final selection and calculation of key metrics
SELECT
    latest_licenses.license_id AS license_id,  -- License identifier
    MAX(latest_licenses.institution_id) AS institution_id,  -- Institution identifier
    MAX(latest_licenses.license_name) AS license_name,  -- License name
    SUM(toInt32(license_order.purchased_seats)) AS purchased_seats,  -- Total purchased seats
    COALESCE(MAX(enrollments_by_license.enrolled), 0) AS enrolled,  -- Number of enrollments, or 0 if none
    COALESCE(MAX(allowed_enrollments_by_license.pending), 0) AS pending,  -- Pending enrollments, or 0 if none
    COALESCE(MAX(enrollments_by_license.enrolled), 0) 
        + COALESCE(MAX(allowed_enrollments_by_license.pending), 0) AS enrollments_and_allowed,  -- Sum of enrolled and pending
    SUM(toInt32(license_order.purchased_seats)) 
        - COALESCE(MAX(enrollments_by_license.enrolled), 0)
        - COALESCE(MAX(allowed_enrollments_by_license.pending), 0) AS remaining  -- Remaining seats after enrollments and pending
FROM
    latest_licenses
JOIN
    latest_license_records
ON
    latest_licenses.license_id = latest_license_records.license_id
    AND latest_licenses.latest_time_last_dumped = latest_license_records.latest_time_last_dumped  -- Only use latest records
JOIN
    {{ source("event_sink", "course_licensing_license_order") }} license_order
ON
    latest_licenses.license_id = license_order.license_id  -- Join to get purchased seats for each license
LEFT JOIN
    enrollments_by_license
ON
    latest_licenses.license_id = enrollments_by_license.license_id  -- Left join to include enrollments data
LEFT JOIN
    allowed_enrollments_by_license
ON
    latest_licenses.license_id = allowed_enrollments_by_license.license_id  -- Left join to include pending enrollments data
GROUP BY
    latest_licenses.license_id  -- Group by license id for final result
