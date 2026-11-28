-- Run once on a disposable reporting/event_sink pair with the dbt models built.
-- These inserts exercise the normal insert-triggered Course Licensing materialized views.
SELECT throwIf(count() != 0, 'This replay requires an empty disposable class event sink')
FROM event_sink.course_licensing_institution_ccx;

INSERT INTO event_sink.course_licensing_institution (source_id, id, active, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('1','1','True','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_course (source_id, master_course_id, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('9','course-v1:EXAMPLE+DEMO+RUN','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_license (source_id, id, institution_id, status, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('7','7','1','active','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_institution_ccx
(source_id, id, institution_id, ccx_id, ccx_name, master_course, license_id, license_status, deleted, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES
('11','11','1','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','A','course-v1:EXAMPLE+DEMO+RUN','7','active','False','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
('12','12','1','ccx-v1:EXAMPLE+DEMO+RUN+ccx@102','B','course-v1:EXAMPLE+DEMO+RUN','7','active','False','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
('11','11','1','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','A','course-v1:EXAMPLE+DEMO+RUN','7','active','False','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.course_overviews (org, course_key, course_start, course_end, modified, time_last_dumped)
VALUES
('EXAMPLE','course-v1:EXAMPLE+DEMO+RUN','2026-01-01','2026-12-31','2026-11-09','2026-11-09 08:00:01'),
('EXAMPLE','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','2026-11-10','2026-11-12','2026-11-09','2026-11-09 08:00:01');

INSERT INTO event_sink.external_id (user_id, external_id_type, external_user_id, time_last_dumped)
VALUES (1001,'xapi','ab000000-0000-4000-8000-000000000001','2026-11-09 08:00:01'),
       (1002,'xapi','ab000000-0000-4000-8000-000000000002','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_licensed_enrollment
(source_id, id, user_id, institution_id, institution_ccx_id, class_id, is_active, expired, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES
('21','21','1001','1','11','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','True','False','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
('22','22','1002','1','11','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','True','False','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
('23','23','1001','1','12','ccx-v1:EXAMPLE+DEMO+RUN+ccx@102','True','False','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
('21','21','1001','1','11','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','True','False','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.user_profile (id, user_id, username, time_last_dumped)
VALUES (91,2001,'pss_coach_a','2026-11-09 08:00:01'), (92,2002,'pss_coach_b','2026-11-09 08:00:01'),
       (93,2003,'pss_admin','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_instructor (source_id, user_id, username, user_is_active, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('31','2001','pss_coach_a','True','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
       ('32','2002','pss_coach_b','True','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_instructor_institution
(source_id, instructor_id, user_id, username, institution_id, active, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('41','31','2001','pss_coach_a','1','True','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
       ('42','32','2002','pss_coach_b','1','True','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_instructor_class
(source_id, instructor_id, user_id, username, institution_id, institution_ccx_id, class_id, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('51','31','2001','pss_coach_a','1','11','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01'),
       ('52','32','2002','pss_coach_b','1','11','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

INSERT INTO event_sink.course_licensing_institution_administrator
(source_id, id, user_id, user_name, institution_id, active, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('61','61','2003','pss_admin','1','True','UPSERT','False','2026-11-09 08:00:00','2026-11-09 08:00:01');

SELECT throwIf(count() != 2 OR countIf(class_start = '2026-11-10' AND class_end = '2026-11-12') != 1, 'Class replay or override dates')
FROM reporting.int_pss_class_context;

SELECT throwIf(count() != 3 OR uniqExact(actor_id) != 2, 'Registered learners with repeated enrollment and no activity')
FROM reporting.int_pss_learner_class_context WHERE is_registered;

SELECT throwIf(count() != 4, 'Two instructors in A and one administrator for A and B')
FROM reporting_pss_restricted.int_pss_instructor_class_access;

-- The administrator can see A and B. Instructor grants do not multiply either roster.
SELECT throwIf(count() != 3 OR uniqExact(m.actor_id) != 2, 'Authorized counts across classes')
FROM reporting.int_pss_learner_class_context AS m
INNER JOIN reporting.int_pss_class_context AS c ON m.ccx_id = c.ccx_id
WHERE m.is_registered = 1 AND (c.institution_id, c.ccx_id) IN (
    SELECT institution_id, ccx_id FROM reporting_pss_restricted.int_pss_instructor_class_access
    WHERE principal_username = 'pss_admin'
);

-- A more recent tombstone removes only this class assignment.
INSERT INTO event_sink.course_licensing_instructor_class
(source_id, instructor_id, user_id, username, institution_id, institution_ccx_id, class_id, operation, is_deleted, source_updated_at, time_last_dumped)
VALUES ('51','31','2001','pss_coach_a','1','11','ccx-v1:EXAMPLE+DEMO+RUN+ccx@101','DELETE','True','2026-11-09 09:00:00','2026-11-09 09:00:01');

SELECT throwIf(count() != 3 OR countIf(principal_username = 'pss_coach_a') != 0, 'Revoked instructor retained access')
FROM reporting_pss_restricted.int_pss_instructor_class_access;

-- An older replay after the class tombstone cannot restore B or its memberships or grants.
INSERT INTO event_sink.course_licensing_institution_ccx
(source_id, id, institution_id, ccx_id, deleted, is_deleted, operation, source_updated_at, time_last_dumped)
VALUES ('12','12','1','ccx-v1:EXAMPLE+DEMO+RUN+ccx@102','False','True','DELETE','2026-11-09 10:00:00','2026-11-09 10:00:01'),
       ('12','12','1','ccx-v1:EXAMPLE+DEMO+RUN+ccx@102','False','False','UPSERT','2026-11-09 08:00:00','2026-11-09 08:00:01');

SELECT throwIf(count() != 1 OR min(ccx_id) != 'ccx-v1:EXAMPLE+DEMO+RUN+ccx@101', 'Class tombstone lost to a replay')
FROM reporting.int_pss_class_context;
SELECT throwIf(count() != 2 OR countIf(ccx_id = 'ccx-v1:EXAMPLE+DEMO+RUN+ccx@102') != 0, 'Removed class retained roster')
FROM reporting.int_pss_learner_class_context;
SELECT throwIf(count() != 2 OR countIf(ccx_id = 'ccx-v1:EXAMPLE+DEMO+RUN+ccx@102') != 0, 'Removed class retained grants')
FROM reporting_pss_restricted.int_pss_instructor_class_access;
