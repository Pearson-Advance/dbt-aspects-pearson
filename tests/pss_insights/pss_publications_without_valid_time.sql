{{ config(tags=["pss_insights"], severity="warn") }}

{#
    A source row whose published_at cannot be parsed never becomes the latest
    publication, but it should be reviewed and removed from the source.
#}
select master_course, snapshot_status
from {{ ref("int_pss_assignment_publications") }}
where snapshot_status = 'invalid_publication_time'
