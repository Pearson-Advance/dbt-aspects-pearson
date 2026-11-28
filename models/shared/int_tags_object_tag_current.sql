{{ config(tags=["shared", "shared_tags"]) }}

{#
    Same latest-row-per-id resolution as aspects-dbt's
    dim_most_recent_object_tags, which drops the `tag` column (tag id). Keeping
    it lets downstream models join dim_most_recent_tags by id instead of by
    tag text.

    The newest dump per id wins; dump_id breaks ties between dumps with the
    same time_last_dumped, so each id yields exactly one row.
#}
select
    id,
    argMax(object_id, (time_last_dumped, dump_id)) as object_id,
    argMax(taxonomy, (time_last_dumped, dump_id)) as taxonomy,
    argMax(tag, (time_last_dumped, dump_id)) as tag,
    argMax(_value, (time_last_dumped, dump_id)) as _value,
    argMax(lineage, (time_last_dumped, dump_id)) as lineage
from {{ source("event_sink", "object_tag") }}
group by id
