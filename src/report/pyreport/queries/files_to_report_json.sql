with samples_categorized as (
select
  coverage_sample.id,
  coverage_sample.source_file_id,
  coverage_sample.line_no,
  coverage_sample.coverage_type,
  iif(coverage_sample.hits > 0 or coverage_sample.hit_branches >= coverage_sample.total_branches, 1, 0) as hit,
  iif(coverage_sample.hits = 0 or coverage_sample.hit_branches = 0, 1, 0) as miss,
  iif(coverage_sample.hit_branches > 0 and coverage_sample.hit_branches < coverage_sample.total_branches, 1, 0) as partial,
  -- If a pyreport only has total_complexity, it will basically swap total_complexity and hit_complexity_paths
  -- when pre-computing its totals/statistics. This logic performs that swap here.
  iif(method_data.hit_complexity_paths is null, method_data.total_complexity, method_data.hit_complexity_paths) as hit_complexity_paths,
  iif(method_data.hit_complexity_paths is null, null, method_data.total_complexity) as total_complexity
from
  coverage_sample
left join
  method_data
on
  method_data.sample_id = coverage_sample.id
),
file_sessions_flattened as (
select
  samples_categorized.source_file_id,
  samples_categorized.line_no,
  samples_categorized.coverage_type,
  max(samples_categorized.hit) as hit,
  max(samples_categorized.miss) as miss,
  max(samples_categorized.partial) as partial,
  max(samples_categorized.hit_complexity_paths) as hit_complexity_paths,
  max(samples_categorized.total_complexity) as total_complexity
from
  samples_categorized
group by
  1, 2, 3
),
file_totals as (
select
  file_sessions_flattened.source_file_id,
  count(*) as file_lines,
  sum(file_sessions_flattened.hit) as file_hits,
  sum(file_sessions_flattened.miss) as file_misses,
  sum(file_sessions_flattened.partial) as file_partials,
  sum(iif(file_sessions_flattened.coverage_type = 'b', 1, 0)) as file_branches,
  sum(iif(file_sessions_flattened.coverage_type = 'm', 1, 0)) as file_methods,
  coalesce(sum(file_sessions_flattened.hit_complexity_paths), 0) as file_hit_complexity_paths,
  coalesce(sum(file_sessions_flattened.total_complexity), 0) as file_total_complexity
from
  file_sessions_flattened
group by
  1
),
session_indices as (
select
  cast(row_number() over (order by context.id) - 1 as text) as session_index,
  context.id as context_id
from
  context
where
  context.context_type = 'Upload'
),
file_session_totals as (
select
  session_indices.session_index,
  context.id,
  samples_categorized.source_file_id,
  count(*) as file_session_lines,
  sum(samples_categorized.hit) as file_session_hits,
  sum(samples_categorized.miss) as file_session_misses,
  sum(samples_categorized.partial) as file_session_partials,
  coalesce(sum(samples_categorized.hit_complexity_paths), 0) as file_session_hit_complexity_paths,
  coalesce(sum(samples_categorized.total_complexity), 0) as file_session_total_complexity
from
  samples_categorized
left join
  context_assoc
on
  context_assoc.sample_id = samples_categorized.id
left join
  context
on
  context.id = context_assoc.context_id
left join
  session_indices
on
  session_indices.context_id = context.id
where
  context.context_type = 'Upload'
group by
  1, 2, 3
)
select
  row_number() over (order by source_file.id) - 1 as chunk_index,
  source_file.id,
  source_file.path,
  file_totals.file_lines,
  file_totals.file_hits,
  file_totals.file_misses,
  file_totals.file_partials,
  file_totals.file_branches,
  file_totals.file_methods,
  file_totals.file_hit_complexity_paths,
  file_totals.file_total_complexity,
  file_session_totals.session_index,
  file_session_totals.file_session_lines,
  file_session_totals.file_session_hits,
  file_session_totals.file_session_misses,
  file_session_totals.file_session_partials,
  file_session_totals.file_session_hit_complexity_paths,
  file_session_totals.file_session_total_complexity
from
  source_file
left join
  file_totals
on
  source_file.id = file_totals.source_file_id
left join
  file_session_totals
on
  source_file.id = file_session_totals.source_file_id;

