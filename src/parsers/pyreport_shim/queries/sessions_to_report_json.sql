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
)
select
  cast(row_number() over (order by context.id) - 1 as text) as session_index,
  context.id,
  count(distinct samples_categorized.source_file_id) as session_files,
  count(*) as session_lines,
  sum(samples_categorized.hit) as session_hits,
  sum(samples_categorized.miss) as session_misses,
  sum(samples_categorized.partial) as session_partials,
  sum(iif(samples_categorized.coverage_type = 'b', 1, 0)) as session_branches,
  sum(iif(samples_categorized.coverage_type = 'm', 1, 0)) as session_methods,
  coalesce(sum(samples_categorized.hit_complexity_paths), 0) as session_hit_complexity_paths,
  coalesce(sum(samples_categorized.total_complexity), 0) as session_total_complexity,
  upload_details.timestamp,
  upload_details.raw_upload_url,
  upload_details.flags,
  upload_details.provider,
  upload_details.build,
  upload_details.name,
  upload_details.job_name,
  upload_details.ci_run_url,
  upload_details.state,
  upload_details.env,
  upload_details.session_type,
  upload_details.session_extras
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
  upload_details
on
  upload_details.context_id = context.id
where
  context.context_type = 'Upload'
group by
  2
