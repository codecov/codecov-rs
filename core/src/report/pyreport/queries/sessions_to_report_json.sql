with samples_categorized as (
select
  coverage_sample.raw_upload_id,
  coverage_sample.local_sample_id,
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
  method_data.raw_upload_id = coverage_sample.raw_upload_id
  and method_data.local_sample_id = coverage_sample.local_sample_id
)
select
  cast(row_number() over (order by raw_upload.id) - 1 as text) as session_index,
  raw_upload.id,
  count(distinct samples_categorized.source_file_id) as session_files,
  count(*) as session_lines,
  sum(samples_categorized.hit) as session_hits,
  sum(samples_categorized.miss) as session_misses,
  sum(samples_categorized.partial) as session_partials,
  sum(iif(samples_categorized.coverage_type = 'b', 1, 0)) as session_branches,
  sum(iif(samples_categorized.coverage_type = 'm', 1, 0)) as session_methods,
  coalesce(sum(samples_categorized.hit_complexity_paths), 0) as session_hit_complexity_paths,
  coalesce(sum(samples_categorized.total_complexity), 0) as session_total_complexity,
  raw_upload.timestamp,
  raw_upload.raw_upload_url,
  raw_upload.flags,
  raw_upload.provider,
  raw_upload.build,
  raw_upload.name,
  raw_upload.job_name,
  raw_upload.ci_run_url,
  raw_upload.state,
  raw_upload.env,
  raw_upload.session_type,
  raw_upload.session_extras
from
  samples_categorized
left join
  raw_upload
on
  raw_upload.id = samples_categorized.raw_upload_id
group by
  2
