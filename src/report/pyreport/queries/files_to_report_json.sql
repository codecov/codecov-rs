-- Determine whether each `coverage_sample` record is a hit/miss/partial/skip.
-- Normalize complexity fields.
with samples_categorized as (
select
  coverage_sample.raw_upload_id,
  coverage_sample.local_sample_id,
  coverage_sample.source_file_id,
  coverage_sample.line_no,
  coverage_sample.coverage_type,
  iif(
    coverage_sample.hits > 0 or coverage_sample.hit_branches >= coverage_sample.total_branches,
    2,     -- hit
    iif(
      coverage_sample.hits = 0 or coverage_sample.hit_branches = 0,
      0,   -- miss
      iif(
        coverage_sample.hit_branches > 0 and coverage_sample.hit_branches < coverage_sample.total_branches,
        1, -- partial
        -1 -- skipped
      )
    )
  ) as coverage_status,
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
),
-- Compute the chunks file index of each `source_file` record. Must match the
-- corresponding logic in `samples_to_chunks.sql`.
source_files_with_index as (
select
  row_number() over (order by source_file.id) - 1 as chunk_index,
  source_file.id,
  source_file.path
from
  source_file
),
-- Each (source_file, line) has potentially many samples from different sessions
-- and this CTE flattens them into a single record per (source_file, line).
file_lines_flattened as (
select
  samples_categorized.source_file_id,
  samples_categorized.line_no,
  samples_categorized.coverage_type,
  max(samples_categorized.coverage_status) as coverage_status,
  max(samples_categorized.hit_complexity_paths) as hit_complexity_paths,
  max(samples_categorized.total_complexity) as total_complexity
from
  samples_categorized
group by
  1, 2, 3
)
select
  source_files_with_index.chunk_index,
  source_files_with_index.id,
  source_files_with_index.path,
  count(*) as file_lines,
  sum(iif(file_lines_flattened.coverage_status = 2, 1, 0)) as file_hits,
  sum(iif(file_lines_flattened.coverage_status = 0, 1, 0)) as file_misses,
  sum(iif(file_lines_flattened.coverage_status = 1, 1, 0)) as file_partials,
  sum(iif(file_lines_flattened.coverage_type = 'b', 1, 0)) as file_branches,
  sum(iif(file_lines_flattened.coverage_type = 'm', 1, 0)) as file_methods,
  coalesce(sum(file_lines_flattened.hit_complexity_paths), 0) as file_hit_complexity_paths,
  coalesce(sum(file_lines_flattened.total_complexity), 0) as file_total_complexity
from
  file_lines_flattened
left join
  source_files_with_index
on
  file_lines_flattened.source_file_id = source_files_with_index.id
group by
  1, 2, 3
