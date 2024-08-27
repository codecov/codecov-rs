with uploads as (
select
  count(*) as count
from
  raw_upload
),
test_cases as (
select
  count(*) as count
from
  context
),
files as (
select
  count(*) as count
from
  source_file
)
select
  (select files.count from files) as file_count,
  (select uploads.count from uploads) as upload_count,
  (select test_cases.count from test_cases) as test_case_count,
  sum(iif(coverage_sample.coverage_type = 'l' and coverage_sample.hits > 0, 1, 0)) as hit_lines,
  sum(iif(coverage_sample.coverage_type = 'l', 1, 0)) as total_lines,
  sum(iif(coverage_sample.coverage_type = 'b', coverage_sample.hit_branches, 0)) as hit_branches,
  sum(iif(coverage_sample.coverage_type = 'b', coverage_sample.total_branches, 0)) as total_branches,
  sum(iif(coverage_sample.coverage_type = 'b', 1, 0)) as total_branch_roots,
  sum(iif(coverage_sample.coverage_type = 'm' and coverage_sample.hits > 0, 1, 0)) as hit_methods,
  sum(iif(coverage_sample.coverage_type = 'm', 1, 0)) as total_methods,
  sum(iif(coverage_sample.coverage_type = 'm', method_data.hit_complexity_paths, 0)) as hit_complexity_paths,
  sum(iif(coverage_sample.coverage_Type = 'm', method_data.total_complexity, 0)) as total_complexity
from
  coverage_sample
left join
  method_data
on
  coverage_sample.raw_upload_id = method_data.raw_upload_id
  and coverage_sample.local_sample_id = method_data.local_sample_id
