with session_indices as (
select
  row_number() over (order by raw_upload.id) - 1 as session_index,
  raw_upload.id as raw_upload_id
from
  raw_upload
),
chunks_file_indices as (
select
  row_number() over (order by source_file.id) - 1 as chunk_index,
  source_file.id as source_file_id,
  json_group_array(distinct session_indices.session_index order by session_indices.session_index asc) as present_sessions
from
  source_file
left join
  coverage_sample
on
  coverage_sample.source_file_id = source_file.id
left join
  session_indices
on
  coverage_sample.raw_upload_id = session_indices.raw_upload_id
group by
  2
),
formatted_span_data as (
select
  span_data.raw_upload_id,
  span_data.local_span_id,
  span_data.local_sample_id,
  json_array(span_data.start_col, span_data.end_col, span_data.hits) as pyreport_partial
from
  span_data
),
line_sessions as (
select
  chunks_file_indices.chunk_index,
  chunks_file_indices.present_sessions,
  coverage_sample.line_no,
  session_indices.session_index,
  coverage_sample.coverage_type,
  coverage_sample.hits,
  coverage_sample.hit_branches,
  coverage_sample.total_branches,
  method_data.hit_complexity_paths,
  method_data.total_complexity,
  -- The `order by` below is not strictly necessary, it just makes writing test cases easier
  json_group_array(branches_data.branch order by branches_data.branch) filter (where branches_data.branch is not null and branches_data.hits = 0) as missing_branches,
  json_group_array(json(formatted_span_data.pyreport_partial)) filter (where formatted_span_data.pyreport_partial is not null) as partials,
  json_group_array(context.name) filter (where context.name is not null) as labels
from
  coverage_sample
left join
  branches_data
on
  branches_data.raw_upload_id = coverage_sample.raw_upload_id
  and branches_data.local_sample_id = coverage_sample.local_sample_id
left join
  method_data
on
  method_data.raw_upload_id = coverage_sample.raw_upload_id
  and method_data.local_sample_id = coverage_sample.local_sample_id
left join
  formatted_span_data
on
  formatted_span_data.raw_upload_id = coverage_sample.raw_upload_id
  and formatted_span_data.local_sample_id = coverage_sample.local_sample_id
left join
  chunks_file_indices
on
  chunks_file_indices.source_file_id = coverage_sample.source_file_id
left join
  session_indices
on
  session_indices.raw_upload_id = coverage_sample.raw_upload_id
left join
  context_assoc
on
  context_assoc.raw_upload_id = coverage_sample.raw_upload_id
  and context_assoc.local_sample_id = coverage_sample.local_sample_id
left join
  context
on
  context_assoc.context_id = context.id
group by 1, 2, 3, 4
order by 1, 2, 3, context.name
),
report_line_totals as (
select
  line_sessions.chunk_index,
  line_sessions.line_no,
  sum(line_sessions.hits) as hits,
  sum(line_sessions.hit_branches) as hit_branches,
  sum(line_sessions.total_branches) as total_branches,
  sum(line_sessions.hit_complexity_paths) as hit_complexity_paths,
  sum(line_sessions.total_complexity) as total_complexity
from
  line_sessions
group by
  1, 2
)
select
  line_sessions.chunk_index,
  line_sessions.line_no,
  line_sessions.coverage_type,
  report_line_totals.hits as report_line_hits,
  report_line_totals.hit_branches as report_line_hit_branches,
  report_line_totals.total_branches as report_line_total_branches,
  report_line_totals.hit_complexity_paths as report_line_hit_complexity_paths,
  report_line_totals.total_complexity as report_line_total_complexity,
  line_sessions.session_index,
  line_sessions.present_sessions,
  line_sessions.hits,
  line_sessions.hit_branches,
  line_sessions.total_branches,
  line_sessions.hit_complexity_paths,
  line_sessions.total_complexity,
  iif(line_sessions.missing_branches = json_array(), null, line_sessions.missing_branches) as missing_branches,
  iif(json(line_sessions.partials) = json_array(), null, json(line_sessions.partials)) as partials,
  iif(line_sessions.labels = json_array(), null, line_sessions.labels) as labels
from
  line_sessions
left join
  report_line_totals
on
  line_sessions.chunk_index = report_line_totals.chunk_index
  and line_sessions.line_no = report_line_totals.line_no
