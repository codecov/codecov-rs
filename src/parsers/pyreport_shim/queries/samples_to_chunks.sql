with session_indices as (
select
  row_number() over (order by context.id) - 1 as session_index,
  context.id as context_id
from
  context
where
  context.context_type = 'Upload'
),
chunks_file_indices as (
select
  row_number() over (order by source_file.id) - 1 as chunk_index,
  source_file.id as source_file_id,
  json_group_array(distinct session_indices.session_index) as present_sessions
from
  source_file
left join
  coverage_sample
on
  coverage_sample.source_file_id = source_file.id
left join
  context_assoc
on
  context_assoc.sample_id = coverage_sample.id
left join
  session_indices
on
  context_assoc.context_id = session_indices.context_id
group by
  2
),
other_contexts as (
select
  *
from
  context
where
  context.context_type <> 'Upload'
),
formatted_span_data as (
select
  span_data.id,
  span_data.sample_id,
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
  json_group_array(branches_data.branch) as missing_branches,
  json_group_array(formatted_span_data.pyreport_partial) as partials,
  json_group_array(other_contexts.name) as labels
from
  coverage_sample
left join
  context_assoc
on
  context_assoc.sample_id = coverage_sample.id
left join
  branches_data
on
  branches_data.sample_id = coverage_sample.id
left join
  method_data
on
  method_data.sample_id = coverage_sample.id
left join
  formatted_span_data
on
  formatted_span_data.sample_id = coverage_sample.id
left join
  chunks_file_indices
on
  chunks_file_indices.source_file_id = coverage_sample.source_file_id
left join
  session_indices
on
  session_indices.context_id = context_assoc.context_id
left join
  other_contexts
on
  other_contexts.id = context_assoc.context_id
group by 1, 2, 3
order by 1, 2, 3, other_contexts.name
)
select
  line_sessions.chunk_index,
  line_sessions.line_no,
  line_sessions.coverage_type,
  sum(line_sessions.hits) over win_line_sessions as report_line_hits,
  sum(line_sessions.hit_branches) over win_line_sessions as report_line_hit_branches,
  sum(line_sessions.total_branches) over win_line_sessions as report_line_total_branches,
  sum(line_sessions.hit_complexity_paths) over win_line_sessions as report_line_hit_complexity_paths,
  sum(line_sessions.total_complexity) over win_line_sessions as report_line_total_complexity,
  line_sessions.session_index,
  line_sessions.present_sessions,
  line_sessions.hits,
  line_sessions.hit_branches,
  line_sessions.total_branches,
  line_sessions.hit_complexity_paths,
  line_sessions.total_complexity,
  line_sessions.missing_branches,
  line_sessions.partials,
  line_sessions.labels
from
  line_sessions
window win_line_sessions as (partition by 1, 2)
