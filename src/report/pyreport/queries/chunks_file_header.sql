with label_indices as (
select
  row_number() over (order by context.id) as label_index,
  context.name
from
  context
where
  context.context_type <> 'Upload'
),
labels_index as (
select
  json_group_object(label_indices.label_index, label_indices.name) as labels_index
from
  label_indices
)
select
  iif(
    labels_index.labels_index <> json_object(),
    json_object(
        'labels_index',
        json(labels_index.labels_index)
    ),
    json_object()
  ) as chunks_file_header
from labels_index
