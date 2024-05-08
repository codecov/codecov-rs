with label_indices as (
select
  row_number() over (order by context.id) as label_index,
  context.name
from
  context
where
  context.context_type <> 'Upload'
)
select
  json_group_object(label_indices.label_index, label_indices.name) as labels_index
from label_indices
