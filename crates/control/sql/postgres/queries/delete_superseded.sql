WITH authorized AS MATERIALIZED (
  SELECT set_config('walrus.manifest_delete_protocol', '2', true) AS protocol
), candidate_group_ids AS MATERIALIZED (
  SELECT DISTINCT stream_group_id AS id
  FROM walrus.file_manifest
  WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
    AND kind <> 'reload' AND lsn_end <= $4
    AND stream_group_id IS NOT NULL
), locked_groups AS MATERIALIZED (
  -- Parent first is the common lock order used by integrity fencing and replay validation.
  SELECT g.id, g.expected_files, g.row_count, g.file_shape, g.status
  FROM walrus.stream_manifest_group AS g
  JOIN candidate_group_ids AS candidate ON candidate.id = g.id
  ORDER BY g.id
  FOR UPDATE OF g
), locked_files AS MATERIALIZED (
  SELECT manifest.id, manifest.stream_group_id, manifest.stream_group_ordinal,
         manifest.kind, manifest.status, manifest.row_count, manifest.lsn_start,
         manifest.lsn_end, manifest.schema_version
  FROM walrus.file_manifest AS manifest
  WHERE manifest.epoch = $1
    AND manifest.source_schema = $2
    AND manifest.source_table = $3
    AND manifest.kind <> 'reload'
    AND manifest.lsn_end <= $4
  ORDER BY manifest.id
  FOR UPDATE OF manifest
), group_stats AS MATERIALIZED (
  SELECT g.id, g.expected_files, g.row_count, g.file_shape, g.status,
         count(file.id)::bigint AS actual_files,
         COALESCE(sum(file.row_count), 0)::bigint AS actual_rows,
         count(DISTINCT file.stream_group_ordinal)::bigint AS distinct_ordinals,
         min(file.stream_group_ordinal) AS min_ordinal,
         max(file.stream_group_ordinal) AS max_ordinal,
         bool_and(file.kind IN ('stream', 'spill')) AS kinds_valid,
         bool_and(file.status = g.status) AS statuses_valid,
         jsonb_agg(
           jsonb_build_object(
             'kind', file.kind,
             'row_count', file.row_count,
             'lsn_start', file.lsn_start - '0/0'::pg_lsn,
             'lsn_end', file.lsn_end - '0/0'::pg_lsn,
             'schema_version', file.schema_version
           )
           ORDER BY file.kind, file.row_count, file.lsn_start, file.lsn_end,
                    file.schema_version
         ) AS actual_shape
  FROM locked_groups AS g
  LEFT JOIN locked_files AS file ON file.stream_group_id = g.id
  GROUP BY g.id, g.expected_files, g.row_count, g.file_shape, g.status
), validation AS MATERIALIZED (
  SELECT count(*) FILTER (
    WHERE status NOT IN ('ready', 'failed')
       OR expected_files <> actual_files
       OR row_count <> actual_rows
       OR distinct_ordinals <> expected_files
       OR min_ordinal <> 0
       OR max_ordinal <> expected_files - 1
       OR kinds_valid IS NOT TRUE
       OR statuses_valid IS NOT TRUE
       OR file_shape <> actual_shape
  )::bigint AS invalid_groups
  FROM group_stats
), superseded_groups AS (
  UPDATE walrus.stream_manifest_group AS group_receipt
  SET status = 'superseded', applied_at = now()
  WHERE group_receipt.id IN (SELECT id FROM locked_groups)
    AND group_receipt.status IN ('ready', 'failed')
    AND (SELECT invalid_groups FROM validation) = 0
  RETURNING group_receipt.id
), deleted AS (
  DELETE FROM walrus.file_manifest AS manifest
  USING locked_files AS locked
  WHERE manifest.id = locked.id
    AND (SELECT protocol = '2' FROM authorized)
    AND (SELECT invalid_groups FROM validation) = 0
  RETURNING manifest.id
)
SELECT (SELECT invalid_groups FROM validation) AS invalid_groups,
       (SELECT count(*)::bigint FROM deleted) AS deleted_count,
       (SELECT count(*)::bigint FROM superseded_groups) AS superseded_group_count
