WITH authorized AS MATERIALIZED (
  SELECT set_config('walrus.manifest_delete_protocol', '2', true) AS protocol
)
DELETE FROM walrus.file_manifest
WHERE reload_id = $1
  AND (SELECT protocol = '2' FROM authorized)
