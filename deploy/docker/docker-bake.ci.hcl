// CI builds both deployable images as one Bake group. BuildKit schedules the independent targets
// concurrently while preserving the per-image GHA cache scopes used by the previous serial steps.
group "default" {
  targets = ["pg-sink", "loader"]
}

target "pg-sink" {
  context    = "."
  dockerfile = "deploy/docker/Dockerfile.pg-sink"
  tags       = ["walrus-pg-sink:ci"]
  cache-from = ["type=gha,scope=pg-sink"]
  cache-to   = ["type=gha,scope=pg-sink,mode=max"]
}

target "loader" {
  context    = "."
  dockerfile = "deploy/docker/Dockerfile.loader"
  tags       = ["walrus-loader:ci"]
  cache-from = ["type=gha,scope=loader"]
  cache-to   = ["type=gha,scope=loader,mode=max"]
}
