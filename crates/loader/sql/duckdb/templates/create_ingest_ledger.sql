CREATE TABLE IF NOT EXISTS "_walrus_ingested_files" (
    "s3_uri" VARCHAR PRIMARY KEY,
    "manifest_id" BIGINT NOT NULL
);
