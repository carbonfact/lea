# Changelog

## v0.18.0

### Breaking changes

- Replaced provider-specific DuckLake secret env vars (`LEA_DUCKLAKE_R2_KEY_ID`, `LEA_DUCKLAKE_GCS_KEY_ID`, `LEA_DUCKLAKE_S3_ENDPOINT`, etc.) with a single `LEA_DUCKLAKE_SECRET` variable. The value is the body of a DuckDB [`CREATE SECRET`](https://duckdb.org/docs/current/configuration/secrets_manager) statement. Same for quack mode with `LEA_QUACK_DUCKLAKE_SECRET`. This supports any secret type DuckDB supports (S3, GCS, R2, Azure, etc.) without lea needing provider-specific code.

### Bug fixes

- Fixed MotherDuck hanging when running concurrent scripts. The `MotherDuckClient` now uses a persistent connection with cursor-based thread safety, matching the pattern used by `DuckLakeClient`.
