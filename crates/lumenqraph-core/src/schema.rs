//! Database schema versioning shared by every service.
//!
//! Migrations are applied by one explicit step (`lumenqraph-indexer migrate`),
//! run as a compose init container, a Fly `release_command`, or by
//! `run-all-in-one.sh`. Every other service only *verifies* the schema at
//! startup with [`check`]: it refuses to start against a database that is
//! missing migrations this binary was built with, and reports migrations it
//! doesn't know about (a newer binary has already upgraded the schema) so the
//! caller can warn about version skew.

use sqlx::migrate::Migrator;
use sqlx::PgPool;

/// The migrations embedded in every binary at build time.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

/// How the database schema relates to the migrations embedded in this binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaStatus {
    /// Exactly the embedded migrations are applied.
    Current,
    /// Every embedded migration is applied, plus newer ones this binary
    /// doesn't know about. Usually a rolling upgrade in progress; safe as long
    /// as the newer migrations are backwards compatible.
    Ahead { unknown_versions: Vec<i64> },
}

/// Why the database schema is incompatible with this binary.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error(
        "database schema is not initialized (no _sqlx_migrations table); \
         run `lumenqraph-indexer migrate` before starting this service"
    )]
    Uninitialized,
    #[error(
        "database schema is behind this binary: migrations {missing:?} are not applied; \
         run `lumenqraph-indexer migrate` before starting this service"
    )]
    Behind { missing: Vec<i64> },
    #[error(
        "migration {version} is recorded as failed (dirty); fix the database manually, \
         then re-run `lumenqraph-indexer migrate`"
    )]
    Dirty { version: i64 },
    #[error(
        "migration {version} was applied with different contents than the one embedded \
         in this binary (checksum mismatch)"
    )]
    ChecksumMismatch { version: i64 },
    #[error("failed to read applied migrations: {0}")]
    Database(#[from] sqlx::Error),
}

/// Compare the migrations applied to the database with [`MIGRATOR`].
///
/// Read-only: it never creates or alters anything, so it is safe to call from
/// every service on every start.
pub async fn check(pool: &PgPool) -> Result<SchemaStatus, SchemaError> {
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    if !table_exists {
        return Err(SchemaError::Uninitialized);
    }

    let applied: Vec<(i64, bool, Vec<u8>)> = sqlx::query_as(
        "SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await?;

    if let Some((version, _, _)) = applied.iter().find(|(_, success, _)| !success) {
        return Err(SchemaError::Dirty { version: *version });
    }

    let embedded: Vec<_> = MIGRATOR
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .collect();

    let mut missing = Vec::new();
    for migration in &embedded {
        match applied.iter().find(|(v, _, _)| *v == migration.version) {
            None => missing.push(migration.version),
            Some((_, _, checksum)) if checksum.as_slice() != &*migration.checksum => {
                return Err(SchemaError::ChecksumMismatch {
                    version: migration.version,
                });
            }
            Some(_) => {}
        }
    }
    if !missing.is_empty() {
        return Err(SchemaError::Behind { missing });
    }

    let unknown_versions: Vec<i64> = applied
        .iter()
        .map(|(v, _, _)| *v)
        .filter(|v| !embedded.iter().any(|m| m.version == *v))
        .collect();
    if unknown_versions.is_empty() {
        Ok(SchemaStatus::Current)
    } else {
        Ok(SchemaStatus::Ahead { unknown_versions })
    }
}
