use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgRow, PgTypeInfo, PgValueRef};
use sqlx::{
    Column, Decode, Encode, FromRow, Postgres, Row, Type, TypeInfo, ValueRef, encode::IsNull,
};
use uuid::Uuid;

/// Adapter-owned representation of a domain value.
///
/// For enums it supplies the PostgreSQL enum encoding. For records it maps a
/// row through the domain type's Serde representation, keeping SQLx traits out
/// of `sbgh-core`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Db<T>(pub T);

impl<T> std::ops::Deref for Db<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Db<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub(crate) trait IntoDomain {
    type Domain;

    fn into_domain(self) -> Self::Domain;
}

impl<T> IntoDomain for Db<T> {
    type Domain = T;

    fn into_domain(self) -> Self::Domain {
        self.0
    }
}

impl<T> IntoDomain for Option<Db<T>> {
    type Domain = Option<T>;

    fn into_domain(self) -> Self::Domain {
        self.map(|value| value.0)
    }
}

impl<T> IntoDomain for Vec<Db<T>> {
    type Domain = Vec<T>;

    fn into_domain(self) -> Self::Domain {
        self.into_iter()
            .map(|value| value.0)
            .collect()
    }
}

pub trait DbEnum: Serialize + DeserializeOwned {
    const TYPE_NAME: &'static str;

    fn as_db_str(&self) -> Result<String, BoxDynError> {
        match serde_json::to_value(self)? {
            Value::String(value) => Ok(value),
            _ => Err("database enum did not serialize as a string".into()),
        }
    }
}

impl<T: DbEnum> Type<Postgres> for Db<T> {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name(T::TYPE_NAME)
    }
}

impl<T: DbEnum> Encode<'_, Postgres> for Db<T> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let value = self.0.as_db_str()?;
        buf.extend(value.as_bytes());
        Ok(IsNull::No)
    }
}

impl<'r, T: DbEnum> Decode<'r, Postgres> for Db<T> {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Self(serde_json::from_value(Value::String(value.as_str()?.to_owned()))?))
    }
}

impl<'r, T> FromRow<'r, PgRow> for Db<T>
where
    T: DeserializeOwned,
{
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let mut object = Map::with_capacity(row.len());
        for (index, column) in row
            .columns()
            .iter()
            .enumerate()
        {
            if object.contains_key(column.name()) {
                return Err(mapping_error(format!(
                    "duplicate column name {:?} in domain row",
                    column.name(),
                )));
            }
            let raw = row.try_get_raw(index)?;
            let value = if raw.is_null() {
                Value::Null
            } else {
                match column.type_info().name() {
                    "BOOL" => Value::Bool(row.try_get::<bool, _>(index)?),
                    "INT2" => Value::from(row.try_get::<i16, _>(index)?),
                    "INT4" => Value::from(row.try_get::<i32, _>(index)?),
                    "INT8" => Value::from(row.try_get::<i64, _>(index)?),
                    "FLOAT4" => Value::from(row.try_get::<f32, _>(index)?),
                    "FLOAT8" => Value::from(row.try_get::<f64, _>(index)?),
                    "JSON" | "JSONB" => row.try_get::<Value, _>(index)?,
                    "UUID" => Value::String(
                        row.try_get::<Uuid, _>(index)?
                            .to_string(),
                    ),
                    "TIMESTAMPTZ" => {
                        let value = row.try_get::<DateTime<Utc>, _>(index)?;
                        serde_json::to_value(value).map_err(decode_error)?
                    }
                    "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" => {
                        Value::String(row.try_get::<String, _>(index)?)
                    }
                    type_name if is_db_enum_type(type_name) => Value::String(
                        raw.as_str()
                            .map_err(sqlx::Error::Decode)?
                            .to_owned(),
                    ),
                    type_name => {
                        return Err(mapping_error(format!(
                            "unsupported PostgreSQL type {type_name:?} for column {:?}",
                            column.name(),
                        )));
                    }
                }
            };
            object.insert(column.name().to_owned(), value);
        }

        serde_json::from_value(Value::Object(object))
            .map(Self)
            .map_err(decode_error)
    }
}

fn decode_error(error: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(error))
}

fn mapping_error(message: String) -> sqlx::Error {
    decode_error(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

macro_rules! db_enums {
    ($($ty:path => $name:literal),+ $(,)?) => {
        $(impl DbEnum for $ty {
            const TYPE_NAME: &'static str = $name;
        })+

        fn is_db_enum_type(type_name: &str) -> bool {
            matches!(type_name, $($name)|+)
        }
    };
}

db_enums! {
    sbgh_core::models::JobStatus => "job_status",
    sbgh_core::models::WebhookStatus => "github_webhook_status",
    sbgh_core::models::WebhookOutcome => "github_webhook_outcome",
    sbgh_core::models::GithubAccountType => "github_account_type",
    sbgh_core::models::TriggerKind => "trigger_kind",
    sbgh_core::models::UserRole => "user_role",
    sbgh_core::models::SubmissionStepKind => "task_step_kind",
    sbgh_core::models::JobKind => "job_kind",
    sbgh_core::models::GitRefKind => "git_ref_kind",
    sbgh_core::models::JobSource => "job_source",
    sbgh_core::models::JobIntent => "job_intent",
    sbgh_core::models::TaskKind => "task_kind",
    sbgh_core::models::BuildTarget => "build_target",
    sbgh_core::models::JobEventKind => "job_event_kind",
    sbgh_core::models::JobEventStatus => "job_event_status",
    sbgh_proto::WorkerCapability => "worker_capability",
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::Db;
    use crate::test_support::setup_pg_db;

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct IntRecord {
        value: i32,
    }

    #[tokio::test]
    async fn domain_row_rejects_duplicate_column_names() {
        let (_db, pool) = setup_pg_db().await;
        let error = sqlx::query_as::<_, Db<IntRecord>>("SELECT 1::INT4 AS value, 2::INT4 AS value")
            .fetch_one(&pool)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate column name"),
            "unexpected error: {error}",
        );
    }

    #[tokio::test]
    async fn domain_row_rejects_unsupported_column_types() {
        let (_db, pool) = setup_pg_db().await;
        let error = sqlx::query_as::<_, Db<IntRecord>>("SELECT 1::NUMERIC AS value")
            .fetch_one(&pool)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unsupported PostgreSQL type"),
            "unexpected error: {error}",
        );
    }
}
