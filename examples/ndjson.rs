use std::{fs::File, io::BufReader, sync::Arc};

use anyhow::Result;
use arrow::{
    datatypes::{DataType, Field, Schema},
    json::{writer::JsonArray, ReaderBuilder, WriterBuilder},
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct UserStat {
    email: String,
    name: String,
    gender: String,
    #[serde(deserialize_with = "deserialize_string_date")]
    created_at: DateTime<Utc>,
    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_visited_at: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_watched_at: Option<DateTime<Utc>>,
    recent_watched: Option<Vec<i32>>,
    viewed_but_not_started: Option<Vec<i32>>,
    started_but_not_finished: Option<Vec<i32>>,
    finished: Option<Vec<i32>>,
    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_email_notification: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_in_app_notification: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_string_date_opt")]
    last_sms_notification: Option<DateTime<Utc>>,
}

fn main() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("email", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("gender", DataType::Utf8, false),
        Field::new("created_at", DataType::Date64, false),
        Field::new("last_visited_at", DataType::Date64, true),
        Field::new("last_watched_at", DataType::Date64, true),
        Field::new(
            "recent_watched",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int32,
                false,
            ))),
            true,
        ),
        Field::new(
            "viewed_but_not_started",
            DataType::List(Arc::new(Field::new(
                "viewed_but_not_started",
                DataType::Int32,
                false,
            ))),
            true,
        ),
        Field::new(
            "started_but_not_finished",
            DataType::List(Arc::new(Field::new(
                "started_but_not_finished",
                DataType::Int32,
                false,
            ))),
            true,
        ),
        Field::new(
            "finished",
            DataType::List(Arc::new(Field::new("finished", DataType::Int32, false))),
            true,
        ),
        Field::new("last_email_notification", DataType::Date64, true),
        Field::new("last_in_app_notification", DataType::Date64, true),
        Field::new("last_sms_notification", DataType::Date64, true),
    ]);

    let file = File::open("assets/users.ndjson")?;
    let reader = BufReader::new(file);

    // infer_json_schema 顺序是不确定的
    // let (schema, _) = infer_json_schema(&mut reader, Some(10))?;
    let reader = ReaderBuilder::new(Arc::new(schema)).build(reader)?;
    for batch in reader {
        let batch = batch?;
        // 读出来的数据列储存的格式 [xx,xx,xx,..]
        // let email = batch.column(0).as_string::<i32>();
        // let name = batch.column(1).as_string::<i32>();
        // println!("{:?}, {:?}", email, name);

        // 读成行数据，方便使用序列化到结构体
        let data: Vec<u8> = Vec::new();
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(data);
        writer.write_batches(&[&batch])?;
        writer.finish()?;
        let data = writer.into_inner();
        let ret: Vec<UserStat> = serde_json::from_slice(&data)?;
        for us in &ret {
            println!("{:?}", us);
        }
    }
    Ok(())
}

fn deserialize_string_date<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let from: NaiveDateTime = s.parse().map_err(serde::de::Error::custom)?;
    let dt = Utc.from_local_datetime(&from).unwrap();
    Ok(dt.to_utc())
}

fn deserialize_string_date_opt<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    match s {
        Some(s) => {
            let from: NaiveDateTime = s.parse().map_err(serde::de::Error::custom)?;
            let dt = Utc.from_local_datetime(&from).unwrap();
            Ok(Some(dt.to_utc()))
        }
        None => Ok(None),
    }
}
