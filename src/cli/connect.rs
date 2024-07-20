use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplMsg};

use super::ReplResult;

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Csv(String),
    Parquet(String),
    NdJson(String),
}

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = verify_conn_str, help = "Connection string to the dataset, could be postgres or local file (support: csv, json, parquet)")]
    pub conn: DatasetConn,

    // short 短名称 如-c=xx，long 长名称 如 --name=xx
    #[arg(short, long, help = "If database, the name of the table")]
    pub table: Option<String>,

    #[arg(long, help = "The name of the dataset")]
    pub name: String,
}

pub fn connect(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let opts: ConnectOpts = args.try_into()?;
    let (msg, rx) = ReplMsg::new(opts);
    Ok(ctx.send(msg, rx))
}

impl CmdExecutor for ConnectOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;
        Ok(format!("Connected to dataset: {}", self.name))
    }
}

impl TryFrom<ArgMatches> for ConnectOpts {
    type Error = reedline_repl_rs::Error;

    fn try_from(args: ArgMatches) -> Result<Self, Self::Error> {
        let conn = args
            .get_one::<DatasetConn>("conn")
            .expect("expect conn")
            .to_owned();
        let table = args.get_one::<String>("table").map(|s| s.to_string());
        let name = args
            .get_one::<String>("name")
            .expect("expect name")
            .to_string();
        Ok(ConnectOpts { conn, table, name })
    }
}

fn verify_conn_str(s: &str) -> Result<DatasetConn, String> {
    let conn_str = s.to_string();
    if conn_str.starts_with("postgres://") {
        Ok(DatasetConn::Postgres(conn_str))
    } else if conn_str.ends_with(".csv") {
        Ok(DatasetConn::Csv(conn_str))
    } else if conn_str.ends_with(".ndjson") {
        Ok(DatasetConn::NdJson(conn_str))
    } else if conn_str.ends_with(".parquet") {
        Ok(DatasetConn::Parquet(conn_str))
    } else {
        Err(format!("Invalid connection string: {}", s))
    }
}
