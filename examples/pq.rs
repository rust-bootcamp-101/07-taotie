use std::fs::File;

use anyhow::Result;
use arrow::array::AsArray;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use polars::{prelude::*, sql::SQLContext};

const PQ_FILE: &str = "assets/sample.parquet";

#[tokio::main]
async fn main() -> Result<()> {
    // 使用不同工具读取 parquet 文件
    read_with_parquet(PQ_FILE)?;
    read_with_datafusion(PQ_FILE).await?;
    read_with_polars(PQ_FILE)?;
    Ok(())
}

// 从parquet底层处理
fn read_with_parquet(file: &str) -> Result<()> {
    let file = File::open(file)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;

    for record_batch in reader {
        let record_batch = record_batch?;

        let emails = record_batch.column(0).as_string_opt::<i32>();
        if let Some(email) = emails {
            println!("{:?}", email);
        }
    }
    Ok(())
}

async fn read_with_datafusion(file: &str) -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("stats", file, Default::default())
        .await?;
    ctx.sql("SELECT email, name FROM stats LIMIT 3")
        .await?
        .show()
        .await?;
    Ok(())
}

fn read_with_polars(file: &str) -> Result<()> {
    let file = File::open(file)?;
    let df = ParquetReader::new(file).finish()?;
    println!("{:?}", df);
    let mut ctx = SQLContext::new();
    ctx.register("stats", df.lazy());
    let df = ctx
        .execute("SELECT email, name FROM stats LIMIT 3")?
        .collect()?;

    println!("{:?}", df);
    Ok(())
}
