mod describe;
mod df_describe;

use std::ops::Deref;

use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion::prelude::{
    CsvReadOptions, DataFrame, NdJsonReadOptions, SessionConfig, SessionContext,
};
use describe::DataFrameDescriber;

use crate::{
    cli::{ConnectOpts, DatasetConn},
    Backend, ReplDisplay,
};

pub struct DataFusionBackend(SessionContext);

impl Backend for DataFusionBackend {
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()> {
        match &opts.conn {
            DatasetConn::Postgres(_) => {
                println!("Postgres connection is not supported yet")
            }
            DatasetConn::Csv(file_opt) => {
                let options = CsvReadOptions::default()
                    .file_extension(&file_opt.ext)
                    .file_compression_type(file_opt.compression);
                self.register_csv(&opts.name, &file_opt.filename, options)
                    .await?;
            }
            DatasetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, filename, Default::default())
                    .await?;
            }
            DatasetConn::NdJson(file_opt) => {
                let options = NdJsonReadOptions::default()
                    .file_extension(&file_opt.ext)
                    .file_compression_type(file_opt.compression);
                self.register_json(&opts.name, &file_opt.filename, options)
                    .await?;
            }
        }

        Ok(())
    }

    async fn list(&self) -> anyhow::Result<impl ReplDisplay> {
        let df = self
            .0
            .sql("SELECT t.table_name, t.table_type FROM information_schema.tables t WHERE t.table_schema = 'public'")
            .await?;
        Ok(df)
    }

    async fn schema(&self, name: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(&format!("DESCRIBE {}", name)).await?;
        Ok(df)
    }

    async fn describe(&self, name: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(&format!("SELECT * FROM {}", name)).await?;
        // let ddf = DescribeDataFrame::new(df);
        // let batch = ddf.to_record_batch().await?;

        let ddf = DataFrameDescriber::try_new(df)?;
        ddf.describe()
    }

    async fn head(&self, name: &str, size: usize) -> anyhow::Result<impl ReplDisplay> {
        let df = self
            .0
            .sql(&format!("SELECT * FROM {} LIMIT {}", name, size))
            .await?;
        Ok(df)
    }

    async fn sql(&self, sql: &str) -> anyhow::Result<impl ReplDisplay> {
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
}

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;
        let ctx = SessionContext::new_with_config(config);
        Self(ctx)
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplDisplay for DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let batches = self.collect().await?;
        let data = pretty_format_batches(&batches)?;
        Ok(data.to_string())
    }
}

impl ReplDisplay for RecordBatch {
    async fn display(self) -> anyhow::Result<String> {
        let data = pretty_format_batches(&[self])?;
        Ok(data.to_string())
    }
}
