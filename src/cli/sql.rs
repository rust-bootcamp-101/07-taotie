use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(help = "The SQL query")]
    pub query: String,
}

pub fn sql(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let opts: SqlOpts = args.try_into()?;
    let (msg, rx) = ReplMsg::new(opts);
    Ok(ctx.send(msg, rx))
}

impl CmdExecutor for SqlOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.sql(&self.query).await?;
        df.display().await
    }
}

impl TryFrom<ArgMatches> for SqlOpts {
    type Error = reedline_repl_rs::Error;

    fn try_from(args: ArgMatches) -> Result<Self, Self::Error> {
        let query = args
            .get_one::<String>("query")
            .expect("expect query")
            .to_string();
        Ok(SqlOpts { query })
    }
}
