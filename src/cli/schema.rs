use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct SchemaOpts {
    #[arg(help = "The name of the dataset")]
    pub name: String,
}

pub fn schema(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let opts: SchemaOpts = args.try_into()?;
    let (msg, rx) = ReplMsg::new(opts);
    Ok(ctx.send(msg, rx))
}

impl CmdExecutor for SchemaOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.schema(&self.name).await?;
        df.display().await
    }
}

impl TryFrom<ArgMatches> for SchemaOpts {
    type Error = reedline_repl_rs::Error;

    fn try_from(args: ArgMatches) -> Result<Self, Self::Error> {
        let name = args
            .get_one::<String>("name")
            .expect("expect name")
            .to_string();
        Ok(SchemaOpts { name })
    }
}
