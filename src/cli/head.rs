use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(help = "The name of the dataset")]
    pub name: String,

    #[arg(short, long, help = "The number of rows to show")]
    pub n: Option<usize>,
}

pub fn head(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let opts: HeadOpts = args.try_into()?;
    let (msg, rx) = ReplMsg::new(opts);
    Ok(ctx.send(msg, rx))
}

impl CmdExecutor for HeadOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.head(&self.name, self.n.unwrap_or(5)).await?;
        df.display().await
    }
}

impl TryFrom<ArgMatches> for HeadOpts {
    type Error = reedline_repl_rs::Error;

    fn try_from(args: ArgMatches) -> Result<Self, Self::Error> {
        let name = args
            .get_one::<String>("name")
            .expect("expect name")
            .to_string();
        let n = args.get_one::<usize>("n").copied().unwrap_or(5);
        Ok(HeadOpts { name, n: Some(n) })
    }
}
