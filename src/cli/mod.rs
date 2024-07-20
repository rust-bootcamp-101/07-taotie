mod connect;
mod describe;
mod head;
mod list;
mod schema;
mod sql;

use enum_dispatch::enum_dispatch;

pub use connect::*;
pub use describe::*;
pub use head::*;
pub use list::*;
pub use schema::*;
pub use sql::*;

use clap::Parser;

#[derive(Debug, Parser)]
#[enum_dispatch(CmdExecutor)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to table"
    )]
    Connect(ConnectOpts),

    #[command(name = "list", about = "List all registered datasets")]
    List(ListOpts),

    #[command(name = "schema", about = "Describe the schema of a dataset")]
    Schema(SchemaOpts),

    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),

    #[command(name = "head", about = "Show first few rows of a dataset")]
    Head(HeadOpts),

    #[command(name = "sql", about = "Query a dataset using given SQL")]
    Sql(SqlOpts),
}

pub type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;
