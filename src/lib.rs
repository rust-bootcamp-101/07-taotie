mod backend;
mod cli;

use std::{ops::Deref, process, thread};

use cli::{ConnectOpts, DescribeOpts, HeadOpts, ListOpts, SchemaOpts, SqlOpts};
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;

use backend::DataFusionBackend;
pub use cli::ReplCommand;
use reedline_repl_rs::CallBackMap;
use tokio::runtime::Runtime;

#[enum_dispatch]
trait CmdExecutor {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String>;
}

trait Backend {
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()>;
    async fn list(&self) -> anyhow::Result<impl ReplDisplay>;
    async fn schema(&self, name: &str) -> anyhow::Result<impl ReplDisplay>;
    async fn describe(&self, name: &str) -> anyhow::Result<impl ReplDisplay>;
    async fn head(&self, name: &str, size: usize) -> anyhow::Result<impl ReplDisplay>;
    async fn sql(&self, sql: &str) -> anyhow::Result<impl ReplDisplay>;
}

trait ReplDisplay {
    async fn display(self) -> anyhow::Result<String>;
}

pub struct ReplContext {
    // 使用 channel 使得 UI 和后端解耦，即使后端换了，UI端的代码也不需要修改
    pub tx: mpsc::Sender<ReplMsg>,
}

pub struct ReplMsg {
    cmd: ReplCommand,
    tx: oneshot::Sender<String>,
}

pub type ReplCallbacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;

pub fn get_callbacks() -> ReplCallbacks {
    let mut callbacks = ReplCallbacks::new();
    callbacks.insert("connect".to_string(), cli::connect);
    callbacks.insert("list".to_string(), cli::list);
    callbacks.insert("describe".to_string(), cli::describe);
    callbacks.insert("schema".to_string(), cli::schema);
    callbacks.insert("head".to_string(), cli::head);
    callbacks.insert("sql".to_string(), cli::sql);
    callbacks
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();
        let rt = Runtime::new().expect("Failed to create runtime");
        let mut backend = DataFusionBackend::new();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    if let Err(err) = rt.block_on(async {
                        let ret = msg.cmd.execute(&mut backend).await?;
                        msg.tx.send(ret)?;
                        Ok::<_, anyhow::Error>(())
                    }) {
                        eprintln!("Failed to process command: {}", err);
                    }
                }
            })
            .unwrap();

        Self { tx }
    }

    pub fn send(&self, msg: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        if let Err(err) = self.tx.send(msg) {
            eprintln!("Repl Send Error: {}", err);
            process::exit(1);
        }

        // if the oneshot receiver is dropped, return None, because never had an error on the command
        rx.recv().ok()
    }
}

impl Deref for ReplContext {
    type Target = mpsc::Sender<ReplMsg>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd: cmd.into(),
                tx,
            },
            rx,
        )
    }
}
