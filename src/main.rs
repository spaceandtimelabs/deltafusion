use std::sync::Arc;
use datafusion::prelude::{SessionContext};
use datafusion_cli::context::Context;
use datafusion_cli::exec;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::PrintOptions;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, world!");

    let mut print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: false,
    };

    let ctx = SessionContext::new();
    let table = deltalake::open_table("/home/bgardner/workspace/ignite-arrow-store/data")
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table))?;
    let mut ctx = Context::Local(ctx);

    exec::exec_from_repl(&mut ctx, &mut print_options).await;

    Ok(())
}
