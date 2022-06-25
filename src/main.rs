use std::borrow::Borrow;
use std::fs;
use std::sync::Arc;
use std::time::Instant;
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

    let dir = "/home/bgardner/workspace/delta-populate-tpch/spark-warehouse/";
    let paths = fs::read_dir(dir).unwrap();
    for path in paths {
        let path = path.unwrap();
        let name = path.file_name().to_str().unwrap().to_string();
        let dir = path.path().to_str().unwrap().to_string();
        println!("Name: {}", name);
        let table = deltalake::open_table(dir.as_str())
            .await
            .unwrap();
        ctx.register_table(name.as_str(), Arc::new(table))?;
    }
    let mut ctx = Context::Local(ctx);

    let dir = "queries/";
    let paths = fs::read_dir(dir).unwrap();
    for path in paths {
        let path = path.unwrap();
        let name = path.file_name().to_str().unwrap().to_string();
        let dir = path.path().to_str().unwrap().to_string();
        let contents = fs::read_to_string(dir).unwrap();
        let start = Instant::now();
        ctx.sql(contents.as_str()).await.unwrap().show().await.unwrap();
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        println!( "{} took {:.1} ms", name, elapsed );
    }

    exec::exec_from_repl(&mut ctx, &mut print_options).await;

    Ok(())
}
