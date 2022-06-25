use std::borrow::Borrow;
use std::fs;
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

    ctx.sql(r#"
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
        l_shipdate <= date '1998-09-02'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
    "#).await.unwrap().show().await.unwrap();

    exec::exec_from_repl(&mut ctx, &mut print_options).await;

    Ok(())
}
