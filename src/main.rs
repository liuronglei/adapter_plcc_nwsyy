use adapter_plcc_nwsyy::runner::run_adapter;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    run_adapter().await
}