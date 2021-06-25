use service::PaymentsEngine;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct ServiceCli {
    #[structopt(parse(from_os_str), name = "from > out")]
    file_path: std::path::PathBuf,
}

#[tokio::main]
async fn main() {
    let ServiceCli { file_path } = ServiceCli::from_args();
    let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();
    let tokio_stdout = tokio::io::stdout();
    let wri = csv_async::AsyncSerializer::from_writer(tokio_stdout);
    let payments_engine = PaymentsEngine::from(wri);
    match  payments_engine.from_csv(file_path).await {
        Ok(r) => {
            if let Err(e) = r.into_inner().flush().await {
                log::error!("{}", e);
                std::process::exit(1);
            };
        },
        Err(e) => {
            log::error!("{}", e);
            std::process::exit(1);
        }
    }
}
