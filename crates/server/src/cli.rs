use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::Config;

#[derive(Parser)]
#[command(
    name = "beyond-objects",
    about = "Object storage with HMAC bucket tokens, conditional writes, and prefix-paginated listing.",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the HTTP server.
    Serve(Box<Config>),
    /// Write `openapi/v1.json` from the compiled route annotations.
    GenerateOpenapi,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(cfg) => crate::serve(*cfg).await,
        Command::GenerateOpenapi => generate_openapi(),
    }
}

fn generate_openapi() -> Result<()> {
    use utoipa::OpenApi as _;
    let doc = crate::routes::ApiDoc::openapi();
    let mut json = serde_json::to_string_pretty(&doc)?;
    json.push('\n');
    std::fs::create_dir_all("openapi")?;
    std::fs::write("openapi/v1.json", json)?;
    println!("wrote openapi/v1.json");
    Ok(())
}
