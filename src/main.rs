use anyhow::{Context, Result};
use clap::Parser;
use dotenv::dotenv;
use lazy_static::lazy_static;
use reqwest::StatusCode;
use reqwest::{header::HeaderMap, Client};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_stream::StreamExt;
lazy_static! {
    static ref DEFAULT_CONCURRENCY: String =
        std::thread::available_parallelism().map_or_else(|_| String::from("4"), |n| n.to_string());
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Which archive files to download, in the format "YYYY-MM-DD-HH"
    files: Vec<String>,
    /// API key for Papertrail.
    #[arg(id = "api_token", env = "PAPERTRAIL_API_TOKEN", long, value_parser = api_client_from_token)]
    api_client: Client,
    /// How many files to download at once.
    #[arg(short, long, default_value = &**DEFAULT_CONCURRENCY)]
    concurrency: usize,
    /// Where to download the files.
    #[arg(short, long, default_value = ".")]
    out: PathBuf,
    /// How long in milliseconds to wait in between requests.
    #[arg(short, long, default_value = "200")]
    throttle_duration: usize,
}

impl Cli {
    async fn download_file(&self, time: String) -> Result<String> {
        let response = self
            .api_client
            .get(format!(
                "https://papertrailapp.com/api/v1/archives/{}/download",
                time
            ))
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {
                let mut byte_stream = response.bytes_stream();

                let mut file =
                    tokio::fs::File::create(self.out.join(format!("{}.tsv.gz", &time))).await?;
                let mut out = BufWriter::new(&mut file);
                while let Some(item) = byte_stream.next().await {
                    tokio::io::copy(&mut item?.as_ref(), &mut out).await?;
                }
                out.flush().await?;
                Ok(time.to_string())
            }
            code => Err(CliError::BadResponse(time.to_string(), code).into()),
        }
    }

    async fn run(&mut self) -> Result<()> {
        if !self.out.try_exists()? {
            return Err(CliError::MissingDirectory(
                self.out
                    .to_str()
                    .expect("output path is not valid UTF-8")
                    .to_string(),
            )
            .into());
        }
        futures::StreamExt::buffer_unordered(
            tokio_stream::iter(
                self.files
                    .iter()
                    .map(|time| self.download_file(time.clone())),
            )
            // TODO: smarter throttling
            .throttle(Duration::from_millis(200)),
            self.concurrency,
        )
        .map(|result| match result {
            Ok(file) => println!("Downloaded {}", file),
            Err(e) => eprintln!("Error: {:?}", e),
        })
        .collect::<Vec<_>>()
        .await;
        Ok(())
    }
}

#[derive(Error, Debug)]
enum CliError {
    #[error("Couldn't find directory: {0}")]
    MissingDirectory(String),
    #[error("Failed to download {0}: {1}")]
    BadResponse(String, StatusCode),
}

fn api_client_from_token(token: &str) -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Papertrail-Token",
        reqwest::header::HeaderValue::from_str(token).context("Invalid API token")?,
    );
    Client::builder()
        .default_headers(headers)
        .build()
        .context("Couldn't build client")
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    Cli::parse().run().await?;
    Ok(())
}
