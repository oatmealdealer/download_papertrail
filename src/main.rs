use anyhow::{Context, Result};
use async_compression::tokio::write::GzipDecoder;
use chrono::{Datelike, DurationRound, Local, NaiveDateTime, TimeDelta, Timelike};
use clap::Parser;
use csv_async::{AsyncReaderBuilder, AsyncWriterBuilder};
use dotenv::dotenv;
use lazy_static::lazy_static;
use reqwest::StatusCode;
use reqwest::{header::HeaderMap, Client};
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncSeekExt;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_stream::StreamExt;
lazy_static! {
    static ref DEFAULT_CONCURRENCY: String =
        std::thread::available_parallelism().map_or_else(|_| String::from("4"), |n| n.to_string());
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Event {
    pub id: u128,
    pub generated_at: String,
    pub received_at: String,
    pub source_id: u32,
    pub source_name: String,
    pub source_ip: Ipv4Addr,
    pub facility_name: String,
    pub severity_name: String,
    pub program: String,
    pub message: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Which archive files to download, in the format "YYYY-MM-DD-HH"
    /// Will be ignored if --start and --end are supplied.
    files: Vec<String>,
    /// API key for Papertrail.
    #[arg(id = "api-token", value_name = "API_TOKEN", env = "PAPERTRAIL_API_TOKEN", long, value_parser = api_client_from_token)]
    api_client: Client,
    /// How many files to download at once.
    #[arg(short, long, default_value = &**DEFAULT_CONCURRENCY)]
    concurrency: usize,
    /// Where to download the files.
    #[arg(short, long, default_value = ".")]
    out: PathBuf,
    /// How long in milliseconds to wait in between requests.
    #[arg(short, long, default_value = "200")]
    throttle_duration: u64,
    /// Decode from gzip before writing.
    #[arg(short, long)]
    deflate: bool,
    /// Convert the downloaded files to CSV.
    #[arg(long, requires = "deflate")]
    csv: bool,
    /// Start of datetime window
    #[arg(long, requires = "start")]
    start: Option<NaiveDateTime>,
    /// End of datetime window
    #[arg(long, requires = "end")]
    end: Option<NaiveDateTime>,
}

impl Cli {
    /// If the start and end args are supplied, generate a list of files to download.
    fn file_names(&self) -> Option<Vec<String>> {
        let (mut start, end) = (
            self.start?
                .and_local_timezone(Local)
                .earliest()
                .unwrap_or_else(move || panic!("--start {} falls within a DST gap", &self.start.unwrap()))
                .to_utc()
                .duration_trunc(TimeDelta::hours(1))
                // TODO: Handle this possibility more gracefully as part of general argument validation
                .with_context(move || format!("Invalid start datetime: {}", &self.start.unwrap()))
                .unwrap(),
            self.end?
                .and_local_timezone(Local)
                .earliest()
                .unwrap_or_else(move || panic!("--end {} falls within a DST gap", &self.start.unwrap()))
                .to_utc(),
        );

        let mut names: Vec<String> = vec![];
        while start <= end {
            names.push(format!(
                "{}-{:02}-{:02}-{:02}",
                start.year(),
                start.month(),
                start.day(),
                start.hour()
            ));

            if let Some(new_start) = start.checked_add_signed(TimeDelta::hours(1)) {
                start = new_start;
            } else {
                break;
            }
        }

        Some(names)
    }

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
                let ext: &str = if self.deflate { "tsv" } else { "tsv.gz" };
                // TODO: Use an intermediary temp file here
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.out.join(format!("{}.{}", &time, ext)))
                    .await?;
                let mut out = BufWriter::new(&mut file);
                if self.deflate {
                    let mut decoder = GzipDecoder::new(out);
                    while let Some(item) = byte_stream.next().await {
                        tokio::io::copy(&mut item?.as_ref(), &mut decoder).await?;
                    }
                    decoder.shutdown().await?;
                    out = decoder.into_inner();
                } else {
                    while let Some(item) = byte_stream.next().await {
                        tokio::io::copy(&mut item?.as_ref(), &mut out).await?;
                    }
                }

                out.shutdown().await?;

                if self.csv {
                    file.rewind().await?;
                    convert_to_csv(file, self.out.join(format!("{}.csv", &time)))
                        .await?
                }

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
                self.file_names()
                    .as_ref()
                    .unwrap_or(&self.files)
                    .iter()
                    .map(|time| self.download_file(time.clone())),
            )
            // TODO: smarter throttling
            .throttle(Duration::from_millis(self.throttle_duration)),
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

async fn convert_to_csv(from: File, to: PathBuf) -> Result<()> {
    let reader = AsyncReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'\t')
        .create_deserializer(from);
    let file = tokio::fs::File::create(to).await?;
    let mut writer = AsyncWriterBuilder::new().create_serializer(file);
    let mut records = reader.into_deserialize::<Event>();

    while let Some(record) = records.next().await {
        writer.serialize(record?).await?;
    }

    writer.flush().await?;
    Ok(())
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
