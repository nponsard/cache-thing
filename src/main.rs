use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use flate2::{Compression, write::GzEncoder};

use crate::storage_backend::StorageBackend;

mod folder_backend;
pub mod storage_backend;

#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = "git")]
#[command(about = "A fictional versioning CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Push(PushArgs),
    Pull(PullArgs),
}

#[derive(Debug, Args)]
struct PushArgs {
    /// Files to push to cache storage
    #[arg(short, long)]
    files: Vec<String>,

    /// Name of the cache, to differentiate if multiple are stored in the same backend
    #[arg(short, long)]
    prefix: String,
}

#[derive(Debug, Args)]
struct PullArgs {
    #[arg(short, long)]
    files: Vec<String>,

    /// Name of the cache, to differentiate if multiple are stored in the same backend
    #[arg(short, long)]
    prefix: String,
}

fn main() {
    let exit_code = match try_main() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err}");
            1
        }
    };
    std::process::exit(exit_code);
}

fn try_main() -> Result<i32> {
    let args = Cli::parse();

    match &args.command {
        Commands::Push(push_args) => push(push_args),
        Commands::Pull(pull_args) => pull(pull_args),
    }
}

fn push(args: &PushArgs) -> Result<i32> {
    let file_backend = get_backend();

    let key = args.prefix.clone();

    let writer = file_backend.writer(&key)?;
    let encoder = GzEncoder::new(writer, Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for file in &args.files {
        let stat = std::fs::metadata(file)?;
        if stat.is_dir() {
            archive.append_dir_all(file, file)?;
        } else {
            archive.append_path_with_name(file,file)?;
        }
    }

    archive.finish()?;
    Ok(0)
}
fn pull(args: &PullArgs) -> Result<i32> {
    let file_backend = get_backend();

    let key = args.prefix.clone();

    let reader = file_backend.reader(&key)?;
    let decoder = flate2::read::GzDecoder::new(reader);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(".")?;
    Ok(0)
}

fn get_backend() -> impl StorageBackend {
    folder_backend::FolderBackend::new(std::path::PathBuf::from("/tmp/cache-thing/data"))
}
