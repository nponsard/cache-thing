use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use flate2::{Compression, write::GzEncoder};
use gix::ObjectId;
use log::{debug, info, trace};

use crate::storage_backend::StorageBackend;

mod folder_backend;
pub mod storage_backend;

#[derive(Debug, Parser)]
#[command(name = "cache-thing")]
#[command(about = "Git-based caching tool", long_about = None)]
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

    /// Optional suffix to append to the cache key
    #[arg(short, long)]
    suffix: Option<String>,
}

#[derive(Debug, Args)]
struct PullArgs {
    #[arg(short, long)]
    files: Vec<String>,

    /// Name of the cache, to differentiate if multiple are stored in the same backend
    #[arg(short, long)]
    prefix: String,

    /// Optional suffix
    #[arg(short, long)]
    suffix: Option<String>,
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
    env_logger::init();

    let args = Cli::parse();

    match &args.command {
        Commands::Push(push_args) => push(push_args),
        Commands::Pull(pull_args) => pull(pull_args),
    }
}

fn push(args: &PushArgs) -> Result<i32> {
    let file_backend = get_backend();

    let key = current_key(&args.prefix, args.suffix.clone())?;

    info!("Storing cache with key {}", &key);

    let writer = file_backend.writer(&key)?;
    let encoder = GzEncoder::new(writer, Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for file in &args.files {
        let stat = std::fs::metadata(file)?;
        if stat.is_dir() {
            trace!("Adding directory {} to archive", file);
            archive.append_dir_all(file, file)?;
        } else {
            trace!("Adding file {} to archive", file);
            archive.append_path_with_name(file, file)?;
        }
    }

    archive.finish()?;

    info!("Cache stored with key {}", &key);
    Ok(0)
}
fn pull(args: &PullArgs) -> Result<i32> {
    let file_backend = get_backend();

    let possible_keys = possible_restore_keys(&args.prefix, args.suffix.clone())?;
    let mut key = None;
    for k in possible_keys {
        trace!("Looking for cache with key {}", &k);
        if file_backend.exists(&k)? {
            debug!("Found cache with key {}", &k);
            key = Some(k);
            break;
        }
    }

    let key = if let Some(k) = key {
        k
    } else {
        bail!("No cache found for prefix {}", &args.prefix);
    };

    let reader = file_backend.reader(&key)?;
    let decoder = flate2::read::GzDecoder::new(reader);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(".")?;
    Ok(0)
}

fn get_backend() -> impl StorageBackend {
    // TODO: storage backend selection

    let location =
        std::env::var("CACHE_THING_LOCATION").unwrap_or("/tmp/cache-thing/data".to_string());

    folder_backend::FolderBackend::new(std::path::PathBuf::from(location))
}

fn current_key(prefix: &str, suffix: Option<String>) -> Result<String> {
    let repository = gix::discover(".")?;
    let head = repository.head_commit()?;

    Ok(format_key(prefix, head.id, suffix))
}

fn format_key(prefix: &str, commit: ObjectId, suffix: Option<String>) -> String {
    if let Some(suffix) = suffix {
        format!("{}-{}-{}", prefix, commit, suffix)
    } else {
        format!("{}-{}", prefix, commit)
    }
}

fn possible_restore_keys(prefix: &str, suffix: Option<String>) -> Result<Vec<String>> {
    let repository = gix::discover(".")?;

    // TODO: ability to set a different default branch
    let main_ref = repository.try_find_reference("main")?;
    let mut main_ref = if let Some(r) = main_ref {
        r
    } else {
        let master_ref = repository.try_find_reference("master")?;
        if let Some(r) = master_ref {
            r
        } else {
            bail!("Could not find 'main' or 'master' reference");
        }
    };
    let main_commit = main_ref.peel_to_commit()?;
    trace!("Main branch is at commit {}", main_commit.id);
    let head = repository.head_commit()?;
    trace!("Current HEAD is at commit {}", head.id);

    // look for cache in the last 10 commits in the current branch.
    // if we are on main we look at the last 10 commits of main.
    let parent_commits = head.ancestors().first_parent_only();
    let parrent_commits = if head.id == main_commit.id {
        parent_commits
    } else {
        parent_commits.with_boundary([main_commit.id])
    };

    let parent_commits_list = parrent_commits.all()?.take(10);

    let mut keys = Vec::new();
    for element in parent_commits_list {
        let commit = element?.id;
        trace!("Considering commit {:?}", commit);
        if suffix.is_some() {
            keys.push(format_key(prefix, commit, suffix.clone()));
        }
        keys.push(format_key(prefix, commit, None));
    }

    if suffix.is_some() {
        keys.push(format_key(prefix, main_commit.id, suffix));
    }
    keys.push(format_key(prefix, main_commit.id, None));
    Ok(keys)
}
