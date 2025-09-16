use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use flate2::{Compression, write::GzEncoder};
use gix::{Commit, ObjectId, Repository, hashtable::hash_map::HashMap, progress::prodash::warn};
use log::{debug, info, trace};
use sha2::{Digest, Sha256};

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

    /// Replace the commit hash with a fixed key
    #[arg(long)]
    fixed_key: Option<String>,
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

    /// Fallback key to use if no cache is found
    /// For example pulling the cache of the nightly build
    /// Fallback key will be checked befor the commit on the main branch
    #[arg(long)]
    fallback_key: Option<String>,
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

    let key = if let Some(fixed_key) = &args.fixed_key {
        format_cache_key_str(&args.prefix, fixed_key.clone(), args.suffix.clone())
    } else {
        current_key(&args.prefix, args.suffix.clone())?
    };

    info!("Storing cache with key {}", &key);

    let writer = file_backend.writer(&key)?;
    let encoder = GzEncoder::new(writer, Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for file in &args.files {
        let stat = std::fs::metadata(file)?;
        let hash = hash_from_path(file);
        if stat.is_dir() {
            trace!("Adding directory {} to archive", file);
            archive.append_dir_all(hash, file)?;
        } else {
            trace!("Adding file {} to archive", file);
            archive.append_path_with_name(file, hash)?;
        }
    }

    archive.finish()?;

    info!("Cache stored with key {}", &key);
    Ok(0)
}

struct FileEntry {
    pub path: String,
    pub extracted: bool,
}

fn pull(args: &PullArgs) -> Result<i32> {
    let file_backend = get_backend();

    let possible_keys =
        possible_restore_keys(&args.prefix, args.suffix.clone(), args.fallback_key.clone())?;
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

    let mut file_etries: HashMap<String, FileEntry> = args
        .files
        .iter()
        .map(|f| {
            let hash = hash_from_path(f);
            (
                hash.clone(),
                FileEntry {
                    path: f.clone(),
                    extracted: false,
                },
            )
        })
        .collect();

    let reader = file_backend.reader(&key)?;
    let decoder = flate2::read::GzDecoder::new(reader);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let components = path.components().collect::<Vec<_>>();
        let hash = components.first().unwrap().as_os_str().to_string_lossy();

        if let Some(file_entry) = file_etries.get_mut(&hash.to_string()) {
            let without_hash = components.iter().skip(1).collect::<PathBuf>();
            let mut output_path = PathBuf::from(&file_entry.path);
            output_path.push(&without_hash);

            trace!(
                "Extracting file {} to {}",
                path.to_string_lossy(),
                output_path.to_string_lossy()
            );
            entry.unpack(output_path)?;
            file_entry.extracted = true;
        } else {
            trace!(
                "Skipping file {} (not in requested files)",
                path.to_string_lossy()
            );
        }
    }

    for (_, file_entry) in &file_etries {
        if !file_entry.extracted {
            warn!("File {} was asked but not found in cache", file_entry.path);
        }
    }

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
    let mut head_id = head.id;

    let main_commit = main_commit(&repository)?;

    // If we're in a merge/pull request, the head is a merge commit between main and the feature branch.
    // We want to find the parent that is not main to use as the cache key.
    if in_merge_request_ci() {
        let parents = head.parent_ids().collect::<Vec<_>>();
        if parents.len() > 1 {
            for parent in &parents {
                let parent_id = parent.detach();
                if parent_id != main_commit.id {
                    head_id = parent_id;
                    break;
                }
            }
        }
    }

    Ok(format_cache_key(prefix, head_id, suffix))
}

fn format_cache_key(prefix: &str, commit: ObjectId, suffix: Option<String>) -> String {
    format_cache_key_str(prefix, commit.to_string(), suffix)
}

fn format_cache_key_str(prefix: &str, key: String, suffix: Option<String>) -> String {
    if let Some(suffix) = suffix {
        format!("{}-{}-{}", prefix, key, suffix)
    } else {
        format!("{}-{}", prefix, key)
    }
}

fn possible_restore_keys(
    prefix: &str,
    suffix: Option<String>,
    fallback_key: Option<String>,
) -> Result<Vec<String>> {
    let repository = gix::discover(".")?;

    let main_commit = main_commit(&repository)?;

    let head = repository.head_commit()?;
    trace!("Current HEAD is at commit {}", head.id);

    let head_ref = repository.head()?;
    let ref_name = head_ref.referent_name();
    trace!(
        "Current HEAD is at reference {:?}",
        ref_name.map(|s| s.as_partial_name().as_bstr())
    );

    let head_parents = head.parent_ids().map(|p| p.detach()).collect::<Vec<_>>();

    trace!("HEAD parents: {:?}", head_parents);

    // look for cache in the last 10 commits in the current branch.
    // if we are on main we look at the last 10 commits of main.
    let parent_commits = head.ancestors();
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

        if commit == main_commit.id {
            // main commit will be added at the end
            continue;
        }

        if suffix.is_some() {
            keys.push(format_cache_key(prefix, commit, suffix.clone()));
        }
        keys.push(format_cache_key(prefix, commit, None));
    }

    if let Some(fallback_key) = fallback_key {
        if suffix.is_some() {
            keys.push(format_cache_key_str(
                prefix,
                fallback_key.clone(),
                suffix.clone(),
            ));
        }
        keys.push(format_cache_key_str(prefix, fallback_key, None));
    }

    if suffix.is_some() {
        keys.push(format_cache_key(prefix, main_commit.id, suffix));
    }
    keys.push(format_cache_key(prefix, main_commit.id, None));
    Ok(keys)
}

fn in_merge_request_ci() -> bool {
    if let Ok(var) = std::env::var("GITHUB_REF")
        && var.contains("refs/pull/")
    {
        true
    } else {
        false
    }
}

fn main_commit(repository: &'_ Repository) -> Result<Commit<'_>> {
    // TODO: ability to set a different default branch
    let main_ref = repository.try_find_reference("origin/main")?;
    let mut main_ref = if let Some(r) = main_ref {
        r
    } else {
        let master_ref = repository.try_find_reference("origin/master")?;
        if let Some(r) = master_ref {
            r
        } else {
            bail!("Could not find 'origin/main' or 'origin/master' reference");
        }
    };
    let main_commit = main_ref.peel_to_commit()?;
    trace!("Main branch is at commit {}", main_commit.id);

    Ok(main_commit)
}

fn hash_from_path<P>(path: P) -> String
where
    P: AsRef<Path>,
{
    let hash = Sha256::digest(path.as_ref().to_string_lossy().as_bytes());
    base16ct::lower::encode_string(&hash)
}
