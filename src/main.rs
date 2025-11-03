use std::{
    fs::{self, File, create_dir_all},
    os::unix,
    path::{Path, PathBuf},
    process,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use gix::{Commit, ObjectId, Repository, hashtable::hash_map::HashMap, prelude::ObjectIdExt};
use log::{debug, info, trace, warn};
use sha2::{Digest, Sha256};

use crate::{
    btrfs::{create_subvolume, delete_subvolume, set_subvolume_readonly, snapshot_subvolume},
    folder_backend::hash_file_name,
    sharing::fetch_btrfs_volume_from_s3,
};

mod btrfs;
mod folder_backend;
pub mod storage_backend;

mod sharing;

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
    /// Clean the changes made to the cache.
    Clean(CleanArgs),
    /// Fetch from S3
    Fetch(FetchArgs),
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

    /// Only store the fixed key, not the commit key
    #[arg(long)]
    only_fixed_key: bool,

    /// Also push to configured S3 (using fixed key)
    #[arg(long)]
    also_to_s3_fixed_key: bool,
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

#[derive(Debug, Args)]
struct CleanArgs {
    /// Name of the cache, to differentiate if multiple are stored in the same backend
    #[arg(short, long)]
    prefix: String,

    /// Optional suffix
    #[arg(short, long)]
    suffix: Option<String>,
}

#[derive(Debug, Args)]
struct FetchArgs {
    /// Name of the cache, to differentiate if multiple are stored in the same backend
    #[arg(short, long)]
    prefix: String,

    /// Optional suffix
    #[arg(short, long)]
    suffix: Option<String>,

    /// Fixed key
    #[arg(long)]
    fixed_key: String,
}

#[tokio::main]
async fn main() {
    let exit_code = match try_main().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err}");
            1
        }
    };
    std::process::exit(exit_code);
}

async fn try_main() -> Result<i32> {
    env_logger::init();

    let args = Cli::parse();

    match &args.command {
        Commands::Push(push_args) => push(push_args).await,
        Commands::Pull(pull_args) => pull(pull_args),
        Commands::Clean(clean_args) => clean(clean_args),
        Commands::Fetch(fetch_args) => fetch(fetch_args).await,
    }
}

// fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
//     fs::create_dir_all(&dst)?;
//     for entry in fs::read_dir(src)? {
//         let dir_entry = entry?;
//         let ty = dir_entry.file_type()?;
//         if ty.is_dir() {
//             copy_dir_all(dir_entry.path(), dst.as_ref().join(dir_entry.file_name()))?;
//         } else {
//             fs::copy(dir_entry.path(), dst.as_ref().join(dir_entry.file_name()))?;
//         }
//     }
//     Ok(())
// }
async fn fetch(args: &FetchArgs) -> Result<i32> {
    let fixed_key = format_cache_key_str(&args.prefix, args.fixed_key.clone(), args.suffix.clone());
    let root = PathBuf::from(get_cache_location());

    let fixed_cache = root.join(hash_file_name(&fixed_key));

    if fixed_cache.exists() {
        let command_status = set_subvolume_readonly(&fixed_cache, false)
            .context("Making subvolume not read-only")?;

        if !command_status.success() {
            warn!("Failed to mark subvolume as read-only off");
        }

        let command_status = delete_subvolume(&fixed_cache)?;

        if !command_status.success() {
            warn!("coudn't delete suvolume");
        }
    }

    fetch_btrfs_volume_from_s3(root, &fixed_key).await?;
    Ok(0)
}

fn clean(args: &CleanArgs) -> Result<i32> {
    let cache_dir = get_cache_location();
    create_dir_all(PathBuf::from(&cache_dir))?;

    let commit_key = current_key(&args.prefix, args.suffix.clone())?;

    info!("Cleaning up cache with key {}", commit_key);

    let current_cache = PathBuf::from(&cache_dir).join(hash_file_name(&commit_key));

    if !current_cache.exists() {
        info!("Cache does not exist, nothing to clean");
        return Ok(0);
    }

    let command_status = delete_subvolume(&current_cache)?;
    if !command_status.success() {
        warn!("couldn't delete cache volume");
    }

    info!("Cache cleaned successfully");
    Ok(0)
}

async fn push(args: &PushArgs) -> Result<i32> {
    let cache_dir = get_cache_location();
    create_dir_all(PathBuf::from(&cache_dir))?;

    let commit_key = current_key(&args.prefix, args.suffix.clone())?;
    let fixed_key = args.fixed_key.clone().map(|fixed_key| {
        format_cache_key_str(&args.prefix, fixed_key.clone(), args.suffix.clone())
    });
    info!(
        "Marking cache as finished with key {}, fixed key: {:?}",
        commit_key, &fixed_key
    );

    let current_cache = PathBuf::from(&cache_dir).join(hash_file_name(&commit_key));

    if !current_cache.exists() {
        debug!("Creating volumee: {:?}", current_cache);

        let command_status = create_subvolume(&current_cache)?;
        if !command_status.success() {
            bail!("Could not create btrfs subvolume");
        }

        debug!("Copying files to newly created volume");
        for file in &args.files {
            trace!("Copying {}", file);
            let hash = hash_from_path(file);
            let cache_path = current_cache.join(&hash);

            // if PathBuf::from(file).is_dir() {
            //     copy_dir_all(file, &cache_path)?;
            // } else {
            //     fs::copy(file, &cache_path)?;
            // }

            let command_status = process::Command::new("cp")
                .arg("-r")
                .arg(file)
                .arg(&cache_path)
                .status()
                .context("Copying file using cp")?;
            if !command_status.success() {
                bail!("Could not copy file {} to cache", file);
            }
        }
    }

    let finished_file = current_cache.join("finished");

    debug!("Touching finished file");
    File::create(finished_file).context("Touching finished file")?;

    debug!("Marking subvolume as read-only");
    // Mark read-only
    let command_status =
        set_subvolume_readonly(&current_cache, true).context("Making subvolume read-only")?;

    if !command_status.success() {
        warn!("Failed to mark subvolume as read-only");
    }

    if let Some(ref key) = fixed_key {
        let fixed_cache = PathBuf::from(&cache_dir).join(hash_file_name(key));

        if fixed_cache.exists() {
            let command_status = set_subvolume_readonly(&fixed_cache, false)
                .context("Making subvolume not read-only")?;

            if !command_status.success() {
                warn!("Failed to mark subvolume as read-only off");
            }

            let status =
                delete_subvolume(&fixed_cache).context("Deleting old fixed key subvolume")?;
            if !status.success() {
                bail!("Failed to delete fixed key subvolume")
            }
        }

        let command_status = process::Command::new("btrfs")
            .arg("subvolume")
            .arg("snapshot")
            .arg("-r")
            .arg(&current_cache)
            .arg(fixed_cache.clone())
            .status()?;

        if !command_status.success() {
            bail!("Could not create btrfs snapshot for fixed key");
        }
        if args.only_fixed_key {
            debug!("Deleding current commit subvolume");
            let command_status = set_subvolume_readonly(&current_cache, false)
                .context("Making subvolume not read-only")?;

            if !command_status.success() {
                warn!("Failed to mark subvolume as read-only off");
            }

            let command_status = delete_subvolume(&current_cache)?;
            if !command_status.success() {
                warn!("only-fixed-key: couldn't delete commit key");
            }
        }
        debug!("Pushing to s3: {}", args.also_to_s3_fixed_key);
        if args.also_to_s3_fixed_key {
            sharing::push_btrfs_volume_to_s3(fixed_cache.clone(), key)
                .await
                .context("pushing to s3")?;
        }
    }

    Ok(0)
}

struct FileEntry {
    pub path: String,
}

fn pull(args: &PullArgs) -> Result<i32> {
    let volume_location = get_cache_location();

    let possible_keys: Vec<String> =
        possible_restore_keys(&args.prefix, args.suffix.clone(), args.fallback_key.clone())?;
    let mut key = None;
    for k in possible_keys {
        trace!("Looking for cache with key {}", &k);

        // Checking for "finished" file, marking that the the cache is not being written to.
        let file = PathBuf::from(&volume_location)
            .join(hash_file_name(&k))
            .join("finished");

        if file.exists() {
            debug!("Found cache with key {}", &k);
            key = Some(k);
            break;
        }
    }

    let directory_entries: HashMap<String, FileEntry> = args
        .files
        .iter()
        .map(|f| {
            let hash = hash_from_path(f);
            (hash.clone(), FileEntry { path: f.clone() })
        })
        .collect();

    let previous_cache_volume =
        key.map(|k| PathBuf::from(&volume_location).join(hash_file_name(&k)));

    let current_key = current_key(&args.prefix, args.suffix.clone())?;
    let current_cache_volume = PathBuf::from(&volume_location).join(hash_file_name(&current_key));

    if current_cache_volume.exists() {
        set_subvolume_readonly(&current_cache_volume, false)?;
        delete_subvolume(&current_cache_volume)?;
    }

    match previous_cache_volume {
        Some(ref previous) => {
            let command_status = snapshot_subvolume(previous, &current_cache_volume)?;

            if !command_status.success() {
                bail!("Could not create btrfs snapshot");
            }
            // Mark that we're working on this cache
            fs::remove_file(current_cache_volume.join("finished"))?;
        }
        None => {
            let command_status = create_subvolume(&current_cache_volume)?;
            if !command_status.success() {
                bail!(
                    "Failed to create subvolume {}",
                    current_cache_volume.to_string_lossy()
                );
            }
        }
    }

    // DEPENDENCY ON findmnt
    // Get what device the btrfs volume is mounted from
    // let btrfs_device = String::from_utf8_lossy(
    //     &process::Command::new("findmnt")
    //         .arg("-v")
    //         .arg("-n")
    //         .arg("-o")
    //         .arg("SOURCE")
    //         .arg("--target")
    //         .arg(&volume_location)
    //         .output()?
    //         .stdout,
    // )
    // .to_string();

    for (hash, entry) in directory_entries {
        let cache_entry_path = PathBuf::from(&current_cache_volume).join(&hash);

        if !cache_entry_path.exists() {
            // create_subvolume(&cache_entry_path)?;
            fs::create_dir_all(&cache_entry_path)?;
        }
        let output_path = PathBuf::from(&entry.path);

        // we replace what was there before
        if output_path.exists() {
            // if we are creating the cache, populate it with the existing content
            if previous_cache_volume.is_none() {
                let res = process::Command::new("cp")
                    .arg("-r")
                    // Add a . to copy all files including hidden files but avoids creating a subfolder at the destination
                    .arg(output_path.join("."))
                    .arg(&cache_entry_path)
                    .status()?;
                if !res.success() {
                    warn!("Copying existing files failed");
                }
            }

            // This may get removed, we're assuming it's a directory at other places
            let result = if output_path.is_file() {
                fs::remove_file(&output_path)
            } else {
                fs::remove_dir_all(&output_path)
            };
            if let Err(e) = result {
                warn!(
                    "Could not remove existing file {}: {}",
                    output_path.to_string_lossy(),
                    e
                );
            }
        }

        // get the subvolume id to mount
        // let subvolid = String::from_utf8_lossy(
        //     &process::Command::new("btrfs")
        //         .arg("inspect-internal")
        //         .arg("rootid")
        //         .arg(&cache_entry_path)
        //         .output()?
        //         .stdout,
        // )
        // .to_string();
        // let result = std::fs::hard_link(&cache_entry_path, &output_path);
        let result = unix::fs::symlink(&cache_entry_path, &output_path);
        trace!(
            "Symlink file {} to {}",
            cache_entry_path.to_string_lossy(),
            output_path.to_string_lossy()
        );
        if let Err(e) = result {
            warn!(
                "Could not create symlink from {} to {}: {}",
                cache_entry_path.to_string_lossy(),
                output_path.to_string_lossy(),
                e
            );
        }
    }

    Ok(0)
}

fn get_cache_location() -> String {
    // TODO: storage backend selection

    std::env::var("CACHE_THING_LOCATION").unwrap_or("/tmp/cache-thing/data".to_string())
}

fn get_real_branch_head(repository: &'_ Repository) -> Result<ObjectId> {
    let head = repository.head_commit()?;
    let mut head_id = head.id;

    let main_commit = main_commit(repository)?;

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
    Ok(head_id)
}

fn current_key(prefix: &str, suffix: Option<String>) -> Result<String> {
    let repository = gix::discover(".")?;
    let head_id = get_real_branch_head(&repository)?;
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
    let branch_head = get_real_branch_head(&repository)?;

    // look for cache in the last 20 commits in the current branch.
    let parent_commits = branch_head.attach(&repository).ancestors();
    // let parent_commits = if head.id == main_commit.id {
    //     parent_commits
    // } else {
    //     parent_commits.with_boundary([main_commit.id])
    // };

    let parent_commits_list = parent_commits
        .sorting(gix::revision::walk::Sorting::BreadthFirst)
        .all()?
        .take(20);

    let mut keys = Vec::new();

    // Push current commit, just in case.
    if suffix.is_some() {
        keys.push(format_cache_key(prefix, branch_head, suffix.clone()));
    }
    keys.push(format_cache_key(prefix, branch_head, None));

    // Go through parent commits
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
