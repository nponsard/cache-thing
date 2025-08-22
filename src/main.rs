use anyhow::Result;
use clap::{Args, Parser, Subcommand};

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
    unimplemented!()
}
fn pull(args: &PullArgs) -> Result<i32> {
    unimplemented!()
}
