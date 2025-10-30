use std::{env, path::PathBuf, process::Stdio, sync::Arc};

use anyhow::{Context, Result, anyhow};
use object_store::{ObjectStore, aws::AmazonS3Builder, buffered::BufWriter};
use tokio::{
    io::{self},
    process::Command,
};

fn build_s3_config_from_env() -> Result<object_store::aws::AmazonS3> {
    let key_id = env::var("S3_KEY_ID").context("S3_KEY_ID not found")?;
    let secret_key = env::var("S3_SECRET_KEY").context("S3_SECRET_KEY not found")?;
    let bucket = env::var("S3_BUCKET").context("S3_BUCKET not found")?;
    let endpoint = env::var("S3_ENDPOINT").context("S3_ENDPOINT not found")?;
    let region = env::var("S3_REGION").context("S3_REGION not found")?;

    AmazonS3Builder::new()
        .with_access_key_id(key_id)
        .with_secret_access_key(secret_key)
        .with_bucket_name(bucket)
        .with_endpoint(endpoint)
        .with_region(region)
        .with_allow_http(true)
        .build()
        .context("Invalid S3 arguments")
}

pub async fn push_btrfs_volume_to_s3(volume: PathBuf, key: &str) -> Result<()> {
    let s3 = Arc::new(build_s3_config_from_env()?);

    let mut send = Command::new("btrfs")
        .arg("send")
        .arg(volume)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdout = send.stdout.take().ok_or(anyhow!("cannot get stdin pipe"))?;

    let object_path = object_store::path::Path::from_url_path(format!("/{key}"))?;

    let mut object_writer = BufWriter::new(s3, object_path);
    io::copy(&mut stdout, &mut object_writer).await?;
    Ok(())
}

pub async fn fetch_btrfs_volume_from_s3(root: PathBuf, key: &str) -> Result<()> {
    let s3 = Arc::new(build_s3_config_from_env()?);
    let mut receive = Command::new("btrfs")
        .arg("send")
        .arg(root)
        .stdin(Stdio::piped())
        .spawn()?;
    let mut stdin = receive
        .stdin
        .take()
        .ok_or(anyhow!("cannot get stdin pipe"))?;

    let object_path = object_store::path::Path::from_url_path(format!("/{key}"))?;

    let object = s3.get(&object_path).await?;
    let mut stream = tokio_util::io::StreamReader::new(object.into_stream());
    io::copy(&mut stream, &mut stdin).await?;
    Ok(())
}
