use std::{
    ffi::OsStr,
    io,
    process::{self, ExitStatus},
};

pub fn set_subvolume_readonly<S: AsRef<OsStr>>(path: S, readonly: bool) -> io::Result<ExitStatus> {
    let readonly_str = if readonly { "true" } else { "false" };

    process::Command::new("btrfs")
        .arg("property")
        .arg("set")
        .arg("-f")
        .arg(&path)
        .arg("ro")
        .arg(readonly_str)
        .status()
}

pub fn delete_subvolume<S: AsRef<OsStr>>(path: S) -> io::Result<ExitStatus> {
    process::Command::new("btrfs")
        .arg("subvolume")
        .arg("delete")
        .arg(&path)
        .status()
}

pub fn create_subvolume<S: AsRef<OsStr>>(path: S) -> io::Result<ExitStatus> {
    process::Command::new("btrfs")
        .arg("subvolume")
        .arg("create")
        .arg(path)
        .status()
}

pub fn snapshot_subvolume<S: AsRef<OsStr>, D: AsRef<OsStr>>(
    source: S,
    destination: D,
) -> io::Result<ExitStatus> {
    process::Command::new("btrfs")
        .arg("subvolume")
        .arg("snapshot")
        .arg(source)
        .arg(destination)
        .status()
}
