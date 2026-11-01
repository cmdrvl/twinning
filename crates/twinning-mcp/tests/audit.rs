use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

#[test]
fn u14_dependency_graph_and_tcp_calls_stay_synchronous_and_loopback_only() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let rustc_output = Command::new("rustc")
        .arg("-vV")
        .output()
        .expect("rustc version starts");
    if !rustc_output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&rustc_output.stderr));
    }
    assert!(rustc_output.status.success(), "rustc -vV failed");
    let host_target = String::from_utf8_lossy(&rustc_output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .expect("rustc reports its host target")
        .to_owned();
    let metadata_output = Command::new("cargo")
        .args([
            "metadata",
            "--offline",
            "--format-version",
            "1",
            "--filter-platform",
            &host_target,
        ])
        .current_dir(&manifest_dir)
        .output()
        .expect("cargo metadata starts");
    if !metadata_output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&metadata_output.stderr));
    }
    assert!(
        metadata_output.status.success(),
        "cargo metadata failed with {:?}",
        metadata_output.status.code()
    );
    let metadata: Value =
        serde_json::from_slice(&metadata_output.stdout).expect("cargo metadata emits JSON");
    let package_names = metadata["packages"]
        .as_array()
        .expect("metadata packages")
        .iter()
        .map(|package| {
            (
                package["id"].as_str().expect("package id").to_owned(),
                package["name"].as_str().expect("package name").to_owned(),
            )
        })
        .collect::<HashMap<_, _>>();
    let nodes = metadata["resolve"]["nodes"]
        .as_array()
        .expect("resolved dependency nodes")
        .iter()
        .map(|node| {
            let id = node["id"].as_str().expect("node id").to_owned();
            let dependencies = node["deps"]
                .as_array()
                .expect("node dependency list")
                .iter()
                .map(|dependency| {
                    dependency["pkg"]
                        .as_str()
                        .expect("dependency package id")
                        .to_owned()
                })
                .collect::<Vec<_>>();
            (id, dependencies)
        })
        .collect::<HashMap<_, _>>();
    let mut pending = package_names
        .iter()
        .filter_map(|(id, name)| {
            matches!(name.as_str(), "twinning-mcp" | "twinning-mcp-core").then_some(id.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(2, pending.len(), "both MCP packages are workspace members");

    let forbidden = ["tokio", "reqwest", "hyper", "ureq", "curl", "isahc"]
        .into_iter()
        .collect::<HashSet<_>>();
    let mut visited = HashSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id.clone()) {
            continue;
        }
        let name = package_names.get(&id).expect("dependency package exists");
        assert!(
            !forbidden.contains(name.as_str()),
            "forbidden dependency: {name}"
        );
        pending.extend(
            nodes
                .get(&id)
                .expect("dependency node exists")
                .iter()
                .cloned(),
        );
    }

    let mut source_files = Vec::new();
    collect_rust_files(&manifest_dir.join("src"), &mut source_files);
    collect_rust_files(&manifest_dir.join("tests"), &mut source_files);
    assert!(!source_files.is_empty(), "MCP Rust source is present");
    for path in source_files {
        if path.ends_with("tests/audit.rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read MCP Rust source");
        for line in source.lines() {
            if line.contains("TcpListener::bind") {
                assert!(
                    line.contains("127.0.0.1"),
                    "non-loopback TcpListener::bind in {}: {line}",
                    path.display()
                );
            }
            if line.contains("TcpStream::connect") && !line.contains("127.0.0.1") {
                let loopback_address_is_fixed = path.ends_with("supervisor.rs")
                    && line.contains("connect_timeout(&address")
                    && source.contains("SocketAddr::from(([127, 0, 0, 1], port))");
                assert!(
                    loopback_address_is_fixed,
                    "non-loopback TcpStream connect in {}: {line}",
                    path.display()
                );
            }
        }
    }
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}
