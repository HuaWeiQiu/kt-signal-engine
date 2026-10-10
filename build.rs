// SPDX-License-Identifier: AGPL-3.0-only

//! 引擎自证面（engine.protocolCapabilities）依赖基线注入：从本仓自己的
//! Cargo.toml / Cargo.lock 解析出编译期钉定的依赖基线，以 `KT_ENGINE_*`
//! env 常量注入（`env!` 编译期内嵌，运行时零网络、零文件读取）。
//!
//! 真相源与解析对象一一对应：
//! - presage rev ← 根 Cargo.toml 依赖声明行（vendor [patch] 改写不回写
//!   Cargo.lock 的 source，声明的 rev 才是上游钉定锚）；
//! - libsignal 版本 ← Cargo.lock `libsignal-protocol` 的 git source 行
//!   （解析后事实，tag + checkout commit 双值）。
//!
//! 任一锚缺失即 panic（fail-closed）：基线自证面宁可编译失败，不静默
//! 报空。vendor/依赖升级触发本脚本重跑（rerun-if-changed）。

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=Cargo.lock");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    let root = Path::new(&manifest_dir);

    let presage_rev = presage_rev(&read(&root.join("Cargo.toml")));
    let (libsignal_version, libsignal_commit) = libsignal_pin(&read(&root.join("Cargo.lock")));

    println!("cargo:rustc-env=KT_ENGINE_PRESAGE_REV={presage_rev}");
    println!("cargo:rustc-env=KT_ENGINE_LIBSIGNAL_VERSION={libsignal_version}");
    println!("cargo:rustc-env=KT_ENGINE_LIBSIGNAL_COMMIT={libsignal_commit}");
}

fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("{} unreadable: {error}", path.display()))
}

/// 根 Cargo.toml 中 `presage = { git = …, rev = "<40 hex>", … }` 的 rev。
fn presage_rev(manifest: &str) -> String {
    let line = manifest
        .lines()
        .find(|line| line.starts_with("presage = "))
        .expect("Cargo.toml declares the presage git dependency");
    let start = line
        .find("rev = \"")
        .expect("presage dependency pins a git rev")
        + "rev = \"".len();
    let rest = &line[start..];
    let end = rest.find('"').expect("rev value is quoted");
    rest[..end].to_owned()
}

/// Cargo.lock 中 `name = "libsignal-protocol"` 块的 git source 行：
/// `git+https://github.com/signalapp/libsignal?tag=v0.99.0#<40 hex>` →
/// `(版本号去 v 前缀, checkout commit)`。tag 缺席（分支/rev 依赖形态）时
/// 版本为空串，commit 仍是钉定事实。
fn libsignal_pin(lockfile: &str) -> (String, String) {
    let mut lines = lockfile.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim() != "[[package]]" {
            continue;
        }
        let mut name = None;
        let mut source = None;
        while let Some(inner) = lines.peek() {
            if inner.trim() == "[[package]]" {
                break;
            }
            let inner = lines.next().expect("peeked");
            if let Some(value) = inner.strip_prefix("name = ") {
                name = Some(unquote(value));
            } else if let Some(value) = inner.strip_prefix("source = ") {
                source = Some(unquote(value));
            }
        }
        if name.as_deref() != Some("libsignal-protocol") {
            continue;
        }
        let source = source.expect("libsignal-protocol resolves from a git source in Cargo.lock");
        let (without_fragment, fragment) = match source.split_once('#') {
            Some((left, right)) => (left, right),
            None => panic!("git source line carries the checkout commit after '#'"),
        };
        let tag = without_fragment
            .split('?')
            .nth(1)
            .and_then(|query| query.split('&').find_map(|pair| pair.strip_prefix("tag=")))
            .map(|tag| tag.strip_prefix('v').unwrap_or(tag).to_owned())
            .unwrap_or_default();
        return (tag, fragment.to_owned());
    }
    panic!("Cargo.lock has no libsignal-protocol package block");
}

fn unquote(value: &str) -> String {
    let value = value.trim();
    value
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .expect("Cargo.lock string values are quoted")
        .to_owned()
}
