use std::process::Command;

// Bake build provenance into the collector binary so a recorded dataset can
// always be traced back to the exact code that produced it, and so an
// operator can tell at a glance which release a running instance is on
// (`collector --version`, or the `collector_starting` log line).
//
// Mirrors the scheme used by the myhft bot's build.rs. If git is unavailable
// or this is not a checkout (e.g. a vendored source tarball), every value
// falls back to "unknown" — a missing git must never fail the build.

fn main() {
    let commit = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let short = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let branch =
        git(&["rev-parse", "--abbrev-ref", "HEAD"]).unwrap_or_else(|| "unknown".to_string());

    // Tracked-only dirty check. `git diff --quiet HEAD` compares working tree
    // + index against HEAD and ignores untracked files, so stray local
    // artefacts (.idea/, scratch scripts) do not flip a reproducible build to
    // "dirty". Exit 0 = clean, 1 = tracked diff, anything else = git error.
    let dirty = match Command::new("git").args(["diff", "--quiet", "HEAD"]).status() {
        Ok(s) if s.success() => "clean",
        Ok(s) if s.code() == Some(1) => "dirty",
        _ => "unknown",
    };

    println!("cargo:rustc-env=COLLECTOR_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=COLLECTOR_GIT_SHORT={short}");
    println!("cargo:rustc-env=COLLECTOR_GIT_BRANCH={branch}");
    println!("cargo:rustc-env=COLLECTOR_GIT_DIRTY={dirty}");

    // Re-run when HEAD or the index moves, otherwise commit/dirty go stale
    // and a binary reports provenance it no longer has. Unlike myhft, this
    // crate is a workspace member, so `.git` is NOT at the crate root — ask
    // git where it actually lives instead of guessing "../.git".
    if let Some(git_dir) = git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        println!("cargo:rerun-if-changed={git_dir}/index");
    }
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}
