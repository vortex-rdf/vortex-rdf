//! Temp-file spill machinery behind the out-of-core builder: the per-build
//! run directory ([`TempRunsGuard`]), where it is placed, and the error a
//! failed spill reports. The runs written into it, term-id quad runs and
//! index-entry runs, are [`runs`](super::runs).

use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::error::{Result, VortexRdfError};

/// Environment variable overriding where spill directories are created: a
/// volume with room for runs that exist because the data outgrew memory (the
/// OS temp dir is often a size-capped, RAM-backed tmpfs).
const SPILL_DIR_ENV: &str = "VORTEX_RDF_SPILL_DIR";

/// The parent-directory precedence behind [`TempRunsGuard::create`]: the env
/// override (empty counts as unset), then `base`, then the OS temp dir.
fn resolve_spill_parent(env_override: Option<std::ffi::OsString>, base: Option<&Path>) -> PathBuf {
    env_override
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| base.map(Path::to_path_buf))
        .unwrap_or_else(std::env::temp_dir)
}

/// A spill run's I/O failure: `what` failed in `dir` (shown as `.`, the
/// working directory, when empty). The error keeps the I/O error's kind, and
/// its message names the directory and the [`SPILL_DIR_ENV`] way out: a
/// directory with more room when the volume or a quota is full, a writable
/// one otherwise.
pub(crate) fn spill_io_error(dir: &Path, what: &str, e: std::io::Error) -> VortexRdfError {
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let dir = dir.display();
    let hint = match e.kind() {
        ErrorKind::StorageFull | ErrorKind::QuotaExceeded => "a directory with more room",
        _ => "a writable directory",
    };
    VortexRdfError::Io(std::io::Error::new(
        e.kind(),
        format!("spill run {what} failed in {dir}: {e}; set {SPILL_DIR_ENV} to {hint}"),
    ))
}

/// The longest file name, in bytes, the common filesystems take.
const MAX_NAME_BYTES: usize = 255;

/// A unique spill directory for one build's runs, deleted when dropped, so
/// the runs go even when the chunk stream is abandoned unfinished.
pub(crate) struct TempRunsGuard {
    dir: PathBuf,
}

impl TempRunsGuard {
    /// Create the run directory under the spill parent, named after the file
    /// the build writes: `.<output>.spill-<uuid>`, by default a hidden sibling
    /// of the output, or `.vortex-rdf.spill-<uuid>` when there is no output
    /// name (a build into memory or into a caller's writer) or it would make
    /// the directory's name longer than a file name may be.
    ///
    /// The parent is, in precedence order: the `VORTEX_RDF_SPILL_DIR`
    /// environment variable, the caller's `base` (a build that writes a file
    /// passes its temp file's directory, beside the file it writes or
    /// replaces, so spills share the output's volume), then
    /// [`std::env::temp_dir`]. Never the working directory: an embedding
    /// server or binding can run with any cwd, even a read-only one.
    pub(crate) fn create(output: Option<&OsStr>, base: Option<&Path>) -> Result<Self> {
        let parent = resolve_spill_parent(std::env::var_os(SPILL_DIR_ENV), base);
        let suffix = format!(".spill-{}", uuid::Uuid::new_v4());
        let output = output
            .filter(|output| 1 + output.len() + suffix.len() <= MAX_NAME_BYTES)
            .unwrap_or(OsStr::new("vortex-rdf"));
        let mut name = OsString::from(".");
        name.push(output);
        name.push(suffix);
        let dir = parent.join(name);
        std::fs::create_dir_all(&dir)
            .map_err(|e| spill_io_error(&parent, "directory creation", e))?;
        #[cfg(test)]
        spill_hooks::note(&dir);
        Ok(Self { dir })
    }

    /// The directory spill files go in.
    pub(crate) fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for TempRunsGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Test hook: the run directory the last [`TempRunsGuard`] this thread
/// created, which the tests read to pin where a build spilled and what the
/// directory is called.
#[cfg(test)]
pub(crate) mod spill_hooks {
    use std::cell::RefCell;
    use std::path::{Path, PathBuf};

    thread_local! {
        static LAST_DIR: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    /// Record the run directory a guard created.
    pub(super) fn note(dir: &Path) {
        LAST_DIR.with(|last| *last.borrow_mut() = Some(dir.to_path_buf()));
    }

    /// The run directory the last guard created on this thread, if any.
    pub(crate) fn last_dir() -> Option<PathBuf> {
        LAST_DIR.with(|last| last.borrow().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The spill parent, by `(env override, base)`: the override wins over
    /// `base`, `base` over the OS temp dir, and an empty override counts as
    /// unset.
    #[test]
    fn spill_parent_resolves_env_then_base_then_os_temp() {
        for (env, base, parent) in [
            (
                Some("/custom/spill"),
                Some("/base"),
                PathBuf::from("/custom/spill"),
            ),
            (None, Some("/base"), PathBuf::from("/base")),
            (Some(""), None, std::env::temp_dir()),
        ] {
            assert_eq!(
                resolve_spill_parent(env.map(std::ffi::OsString::from), base.map(Path::new)),
                parent,
                "env {env:?}, base {base:?}"
            );
        }
    }

    #[test]
    fn temp_runs_guard_honors_base_and_cleans_up() {
        // The env override outranks `base` by design, so a preset override in
        // the test environment would (correctly) redirect this spill; only
        // assert placement when it is absent.
        if std::env::var_os(SPILL_DIR_ENV).is_some() {
            return;
        }
        let base = std::env::temp_dir().join(format!("vortex_spill_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&base).unwrap();
        let guard = TempRunsGuard::create(None, Some(&base)).unwrap();
        let dir = guard.path().to_path_buf();
        assert!(dir.starts_with(&base));
        assert!(dir.is_dir());
        drop(guard);
        assert!(!dir.exists());
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn temp_runs_guard_names_the_directory_after_its_output() {
        // As above: a preset override would (correctly) take the directory
        // away from `base`, so only assert where it is absent.
        if std::env::var_os(SPILL_DIR_ENV).is_some() {
            return;
        }
        let base = std::env::temp_dir().join(format!("vortex_spill_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&base).unwrap();
        for (output, prefix) in [
            (Some(OsStr::new("out.vortex")), ".out.vortex.spill-"),
            (None, ".vortex-rdf.spill-"),
        ] {
            let guard = TempRunsGuard::create(output, Some(&base)).unwrap();
            let dir = guard.path();
            assert_eq!(dir.parent(), Some(base.as_path()), "{output:?}");
            let name = dir.file_name().unwrap().to_str().unwrap();
            let id = name
                .strip_prefix(prefix)
                .unwrap_or_else(|| panic!("{output:?}: {name} does not start with {prefix}"));
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{output:?}: {name}");
        }
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn temp_runs_guard_falls_back_when_the_output_name_is_too_long() {
        // `.<output>.spill-<uuid>` is the output's length plus 44 bytes: a
        // 211-byte output makes a name of 255 bytes, as long as a file name
        // may be, and a longer one (212, and 250: 294 bytes) takes the name a
        // build without an output has.
        for (len, fits) in [(211, true), (212, false), (250, false)] {
            let output = "x".repeat(len);
            let guard = TempRunsGuard::create(Some(OsStr::new(&output)), None).unwrap();
            let dir = guard.path();
            assert!(dir.is_dir(), "{len}");
            let name = dir.file_name().unwrap().to_str().unwrap();
            let prefix = if fits {
                format!(".{output}.spill-")
            } else {
                ".vortex-rdf.spill-".to_owned()
            };
            let id = name
                .strip_prefix(prefix.as_str())
                .unwrap_or_else(|| panic!("{len}: {name} does not start with {prefix}"));
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{len}: {name}");
            if fits {
                assert_eq!(name.len(), MAX_NAME_BYTES, "{len}");
            }
        }
    }

    #[test]
    fn spill_io_error_suggests_room_for_a_full_volume() {
        for kind in [ErrorKind::StorageFull, ErrorKind::QuotaExceeded] {
            let error = spill_io_error(Path::new("/spill/runs"), "write", kind.into());
            assert!(
                matches!(&error, VortexRdfError::Io(e) if e.kind() == kind),
                "{error}"
            );
            let message = error.to_string();
            assert!(
                message.contains("spill run write failed in /spill/runs: "),
                "{message}"
            );
            assert!(
                message.ends_with("; set VORTEX_RDF_SPILL_DIR to a directory with more room"),
                "{message}"
            );
        }
    }
}
