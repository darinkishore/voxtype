//! `voxtype config set` / `voxtype config unset` — small dispatchers over
//! `config_set`.
//!
//! Every write goes through `voxtype::config_set`, which shares the TUI's
//! `ConfigEditor` (atomic write, post-save validation, rollback). This file
//! only resolves the target path, prints the outcome, and maps errors onto
//! exit codes.

use std::path::PathBuf;
use voxtype::{config, config_set};

/// Resolve the config file path the same way the daemon does — honoring
/// `--config <FILE>` first, then the existing user/system path, then the
/// XDG default. The default path is used even when nothing is on disk so
/// the file gets created in a predictable location on first write.
pub(crate) fn resolve_config_path_for_write(
    cli_override: Option<PathBuf>,
) -> anyhow::Result<PathBuf> {
    if let Some(p) = cli_override {
        return Ok(p);
    }
    if let Some(p) = config::Config::resolve_existing_path() {
        return Ok(p);
    }
    config::Config::default_path().ok_or_else(|| {
        anyhow::anyhow!(
            "Cannot determine config path. Set $XDG_CONFIG_HOME or $HOME, \
             or pass --config <FILE>."
        )
    })
}

/// Print the error on stderr and exit with the code its variant maps to
/// (2 for anything the user can fix in the command, 1 for I/O), matching
/// the contract documented in `voxtype config set --help`.
fn fail(e: config_set::ConfigSetError) -> ! {
    eprintln!("error: {}", e);
    std::process::exit(e.exit_code());
}

fn restart_hint(restart_required: bool) {
    if restart_required {
        println!("Restart voxtype to apply: systemctl --user restart voxtype");
    }
}

/// Dispatcher for `voxtype config set <KEY> <VALUE>`.
pub(crate) fn run_config_set(
    cli_override: Option<PathBuf>,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    let path = resolve_config_path_for_write(cli_override)?;
    match config_set::set_key(path, key, value) {
        Ok(out) => {
            println!(
                "Set {} = {} in {}",
                out.key,
                out.value.display(),
                out.path.display()
            );
            restart_hint(out.restart_required);
            Ok(())
        }
        Err(e) => fail(e),
    }
}

/// Dispatcher for `voxtype config unset <KEY>`.
pub(crate) fn run_config_unset(cli_override: Option<PathBuf>, key: &str) -> anyhow::Result<()> {
    let path = resolve_config_path_for_write(cli_override)?;
    match config_set::unset_key(path, key) {
        Ok(out) => {
            println!("Unset {} in {}", out.key, out.path.display());
            restart_hint(out.restart_required);
            Ok(())
        }
        Err(e) => fail(e),
    }
}
