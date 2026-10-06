//! `voxtype config get [<KEY>] [--json]`.
//!
//! Reads only. `value` is the resolved configuration — defaults applied,
//! environment overrides applied — because that is what the daemon would
//! actually use. `file_value` is the literal entry in the config file, so a
//! caller can tell "explicitly set to the default" from "not set".

use std::path::PathBuf;

use voxtype::config::schema;
use voxtype::config::Config;
use voxtype::tui::ConfigEditor;

use super::config_set::resolve_config_path_for_write;

/// Print a JSON scalar the way a shell caller wants it: bare strings without
/// quotes, `null` for absent optionals.
fn plain(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

pub(crate) fn run_config_get(
    cli_override: Option<PathBuf>,
    config: &Config,
    key: Option<String>,
    json: bool,
) -> anyhow::Result<()> {
    let Some(key) = key else {
        return run_get_all(config, json);
    };

    let Some(found) = schema::find_key(&key) else {
        eprintln!(
            "error: unknown config key '{}'.\n  \
             Run `voxtype config schema` to list every settable key.",
            key
        );
        std::process::exit(2);
    };

    let value = schema::resolve_found(&found, config);

    if !json {
        println!("{}", plain(&value));
        return Ok(());
    }

    // The literal file value needs the file, which the resolved config has
    // already folded defaults into. Failing to open it is not fatal — report
    // the resolved value with a null file_value rather than erroring out.
    let file_value = match resolve_config_path_for_write(cli_override)
        .ok()
        .and_then(|p| ConfigEditor::load_from_path(p).ok())
    {
        Some(ed) => schema::file_value_found(&found, &ed),
        None => serde_json::Value::Null,
    };

    let doc = serde_json::json!({
        "key": found.dotted_key(),
        "value": value,
        "file_value": file_value,
    });
    println!("{}", serde_json::to_string_pretty(&doc)?);
    Ok(())
}

fn run_get_all(config: &Config, json: bool) -> anyhow::Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&schema::get_all_json(config))?
        );
        return Ok(());
    }

    let width = schema::scalar_keys()
        .map(|s| s.key.len())
        .max()
        .unwrap_or(0);
    for section in schema::SECTIONS {
        let keys: Vec<_> = schema::scalar_keys()
            .filter(|s| s.section == *section)
            .collect();
        if keys.is_empty() {
            continue;
        }
        println!("{}", section);
        for spec in keys {
            let value = schema::resolve(spec.key, config).unwrap_or(serde_json::Value::Null);
            println!("  {:<width$}  {}", spec.key, plain(&value), width = width);
        }
        println!();
    }

    if !config.text.replacements.is_empty() {
        println!("Replacements");
        let mut entries: Vec<_> = config.text.replacements.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (from, to) in entries {
            println!("  {} -> {}", from, to);
        }
    }
    Ok(())
}
