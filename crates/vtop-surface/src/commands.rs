//! A clap command tree as snapshot lines.

use clap::{Arg, ArgAction, Command};

/// One line per command and one per argument, walked from clap's own model
/// of the tree (never from help text).
///
/// A command line is its path (`vtopctl meta status`); an argument line is
/// the path, then the spelling an operator types first (`--config`, `-v`, or
/// `<NAME>` for a positional), then what else the argument accepts and does:
///
/// - `short=-c`, `aliases=--a,--b`, `short-aliases=-x`: other spellings;
/// - `flag` or `count` for an argument that takes no value, else
///   `value=<NAME>`, and `multiple` when it can be repeated;
/// - `values=a,b`: the accepted values, aliases included;
/// - `default=...`;
/// - `required`, `global`, `hidden`, and `position=N` for a positional.
///
/// Words are separated by single spaces and none contains one: several value
/// names or defaults are joined with commas, and a space inside a value is
/// written `\x20`. So `scripts/check-surface-deprecations.py` can split a
/// line on spaces.
///
/// Help text is left out, so rewording it is not a surface change. A
/// default is in, so a changed default shows in review; it does not need a
/// deprecation entry, since every value accepted before is accepted still.
/// clap's own `--help`, `--version` and `help` are left out: they come with
/// every command and cannot be renamed here.
pub fn command_lines(root: &Command) -> Vec<String> {
    let mut lines = Vec::new();
    walk(root, root.get_name(), &mut lines);
    lines
}

fn walk(command: &Command, path: &str, lines: &mut Vec<String>) {
    let mut line = path.to_string();
    let aliases: Vec<&str> = command.get_all_aliases().collect();
    if !aliases.is_empty() {
        line.push_str(&format!(" aliases={}", aliases.join(",")));
    }
    if command.is_hide_set() {
        line.push_str(" hidden");
    }
    lines.push(line);

    let mut position = 0;
    for arg in command.get_arguments() {
        if is_builtin(arg) {
            continue;
        }
        let position = arg.is_positional().then(|| {
            position += 1;
            position
        });
        lines.push(format!("{path} {}", describe(arg, position)));
    }
    for sub in command.get_subcommands() {
        walk(sub, &format!("{path} {}", sub.get_name()), lines);
    }
}

fn is_builtin(arg: &Arg) -> bool {
    matches!(
        arg.get_action(),
        ArgAction::Help | ArgAction::HelpShort | ArgAction::HelpLong | ArgAction::Version
    )
}

fn describe(arg: &Arg, position: Option<usize>) -> String {
    let mut words: Vec<String> = Vec::new();
    match (arg.get_long(), arg.get_short()) {
        (Some(long), short) => {
            words.push(format!("--{long}"));
            if let Some(short) = short {
                words.push(format!("short=-{short}"));
            }
        }
        (None, Some(short)) => words.push(format!("-{short}")),
        (None, None) => words.push(format!("<{}>", value_name(arg))),
    }
    if let Some(aliases) = arg.get_all_aliases() {
        let aliases: Vec<String> = aliases.iter().map(|a| format!("--{a}")).collect();
        words.push(format!("aliases={}", aliases.join(",")));
    }
    if let Some(shorts) = arg.get_all_short_aliases() {
        let shorts: Vec<String> = shorts.iter().map(|s| format!("-{s}")).collect();
        words.push(format!("short-aliases={}", shorts.join(",")));
    }
    match arg.get_action() {
        ArgAction::SetTrue | ArgAction::SetFalse => words.push("flag".into()),
        ArgAction::Count => words.push("count".into()),
        action => {
            if !arg.is_positional() {
                words.push(format!("value=<{}>", value_name(arg)));
            }
            if matches!(action, ArgAction::Append) {
                words.push("multiple".into());
            }
            let values: Vec<String> = arg
                .get_possible_values()
                .iter()
                .flat_map(|value| value.get_name_and_aliases().map(word))
                .collect();
            if !values.is_empty() {
                words.push(format!("values={}", values.join(",")));
            }
            let defaults: Vec<String> = arg
                .get_default_values()
                .iter()
                .map(|value| word(&value.to_string_lossy()))
                .collect();
            if !defaults.is_empty() {
                words.push(format!("default={}", defaults.join(",")));
            }
        }
    }
    if arg.is_required_set() {
        words.push("required".into());
    }
    if arg.is_global_set() {
        words.push("global".into());
    }
    if arg.is_hide_set() {
        words.push("hidden".into());
    }
    if let Some(position) = position {
        words.push(format!("position={position}"));
    }
    words.join(" ")
}

/// A value as one word of the line.
fn word(value: &str) -> String {
    value.replace(' ', "\\x20")
}

fn value_name(arg: &Arg) -> String {
    match arg.get_value_names() {
        Some(names) if !names.is_empty() => names
            .iter()
            .map(|name| word(name.as_str()))
            .collect::<Vec<_>>()
            .join(","),
        _ => word(&arg.get_id().as_str().to_uppercase()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Parser, Subcommand, ValueEnum};

    #[derive(Parser)]
    #[command(name = "tool", version)]
    struct Cli {
        #[command(subcommand)]
        command: Sub,
        #[arg(long, global = true)]
        json: bool,
    }

    #[derive(Copy, Clone, ValueEnum)]
    enum Kind {
        File,
        #[value(alias = "spool")]
        Syslog,
    }

    #[derive(Subcommand)]
    enum Sub {
        /// Help text is not in the snapshot.
        Run {
            #[arg(long, short = 'c')]
            config: std::path::PathBuf,
            #[arg(long, value_enum, default_value_t = Kind::File)]
            kind: Kind,
            #[arg(long, default_value_t = 500)]
            limit: u32,
            #[arg(short, action = ArgAction::Count)]
            verbose: u8,
            #[arg(long, hide = true, alias = "older")]
            old: Option<String>,
            target: String,
        },
        #[command(hide = true)]
        Secret,
    }

    #[test]
    fn every_command_and_argument_is_a_line() {
        use clap::CommandFactory;
        let lines = command_lines(&Cli::command());
        assert_eq!(
            lines,
            vec![
                "tool",
                "tool --json flag global",
                "tool run",
                "tool run --config short=-c value=<CONFIG> required",
                "tool run --kind value=<KIND> values=file,syslog,spool default=file",
                "tool run --limit value=<LIMIT> default=500",
                "tool run -v count",
                "tool run --old aliases=--older value=<OLD> hidden",
                "tool run <TARGET> required position=1",
                "tool secret hidden",
            ]
        );
    }
}
