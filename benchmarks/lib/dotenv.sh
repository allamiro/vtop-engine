# shellcheck shell=bash
# What compose will read: one answer, for every lab script that has to predict it.
#
# WHY THIS FILE EXISTS (#484, review). Compose auto-loads `benchmarks/.env` for
# a `-f` file that lives in that directory, and it resolves `${VAR:-default}`
# in a fixed order: a value present in the invoking SHELL first, the `.env`
# file second, the literal default last. Two scripts in this lab have to
# predict that resolution before the stack is up, and each of them looked at a
# different half of it — `verify-h3.sh` read the shell alone, so a
# `VTOP_H3_PORT` filed in `.env` moved the proxy while the check went on
# probing 9443 and reported the lab broken; `gen-h3-certs.sh` read the file
# alone, so an exported `VTOP_BENCH_UID` outranked the line it had just
# validated and the proxy started as an identity that cannot read its own key.
# Both are the same defect: a second notion of how a value is read. There is
# one notion here, and `lib/engine.py`'s `_compose_value` is its Python
# counterpart, held to the same order by the tests in `benchmarks/tests`.
#
# WHAT IT DELIBERATELY DOES NOT DO. The values these scripts resolve are the
# lab's own: unquoted, whitespace-free uids, gids and port numbers. Quoted
# values, escapes and inline comments after a quote are out of scope — that
# parsing lives in `lib/engine.py`, which is what hands credentials to the
# engine, and a second half-implementation of Compose's dotenv grammar is
# precisely what this file exists to prevent.
#
# Sourced, never executed:  source "$BENCH_DIR/lib/dotenv.sh"

dotenv_value() { # dotenv_value <env-file> <key> -> the value the FILE assigns, or empty
  local file="$1" key="$2" raw
  [[ -f "$file" ]] || return 0
  # The LAST assignment wins, which is what compose does with a duplicated
  # key, and everything from the first whitespace on is dropped the way a
  # trailing comment or a stray CR would be — so a value reported from here is
  # the one compose would use and not a quoting artefact.
  raw="$(sed -n "s/^$key=//p" "$file" | tail -n 1)"
  printf '%s' "${raw%%[[:space:]]*}"
}

compose_value_origin() { # compose_value_origin <env-file> <key> -> shell | file | unset
  # WHERE the value comes from, which is not the same question as what it is:
  # a refusal has to send the operator to the shell or to the file, and those
  # are fixed in different ways — an exported value cannot be corrected by
  # editing `.env`, because compose never gets that far.
  local file="$1" key="$2"
  # PRESENT, not non-empty. An exported-but-blank variable still masks the
  # file for compose, which then falls back to its `${VAR:-default}` default
  # rather than to the filed line; calling that "file" would send an operator
  # to correct a line compose does not read.
  if [[ -n "${!key+present}" ]]; then
    printf 'shell'
  elif [[ -f "$file" ]] && grep -q "^$key=" "$file"; then
    printf 'file'
  else
    printf 'unset'
  fi
}

compose_raw_value() { # compose_raw_value <env-file> <key> -> the winning value, unfolded
  # Unfolded: an empty answer here means "set to nothing", which compose
  # resolves to the default — the caller decides whether that distinction
  # matters. `compose_value` is the caller that does not care.
  local file="$1" key="$2"
  if [[ -n "${!key+present}" ]]; then
    printf '%s' "${!key}"
  else
    dotenv_value "$file" "$key"
  fi
}

compose_value() { # compose_value <env-file> <key> <default> -> what ${key:-default} resolves to
  local raw
  raw="$(compose_raw_value "$1" "$2")"
  printf '%s' "${raw:-$3}"
}
