# Contributing

## Licensing of what you send

By opening a pull request you agree that your contribution is licensed under
the [MIT License](LICENSE), the same terms as the rest of the repository. That
is the whole agreement — there is no CLA to sign.

The one exception is **EE Software**, which is licensed under
[LICENSE-EE](LICENSE-EE) and requires a separate written agreement before a
contribution can be accepted. EE Software is anything inside a directory named
`ee/` **or** any file carrying the header `VTOP Engine Enterprise Edition`,
wherever that file sits — either marker is enough, and a contribution touching
either one needs the agreement. Neither exists yet; see
[COMMERCIAL.md](COMMERCIAL.md) for why the boundary is written down in
advance.

## How changes get in

Every change — feature, bugfix, docs, workflow — goes:

    branch → pull request → review → squash-merge

Nothing is pushed directly to `main`. The property that buys is one commit on
`main` per pull request, so a bad change comes out with a single `git revert`.

Create the branch from `origin/main` and give it its own upstream:

```bash
git fetch origin main
git checkout -b fix/short-description origin/main
git push -u origin fix/short-description
```

`git checkout -b <name> origin/main` leaves the branch's upstream pointing at
`refs/heads/main`, which is how an unreviewed commit once reached `main` from a
feature branch. `git push -u origin <name>` on the first push repoints it.

## What CI expects

The pull request must be green before it merges. Locally, the checks that fail
most often are cheap to run first:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Shell changes are linted with the same pinned shellcheck CI uses:

```bash
# The same file list CI lints — a shorter one gives a false green.
docker run --rm -v "$PWD:/mnt" -w /mnt koalaman/shellcheck:v0.11.0 \
  -x -P scripts/live-chaos/scenarios \
  scripts/live-chaos/lib.sh scripts/live-chaos/run-all.sh \
  scripts/live-chaos/gen-certs.sh scripts/live-chaos/scenarios/*.sh
```

Chart changes need `helm lint` and a render of both value files in
`helm/vtop/ci/`.

## What a change is expected to carry

This repository holds itself to a particular standard, and it is worth knowing
before you write rather than after review:

- **Comments say why, not what.** The interesting comments here record the
  failure that motivated the code — a scenario that flaked, a review finding, a
  bug the shape prevents. If a reader would ask "why is it done this way?",
  answer it where they will ask.
- **A test that cannot fail is worse than no test**, because the suite reports
  it green. Where a bug is being fixed, show the test failing against the old
  behaviour.
- **Failures must name their own cause.** A message that says only that
  something timed out sends the next reader to the wrong component; say what
  was observed, how many times, and what was expected.
- **Do not weaken an assertion to make a run pass.** If an assertion is wrong,
  fix the assertion and say why in the commit message.

## Commit messages

The subject names the areas touched and what changed, e.g.

    chaos: a metric read that printed the right value and reported failure

The body explains the problem, the evidence, and what was validated. Long is
fine; a commit message here is the durable record of why the code looks like it
does.

Do not add AI-assistant co-author trailers or "generated with" lines.

## Security

Do not open a public issue for a vulnerability. Contact the maintainer
directly, and give the version, the configuration, and what an attacker gains —
the security model this engine holds itself to is written down in
[docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md).
