# Contributing

Use a short-lived branch named `feature/<name>`, `fix/<name>`, or
`chore/<name>`. Commits use Conventional Commits, such as
`fix: reject negative stack pops`.

Every behavior change needs a regression test. Pull requests must describe the
invariant being protected, security or operational impact, and the commands
used for verification. Run `make check` before requesting review.

Keep changes scoped. Do not commit binaries, credentials, coverage output, or
local editor files. Update an ADR when changing an architectural contract and
update the runbook when changing deployment or incident behavior.
