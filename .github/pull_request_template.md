## What this changes

<!-- One or two sentences. Link the issue: Closes #123 -->

## Why

## How it was tested

- [ ] `ruff check .`, `ruff format --check .` and `mypy src` pass
- [ ] `pytest` passes (and the PostgreSQL tests, if a connector or SQL generation changed)
- [ ] New behaviour has a test; numeric tests compare against independent SQL

## Checklist

- [ ] The privacy guard is not weakened (or new guard tests prove it still rejects row-level and write queries)
- [ ] Anything the engine cannot check surfaces as a finding (no silent skips)
- [ ] Finding codes are unchanged, or new ones are added to `docs/how-it-works.md`
- [ ] `CHANGELOG.md` updated under Unreleased
- [ ] Docs updated if behaviour or configuration changed
