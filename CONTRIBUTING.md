# Contributing to Number Passport

Thank you for helping. Number Passport produces evidence that people sign off on, so the bar for changes is
correctness first: every number it prints must be reproducible, and every claim it makes must be backed by
a query in the evidence pack.

By taking part you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to help

- **Report a bug** with the output of `passport doctor` and, if you can, a minimal pipeline that reproduces
  it (the sample in `examples/bank` is a good starting point).
- **Add a connector** for a warehouse you use (see [docs/architecture.md](docs/architecture.md#extending)).
- **Widen the PySpark translator** to a DataFrame method that is marked opaque today.
- **Improve the docs**, especially worked examples from real (anonymised) pipelines.

For anything larger than a small fix, please open an issue first so we can agree on the approach.

## Development setup

```bash
git clone https://github.com/sagarkonkanagaon-netizenk/number-passport
cd number-passport
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]" pyspark      # pyspark and Java 17+ are only needed for the Spark test
python examples/bank/build.py
```

## Checks

Run all of these before you open a pull request. CI runs the same ones.

```bash
ruff check .
ruff format --check .
mypy src
pytest
```

The PostgreSQL tests run when `NP_TEST_POSTGRES_URL` (an owner login) and `NP_TEST_POSTGRES_READER_URL` (a
login with no grants of its own) point at a scratch database; see `.github/workflows/ci.yml` for the exact
setup. CI sets `NP_TEST_NO_SKIP=1` in the integration job, so a test that silently skips there is a failure.

## Rules for changes

1. **Every behaviour change has a test**, and numeric tests compare against an independent SQL query, not
   against the engine's own output.
2. **Never weaken the guard** to make a feature work. A new query shape needs a guard test proving that
   row-level and write queries are still rejected.
3. **Fail loudly.** Anything the engine cannot check must surface as a finding and affect the status. A
   silent skip is a bug.
4. **Keep it read-only.** Connectors must enforce read-only at the session level, and pipeline code is
   parsed, never executed.
5. **Stable finding codes.** Codes are part of the public interface; add new ones to the catalogue in
   `docs/how-it-works.md`, and never rename existing ones.
6. **Update `CHANGELOG.md`** under `Unreleased`.

## Commit messages and pull requests

Use [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `test:`,
`refactor:`, `ci:`, `chore:`. Keep pull requests focused on one change, fill in the template, and link the
issue they close.

## License of contributions

By submitting a contribution you agree that it is licensed under the
[Apache License 2.0](LICENSE), as described in section 5 of that license.
