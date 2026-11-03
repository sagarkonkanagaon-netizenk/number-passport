# Releasing

Releases are fully automated. Pushing a tag `vX.Y.Z`, or running the **Release** workflow by hand from
`main`, runs `.github/workflows/release.yml`, which:

1. checks that the tag matches `version` in `pyproject.toml` (for a manual run: that the run is on `main`
   and that version has not been released yet);
2. builds the wheel and sdist, runs `twine check --strict` and smoke-tests the wheel;
3. publishes to PyPI with **trusted publishing** (OpenID Connect; no API token is stored anywhere);
4. creates the GitHub release with the built files and the notes from `CHANGELOG.md` (and, for a manual
   run, the `vX.Y.Z` tag on the released commit).

## One-time setup (maintainer)

1. Create a free account at [pypi.org](https://pypi.org/account/register/) and turn on two-factor
   authentication.
2. Go to **Your account → Publishing → Add a new pending publisher** and enter:

   | Field | Value |
   |-------|-------|
   | PyPI project name | `number-passport` |
   | Owner | `sagarkonkanagaon-netizenk` |
   | Repository name | `number-passport` |
   | Workflow name | `release.yml` |
   | Environment name | `pypi` |

3. In the GitHub repository, open **Settings → Environments → New environment**, name it `pypi`, and
   optionally add yourself as a required reviewer so every publish waits for your approval.

## Cutting a release

1. Move the `Unreleased` entries in `CHANGELOG.md` under a new version heading with today's date.
2. Set the same version in `pyproject.toml` and `src/number_passport/__init__.py`.
3. Merge that change to `main`, then either tag it:

   ```bash
   git tag v0.1.0 && git push origin v0.1.0
   ```

   or open **Actions → Release → Run workflow** on `main` (works from a phone too).

4. Watch the **Release** workflow. When it is green, `pip install number-passport` installs the new version.
