# Contributing

Thanks for contributing to Orleans.Streams.Confluent.

## Before you open an issue

1. Read the usage guidance in README.md.
2. Check release and versioning behavior in VERSIONING.md.
3. Search existing issues for duplicates.

## Issue intake model

This repository uses GitHub issue forms to keep reports actionable.

- Bug report form: requires versions, environment, repro steps, config snippet, and logs.
- Feature request form: requires problem statement, proposal, alternatives, and impact.
- Usage question form: requires package, versions, scenario, and focused question.
- Blank issues are disabled.

## Automatic issue triage

Issue triage automation applies area labels based on issue form selections.

Current labels:

- area:runtime
- area:aspire-runtime
- area:aspire-hosting
- area:tooling
- area:docs
- triage:needs-repro

Bug issues may receive triage:needs-repro when the report does not appear to include enough reproduction detail.

## Stale issue and PR policy

A stale management workflow runs daily and can also be run manually.

Defaults:

- Mark stale after 21 days of inactivity.
- Close after 14 additional inactive days.
- Remove stale label automatically when activity resumes.

Exempt labels:

- Issues: pinned, security, help wanted
- Pull requests: pinned, security

## Pull request expectations

1. Keep changes scoped and focused.
2. Add or update tests for behavior changes.
3. Update documentation when public behavior changes.
4. Ensure CI passes.

## Releases and package publishing

Versioning is git-based using MinVer.

- Merge to main: prerelease package versions.
- Tag vX.Y.Z: stable package versions.
- Manual publish workflows can override version input when needed.

Publishing workflows:

- GitHub Packages: automatic on merged PR to main, tag push, or manual run.
- NuGet.org: manual run.

## Security reporting

Do not open public issues for security vulnerabilities.

Open a private security advisory through the repository Security tab.
