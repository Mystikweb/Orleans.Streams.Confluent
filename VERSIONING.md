# Versioning

This repository uses git-based versioning with MinVer.

## Source of truth

- Git tags are the source of truth for stable versions.
- Tag format is `vX.Y.Z` (for example `v1.2.3`).
- `MinVerTagPrefix` is `v`.

## Package version behavior

- Tagged builds (`vX.Y.Z`) produce stable package versions (`X.Y.Z`).
- Non-tagged builds produce prerelease versions derived by MinVer.
- If a manual workflow run provides a `version` input, that value overrides MinVer for that run.

## Workflows

- GitHub Packages publish workflow:
  - File: `.github/workflows/publish-packages.yml`
  - Triggers:
    - PR merged into `main`
    - Tag push `v*`
    - Manual run (`workflow_dispatch`)
- NuGet.org publish workflow:
  - File: `.github/workflows/publish-nuget.yml`
  - Trigger:
    - Manual run (`workflow_dispatch`)

## Contributor guidance

- For normal development, do not manually edit versions in project files.
- Merge changes into `main` to produce prerelease package versions.
- Create a `vX.Y.Z` tag when you are ready to cut a stable release.

## Notes

- CI uses full git history (`fetch-depth: 0`) for correct MinVer calculation.
- MinVer is configured centrally in `Directory.Build.props`.
