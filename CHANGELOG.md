# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Split monolithic `index.js` (876 LOC) into focused modules under `src/`
- Deduplicated mock Redis client factory (was copy-pasted twice)
- Replaced hardcoded shared secret in `EdgeProxyService.cls` with Custom Setting lookup
- Cleaned up verbose comments and removed internal task labels from code

### Added

- Test suite using vitest + @cloudflare/vitest-pool-workers
- Tests for firewall patterns, auth, Redis client, Salesforce mock data, telemetry, Slack dashboard
- GitHub Actions CI pipeline (prettier, tests, audit)
- Dependabot for automated dependency updates
- `.dev.vars.example` template for local development
- `CONTRIBUTING.md` with setup and code style guidelines
- Input length validation in firewall (max 2000 chars)
- `src/constants.js` for centralized config values
- `src/telemetry.js` with testable cost estimation

### Fixed

- `.dev.vars` with real secret was committed to git — removed from tracking, added to `.gitignore`

### Security

- Removed hardcoded `PROXY_SHARED_SECRET` from Apex source code
- Added `.dev.vars` to `.gitignore` to prevent future secret leaks
