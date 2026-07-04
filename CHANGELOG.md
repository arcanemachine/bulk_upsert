# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Nested associations are now upserted recursively at any depth (previously only the parent's
  direct associations were handled, and a child carrying its own nested associations crashed)

### Changed

- **Breaking:** `bulk_upsert/4` now returns `{:ok, %{upserted: count, skipped: count}}` (counts
  of top-level attrs) instead of `:ok`, making silently skipped invalid rows visible to callers

### Fixed

- Wrap the entire bulk upsert in a single transaction. Previously each chunk of `:chunk_size`
  parents ran in its own transaction, so a failure in a later chunk left earlier chunks committed

## v0.2.0 - 2026-05-31

### Added

- Support for `has_one` associations
- Support for `many_to_many` associations (related records and join table rows)
- Option `:placeholders` to set fields from shared values via Ecto's `insert_all/3` placeholders

### Fixed

- Default `:insert_all_opts` to a map so string-keyed sources (e.g. join tables) work

## v0.1.5 - 2026-03-23

### Fixed

- Apply `chunk_size` option to `has_many` association upserts

## v0.1.4 - 2026-02-23

### Fixed

- Use separate variable for parent `insert_all_opts`

## v0.1.3 - 2026-02-20

### Fixed

- "Has many" assoc upserts now working

### Added

- Option `:chunk_size` (used to specify the maximum number of items to upsert in a single query)

## v0.1.2 - 2025-05-03

### Added

- Add changelog to HexDocs

## v0.1.1 - 2025-04-29

### Added

- Fix issues with docs

## v0.1.0 - 2025-04-28

### Added

- Initial release

