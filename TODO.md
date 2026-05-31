# TODO

## Allow placeholder fields to be `validate_required`

Placeholder fields (the `:placeholders` option) are set after changeset
validation, so a `{:placeholder, key}` tuple never reaches the changeset. If a
placeholder field is listed in the changeset's `validate_required/2`, the value
is absent during validation, the changeset is invalid, and the row is silently
skipped.

Possible fix: inject the real value into each attrs map before the changeset
(so it is cast and satisfies `validate_required`), then swap it to
`{:placeholder, field}` after validation. Straightforward for the parent;
trickier for nested associations, whose attrs are nested inside the parent
attrs before `cast_assoc`.

## Improve the test setup beyond the `InsertAllSpy`

Tests record calls via a global `InsertAllSpy` Agent. It can verify what we
pass to `insert_all`, but not what the database actually does.

- Biggest gain: real-DB integration tests (Ecto SQL sandbox) to cover
  `on_conflict`/`conflict_target` behavior, placeholder type and substitution,
  transaction rollback, and foreign key constraints. Cost: new dependency plus
  a database in CI.
- Cheap win: a `calls_for(schema_or_source)` test helper to remove the repeated
  `Enum.filter(calls, ...)` boilerplate.
- Alternative: replace the bespoke Agent with Mox against a `Repo` behaviour for
  verified expectations and `async: true`.
