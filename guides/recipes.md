# Recipes

Short, runnable patterns. The examples use `Bulkinup.upsert/4`/`Bulkinup.insert/4` directly;
with `use Bulkinup` in your repo, the same options apply to `bulk_upsert/3`/`bulk_insert/3`.

## Timestamps

Ecto's `insert_all/3` does not autogenerate fields such as timestamps, so schemas with
`timestamps()` fields need those values supplied explicitly — Bulkinup never invents them for
you. The simplest way is the `:placeholders` option, which sets fields from shared values
(sent to the database once):

```elixir
now = DateTime.utc_now()

Bulkinup.upsert(
  YourProject.Repo,
  YourProject.Persons.Person,
  attrs_list,
  placeholders: %{
    YourProject.Persons.Person => %{inserted_at: now, updated_at: now}
  }
)
```

Each placeholder value is injected into the attrs before validation, so a placeholder field is
cast and validated like any other field (and may be marked as required in your changeset). The
shared value replaces any per-row value supplied for the field.

To preserve the original `:inserted_at` when an upsert updates an existing row, add
`:replace_all_except`:

```elixir
Bulkinup.upsert(
  YourProject.Repo,
  YourProject.Persons.Person,
  attrs_list,
  placeholders: %{
    YourProject.Persons.Person => %{inserted_at: now, updated_at: now}
  },
  # On conflict, replace every field except the primary key and :inserted_at
  replace_all_except: [:inserted_at]
)
```

If you need per-call logic that placeholders cannot express, pass a custom function that
accepts the same arguments as `insert_all/3` via the `:insert_all_function` (or
`:insert_all_module`) option — see the options in `Bulkinup.upsert/4`.

## Inserting alongside shared `many_to_many` children

`Bulkinup.insert/4` applies no conflict defaults anywhere, so a `many_to_many` child (or join
row) that already exists in the database raises. When shared children are expected — tags
referenced by many posts, for example — override the conflict behavior for just those sources:

```elixir
Bulkinup.insert(
  YourProject.Repo,
  YourProject.Blog.Post,
  attrs_list,
  insert_all_opts: %{
    YourProject.Blog.Tag => [on_conflict: :nothing],
    "posts_tags" => [on_conflict: :nothing]
  }
)
```

The parent posts are still pure inserts: a duplicate post raises.

## App-wide defaults with `use Bulkinup`

Declare shared defaults once in your repo module instead of at every call site — never in the
application environment:

```elixir
defmodule YourProject.Repo do
  use Ecto.Repo,
    otp_app: :your_project,
    adapter: Ecto.Adapters.Postgres

  use Bulkinup,
    timeout: 60_000,
    upsert: [replace_all_except: [:inserted_at]]
end
```

Flat keys apply to both verbs (where valid); the `insert:`/`upsert:` namespaces hold per-verb
defaults; per-call opts always win. Values are evaluated per call at runtime, so dynamic
defaults like `timeout: fetch_timeout!()` work. See `Bulkinup.__using__/1`.

## Recovering dirty data

When importing messy data, `:recover_changeset_errors` replaces invalid field values with
per-schema fallbacks instead of skipping the whole row. Here, a person with a missing
(required) name is written with the fallback name instead of being skipped:

```elixir
Bulkinup.upsert(
  YourProject.Repo,
  YourProject.Persons.Person,
  [%{id: 1}, %{id: 2, name: "Bob"}],
  recover_changeset_errors: %{YourProject.Persons.Person => %{name: "UNKNOWN"}}
)
```

Fallbacks apply recursively to nested association and embedded changesets, and a row is only
recovered if every error in it has a fallback.

## Streaming large imports

`attrs_list` accepts any `Enumerable`, so a large input can be streamed instead of loaded into
memory:

```elixir
"people.csv"
|> File.stream!()
|> Stream.map(&parse_csv_row/1)
|> then(&Bulkinup.insert(YourProject.Repo, YourProject.Persons.Person, &1))
```

Rows are validated and written in chunks of `:chunk_size` as the stream is consumed, so memory
stays bounded no matter how large the input is. Note that the single transaction stays open
for the stream's full duration. To trade the single-transaction guarantee for throughput
(chunks written concurrently, each in its own transaction), see the `:max_concurrency` option
in `Bulkinup.upsert/4` — and read its caveats first.

## Per-schema conflict handling

By default, an upsert replaces all of a conflicting row's fields except the primary key. Use
`:insert_all_opts` to override the conflict behavior for specific schemas (or join-table
sources):

```elixir
Bulkinup.upsert(
  YourProject.Repo,
  YourProject.Persons.Person,
  attrs_list,
  insert_all_opts: %{
    # Never update existing persons; only insert new ones
    YourProject.Persons.Person => [on_conflict: :nothing],
    # Only update a pet's name on conflict
    YourProject.Persons.Pet => [on_conflict: {:replace, [:name]}]
  }
)
```
