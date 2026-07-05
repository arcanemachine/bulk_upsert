# Nested Associations

The main reason to reach for Bulkinup over a plain `insert_all/3` is that it writes a parent
and its children at the same time, from a single list of attrs. The parent and each
association are written into their own tables, all within one transaction (by default).

## A `has_many` example

Extending the `Person` example from [Getting Started](getting_started.html) with a
`has_many :pets` association:

`priv/repo/migrations/0002_create_pets.exs`

```elixir
defmodule YourProject.Repo.Migrations.CreatePets do
  use Ecto.Migration

  def change do
    create table(:pets) do
      add :person_id, references(:persons)
      add :name, :string
    end
  end
end
```

`lib/your_project/persons/pet.ex`

```elixir
defmodule YourProject.Persons.Pet do
  use Ecto.Schema
  import Ecto.Changeset

  schema "pets" do
    field :person_id, :integer
    field :name, :string
  end

  def changeset(pet \\ %__MODULE__{}, attrs) do
    pet
    |> cast(attrs, [:id, :person_id, :name])
    |> validate_required([:id, :person_id, :name])
  end
end
```

`lib/your_project/persons/person.ex`

```elixir
defmodule YourProject.Persons.Person do
  use Ecto.Schema
  import Ecto.Changeset

  schema "persons" do
    field :name, :string

    has_many :pets, YourProject.Persons.Pet
  end

  def changeset(person \\ %__MODULE__{}, attrs) do
    person
    |> cast(attrs, [:id, :name])
    |> validate_required([:id, :name])
    |> cast_assoc(:pets)
  end
end
```

> #### Foreign keys are not inferred {: .warning}
>
> Each child's foreign key (here, `person_id`) must be present in its own attrs. Associations
> are written via `insert_all/3`, so the foreign key is not inferred from the parent.

Now a single call writes both the persons and their pets across both tables:

```text
iex> Bulkinup.upsert(
...>   YourProject.Repo,
...>   YourProject.Persons.Person,
...>   [
...>     %{id: 1, name: "Alice", pets: [
...>       %{id: 10, person_id: 1, name: "Rex"},
...>       %{id: 11, person_id: 1, name: "Whiskers"}
...>     ]},
...>     %{id: 2, name: "Bob", pets: [
...>       %{id: 20, person_id: 2, name: "Buddy"}
...>     ]}
...>   ]
...> )
{:ok, %{upserted: 2, skipped: 0}}
```

Running the same call again with changed pet names upserts the existing rows in place, exactly
like the top-level structs. Both verbs are write-only operations: children absent from the
attrs are never deleted or nilified, at any level (unlike `cast_assoc/3`'s `:on_replace`
behavior during regular `Repo.insert/update` calls).

## The other association kinds

`has_one` and `many_to_many` associations work the same way: cast them in the changeset and
include them in the attrs.

- `has_many` / `has_one`: each child must carry its own foreign key (as shown above with
  `person_id`).
- `many_to_many`: the associated records and the join table rows are both written for you, and
  duplicate records and links are removed automatically. With `insert/4`, a pre-existing
  shared child or join row raises by default — see the join-table recipe in
  [Recipes](recipes.html).
- `embeds_one` / `embeds_many`: embedded schemas have no table of their own, so they are
  stored inline on the parent row.

Nesting works recursively at any depth — a child's own associations (e.g. the pets' vet
appointments) are written the same way.

## Known limitations

Nested `belongs_to` associations are not written. To associate with a `belongs_to` parent,
include its foreign key field in the attrs (e.g. `category_id`). This applies at every level
of nesting.
