defmodule BulkUpsertTest do
  use ExUnit.Case

  defmodule RepoStub do
    def transaction(fun, _opts), do: fun.()
  end

  defmodule InsertAllSpy do
    use Agent

    def start_link(_opts), do: Agent.start_link(fn -> [] end, name: __MODULE__)

    def insert_all(schema_module, attrs_list, opts) do
      Agent.update(__MODULE__, &[{schema_module, attrs_list, opts} | &1])
      {length(attrs_list), nil}
    end

    def calls, do: Agent.get(__MODULE__, &Enum.reverse/1)
  end

  defmodule Child do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:id, :integer, autogenerate: false}
    schema "children" do
      field :parent_id, :integer
      field :value, :string
    end

    def changeset(attrs), do: changeset(%__MODULE__{}, attrs)

    def changeset(struct, attrs) do
      struct
      |> cast(attrs, [:id, :parent_id, :value])
      |> validate_required([:id, :parent_id, :value])
    end
  end

  defmodule Parent do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:id, :integer, autogenerate: false}
    schema "parents" do
      field :name, :string
      has_many :children, Child
    end

    def changeset(attrs) do
      %__MODULE__{}
      |> cast(attrs, [:id, :name])
      |> validate_required([:id, :name])
      |> cast_assoc(:children)
    end
  end

  setup do
    start_supervised!({InsertAllSpy, []})
    :ok
  end

  test "chunks has_many association upserts using the configured chunk size" do
    attrs_list = [
      %{
        id: 1,
        name: "parent-1",
        children: [
          %{id: 101, parent_id: 1, value: "a"},
          %{id: 102, parent_id: 1, value: "b"},
          %{id: 103, parent_id: 1, value: "c"}
        ]
      },
      %{
        id: 2,
        name: "parent-2",
        children: [
          %{id: 201, parent_id: 2, value: "d"},
          %{id: 202, parent_id: 2, value: "e"},
          %{id: 203, parent_id: 2, value: "f"}
        ]
      }
    ]

    :ok =
      BulkUpsert.bulk_upsert(RepoStub, Parent, attrs_list,
        chunk_size: 2,
        insert_all_function_module: InsertAllSpy,
        insert_all_function_atom: :insert_all
      )

    parent_calls =
      InsertAllSpy.calls()
      |> Enum.filter(fn {schema_module, _attrs_list, _opts} -> schema_module == Parent end)

    child_calls =
      InsertAllSpy.calls()
      |> Enum.filter(fn {schema_module, _attrs_list, _opts} -> schema_module == Child end)

    assert length(parent_calls) == 1
    assert [{Parent, parent_attrs, _opts}] = parent_calls
    assert length(parent_attrs) == 2

    assert Enum.map(child_calls, fn {_schema_module, attrs_list, _opts} -> length(attrs_list) end) ==
             [2, 2, 2]
  end
end
