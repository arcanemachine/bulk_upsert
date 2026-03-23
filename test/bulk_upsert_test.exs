defmodule BulkUpsertTest do
  use ExUnit.Case, async: false

  defmodule RepoStub do
    def transaction(fun, _opts), do: fun.()
  end

  defmodule InsertAllSpy do
    use Agent

    def start_link(_opts), do: Agent.start_link(fn -> [] end, name: __MODULE__)

    def clear, do: Agent.update(__MODULE__, fn _ -> [] end)

    def insert_all(schema_module, attrs_list, opts) do
      Agent.update(__MODULE__, &[{:insert_all, schema_module, attrs_list, opts} | &1])
      {length(attrs_list), nil}
    end

    def custom_insert_all(schema_module, attrs_list, opts) do
      Agent.update(__MODULE__, &[{:custom_insert_all, schema_module, attrs_list, opts} | &1])
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

  defmodule ParentWithAltChangeset do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:id, :integer, autogenerate: false}
    schema "parents_with_alt_changeset" do
      field :name, :string
    end

    def changeset(attrs) do
      %__MODULE__{}
      |> cast(attrs, [:id, :name])
      |> validate_required([:id, :name])
    end

    def upsert_changeset(attrs) do
      %__MODULE__{}
      |> cast(attrs, [:id])
      |> validate_required([:id])
    end
  end

  defmodule RecoverableParent do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:id, :integer, autogenerate: false}
    schema "recoverable_parents" do
      field :phone_number, :string
    end

    def changeset(attrs) do
      %__MODULE__{}
      |> cast(attrs, [:id, :phone_number])
      |> validate_required([:id, :phone_number])
      |> validate_format(:phone_number, ~r/^\d{3}-\d{4}$/)
    end
  end

  setup do
    start_supervised!({InsertAllSpy, []})
    :ok
  end

  test "chunks parent upserts according to chunk_size" do
    attrs_list =
      Enum.map(1..5, fn id ->
        %{id: id, name: "parent-#{id}"}
      end)

    :ok =
      BulkUpsert.bulk_upsert(RepoStub, Parent, attrs_list,
        chunk_size: 2,
        insert_all_function_module: InsertAllSpy
      )

    parent_chunk_sizes =
      InsertAllSpy.calls()
      |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} -> schema_module == Parent end)
      |> Enum.map(fn {_fun, _schema_module, attrs_list, _opts} -> length(attrs_list) end)

    assert parent_chunk_sizes == [2, 2, 1]
  end

  test "chunks has_many association upserts according to chunk_size" do
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
        insert_all_function_module: InsertAllSpy
      )

    child_chunk_sizes =
      InsertAllSpy.calls()
      |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} -> schema_module == Child end)
      |> Enum.map(fn {_fun, _schema_module, attrs_list, _opts} -> length(attrs_list) end)

    assert child_chunk_sizes == [2, 2, 2]
  end

  test "uses changeset_function_atom when provided" do
    attrs_list = [%{id: 10}]

    :ok =
      BulkUpsert.bulk_upsert(RepoStub, ParentWithAltChangeset, attrs_list,
        changeset_function_atom: :upsert_changeset,
        insert_all_function_module: InsertAllSpy
      )

    parent_calls =
      InsertAllSpy.calls()
      |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} ->
        schema_module == ParentWithAltChangeset
      end)

    assert length(parent_calls) == 1
    assert [{:insert_all, ParentWithAltChangeset, [%{id: 10}], _opts}] = parent_calls
  end

  test "rejects invalid changesets" do
    attrs_list = [
      %{id: 1, name: "valid"},
      %{id: 2}
    ]

    :ok =
      BulkUpsert.bulk_upsert(RepoStub, Parent, attrs_list,
        insert_all_function_module: InsertAllSpy
      )

    assert [{:insert_all, Parent, [%{id: 1, name: "valid"}], _opts}] =
             InsertAllSpy.calls()
             |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} -> schema_module == Parent end)
  end

  test "recovers configured changeset errors before upsert" do
    attrs_list = [%{id: 1, phone_number: "INVALID"}]

    :ok =
      BulkUpsert.bulk_upsert(RepoStub, RecoverableParent, attrs_list,
        insert_all_function_module: InsertAllSpy,
        recover_changeset_errors: %{RecoverableParent => %{phone_number: "555-1234"}}
      )

    assert [{:insert_all, RecoverableParent, [%{id: 1, phone_number: "555-1234"}], _opts}] =
             InsertAllSpy.calls()
             |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} ->
               schema_module == RecoverableParent
             end)
  end

  test "applies insert_all_opts and timeout for parent and child upserts" do
    attrs_list = [
      %{
        id: 1,
        name: "parent-1",
        children: [%{id: 101, parent_id: 1, value: "x"}]
      }
    ]

    :ok =
      BulkUpsert.bulk_upsert(RepoStub, Parent, attrs_list,
        timeout: 45_000,
        replace_all_except: [:name],
        insert_all_function_module: InsertAllSpy,
        insert_all_opts: %{
          Parent => [on_conflict: {:nothing}],
          Child => [on_conflict: {:replace, [:value]}]
        }
      )

    [{:insert_all, Parent, _parent_attrs, parent_opts}] =
      InsertAllSpy.calls()
      |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} -> schema_module == Parent end)

    [{:insert_all, Child, _child_attrs, child_opts}] =
      InsertAllSpy.calls()
      |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} -> schema_module == Child end)

    assert parent_opts[:conflict_target] == [:id]
    assert parent_opts[:on_conflict] == {:nothing}
    assert parent_opts[:timeout] == 45_000

    assert child_opts[:conflict_target] == [:id]
    assert child_opts[:on_conflict] == {:replace, [:value]}
    assert child_opts[:timeout] == 45_000
  end

  test "uses insert_all_function_atom when provided" do
    attrs_list = [%{id: 1, name: "parent-1"}]

    :ok =
      BulkUpsert.bulk_upsert(RepoStub, Parent, attrs_list,
        insert_all_function_module: InsertAllSpy,
        insert_all_function_atom: :custom_insert_all
      )

    assert [{:custom_insert_all, Parent, [%{id: 1, name: "parent-1"}], _opts}] =
             InsertAllSpy.calls()
             |> Enum.filter(fn {_fun, schema_module, _attrs_list, _opts} -> schema_module == Parent end)
  end
end
