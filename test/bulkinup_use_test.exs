defmodule BulkinupUseTest do
  use BulkinupDemo.DataCase, async: true

  alias BulkinupDemo.Blog.Author
  alias BulkinupDemo.MacroRepo

  # MacroRepo's `use Bulkinup` defaults, for reference in the tests below:
  #
  #     insert_all_function_module: BulkinupDemo.ProxyRepo,
  #     timeout: default_timeout(),               # dynamic; returns 12_345 at runtime
  #     replace_all_except: [:phone_number],      # flat upsert-only key
  #     chunk_size: 3,
  #     insert: [chunk_size: 2],
  #     upsert: [replace_all_except: [:inserted_at]]

  test "bulk_upsert applies flat defaults, including dynamic values evaluated at runtime" do
    {:ok, %{upserted: 1, skipped: 0}} = MacroRepo.bulk_upsert(Author, [%{id: 1, name: "Alice"}])

    assert_received {:proxy_insert_all, Author, _entries, opts}
    assert opts[:timeout] == 12_345
    assert Repo.get!(Author, 1).name == "Alice"
  end

  test "the verb namespace overrides a flat key: upsert's replace_all_except" do
    {:ok, _} = MacroRepo.bulk_upsert(Author, [%{id: 1, name: "Alice"}])

    # From `upsert: [replace_all_except: [:inserted_at]]`, not the flat `[:phone_number]`
    assert_received {:proxy_insert_all, Author, _entries, opts}
    assert opts[:on_conflict] == {:replace_all_except, [:id, :inserted_at]}
  end

  test "the verb namespace overrides a flat key: insert's chunk_size" do
    attrs_list = Enum.map(1..5, fn id -> %{id: id, name: "author-#{id}"} end)

    {:ok, %{inserted: 5, skipped: 0}} = MacroRepo.bulk_insert(Author, attrs_list)

    # 5 authors in chunks of 2 (`insert: [chunk_size: 2]`, not the flat 3) -> 3 queries
    assert_received {:proxy_insert_all, Author, [_, _], _opts}
    assert_received {:proxy_insert_all, Author, [_, _], _opts}
    assert_received {:proxy_insert_all, Author, [_], _opts}
  end

  test "the flat chunk_size applies to upsert, which does not override it" do
    attrs_list = Enum.map(1..5, fn id -> %{id: id, name: "author-#{id}"} end)

    {:ok, %{upserted: 5, skipped: 0}} = MacroRepo.bulk_upsert(Author, attrs_list)

    # 5 authors in chunks of 3 (the flat default) -> 2 queries
    assert_received {:proxy_insert_all, Author, [_, _, _], _opts}
    assert_received {:proxy_insert_all, Author, [_, _], _opts}
  end

  test "per-call opts override the verb namespace" do
    attrs_list = Enum.map(1..5, fn id -> %{id: id, name: "author-#{id}"} end)

    {:ok, _} = MacroRepo.bulk_insert(Author, attrs_list, chunk_size: 10)

    assert_received {:proxy_insert_all, Author, entries, _opts}
    assert length(entries) == 5
    refute_received {:proxy_insert_all, _, _, _}
  end

  test "the flat upsert-only replace_all_except is not applied to bulk_insert" do
    {:ok, %{inserted: 1, skipped: 0}} = MacroRepo.bulk_insert(Author, [%{id: 1, name: "Alice"}])

    assert_received {:proxy_insert_all, Author, _entries, opts}
    refute Keyword.has_key?(opts, :on_conflict)
    refute Keyword.has_key?(opts, :replace_all_except)
  end

  test "passing replace_all_except per-call to bulk_insert still raises" do
    assert_raise ArgumentError, ~r/only apply to an upsert/, fn ->
      MacroRepo.bulk_insert(Author, [%{id: 1, name: "Alice"}], replace_all_except: [:name])
    end
  end

  test "an unknown flat key is a compile error" do
    assert_raise ArgumentError, ~r/unknown option\(s\) \[:chunck_size\]/, fn ->
      Code.compile_string("""
      defmodule BulkinupUseTest.BadFlatKey do
        use Bulkinup, chunck_size: 100
      end
      """)
    end
  end

  test "an upsert-only key in the insert namespace is a compile error" do
    assert_raise ArgumentError, ~r/invalid option\(s\) \[:replace_all_except\]/, fn ->
      Code.compile_string("""
      defmodule BulkinupUseTest.BadInsertNamespace do
        use Bulkinup, insert: [replace_all_except: [:name]]
      end
      """)
    end
  end

  test "a non-keyword verb namespace is a compile error" do
    assert_raise ArgumentError, ~r/expects a literal keyword list/, fn ->
      Code.compile_string("""
      defmodule BulkinupUseTest.BadNamespaceShape do
        use Bulkinup, upsert: %{replace_all_except: [:name]}
      end
      """)
    end
  end
end
