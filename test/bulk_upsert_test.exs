defmodule BulkUpsertTest do
  use BulkUpsertDemo.DataCase, async: true

  alias BulkUpsertDemo.Blog.{Address, Author, Category, Comment, Post, Profile, SocialLink, Tag}
  alias BulkUpsertDemo.ProxyRepo

  test "upserts rows, updating them on conflict" do
    {:ok, %{upserted: 2, skipped: 0}} =
      BulkUpsert.bulk_upsert(Repo, Author, [
        %{id: 1, name: "Alice"},
        %{id: 2, name: "Bob"}
      ])

    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, [
        %{id: 1, name: "Alicia"},
        %{id: 2, name: "Bobby"}
      ])

    assert Repo.all(from a in Author, order_by: a.id, select: a.name) == ["Alicia", "Bobby"]
  end

  test "chunks parent upserts according to chunk_size" do
    attach_insert_counter("authors")

    attrs_list = Enum.map(1..5, fn id -> %{id: id, name: "author-#{id}"} end)

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list, chunk_size: 2)

    # 5 authors in chunks of 2 -> 3 INSERT queries
    assert count_insert_queries("authors") == 3
    assert Repo.aggregate(Author, :count) == 5
  end

  test "chunks has_many association upserts according to chunk_size" do
    attach_insert_counter("posts")

    attrs_list = [
      %{
        id: 1,
        name: "Alice",
        posts: [
          %{id: 101, author_id: 1, title: "a"},
          %{id: 102, author_id: 1, title: "b"},
          %{id: 103, author_id: 1, title: "c"}
        ]
      },
      %{
        id: 2,
        name: "Bob",
        posts: [
          %{id: 201, author_id: 2, title: "d"},
          %{id: 202, author_id: 2, title: "e"},
          %{id: 203, author_id: 2, title: "f"}
        ]
      }
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list, chunk_size: 2)

    # 6 posts in chunks of 2 -> 3 INSERT queries
    assert count_insert_queries("posts") == 3
    assert Repo.aggregate(Post, :count) == 6
  end

  test "upserts has_one associations into their own table" do
    attrs_list = [
      %{id: 1, name: "Alice", profile: %{id: 101, author_id: 1, bio: "a"}},
      %{id: 2, name: "Bob", profile: %{id: 102, author_id: 2, bio: "b"}}
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list)

    assert Repo.all(from p in Profile, order_by: p.id, select: {p.author_id, p.bio}) ==
             [{1, "a"}, {2, "b"}]
  end

  test "skips parents whose has_one association is absent" do
    attrs_list = [
      %{id: 1, name: "Alice", profile: %{id: 101, author_id: 1, bio: "a"}},
      %{id: 2, name: "Bob"}
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list)

    # Only the author that supplied a profile results in a profile row
    assert Repo.all(from p in Profile, select: p.id) == [101]
    assert Repo.aggregate(Author, :count) == 2
  end

  test "stores embedded data inline on the parent row instead of a separate table" do
    attrs_list = [
      %{
        id: 1,
        name: "Alice",
        address: %{street: "1 Main St", city: "Springfield"},
        social_links: [
          %{label: "website", url: "https://example.com"},
          %{label: "mastodon", url: "https://social.example.com/@alice"}
        ]
      }
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list)

    author = Repo.get!(Author, 1)
    assert author.address == %Address{street: "1 Main St", city: "Springfield"}

    assert author.social_links == [
             %SocialLink{label: "website", url: "https://example.com"},
             %SocialLink{label: "mastodon", url: "https://social.example.com/@alice"}
           ]
  end

  test "upserts many_to_many related records and join rows, deduplicated" do
    Repo.insert!(%Author{id: 1, name: "Alice"})

    # Both posts share tag 10, which must be upserted (and linked) without duplication
    attrs_list = [
      %{
        id: 1,
        author_id: 1,
        title: "P1",
        tags: [%{id: 10, name: "elixir"}, %{id: 11, name: "ecto"}]
      },
      %{id: 2, author_id: 1, title: "P2", tags: [%{id: 10, name: "elixir"}]}
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Post, attrs_list)

    assert Repo.all(from t in Tag, order_by: t.id, select: {t.id, t.name}) ==
             [{10, "elixir"}, {11, "ecto"}]

    join_rows =
      Repo.all(
        from j in "posts_tags", order_by: [j.post_id, j.tag_id], select: {j.post_id, j.tag_id}
      )

    assert join_rows == [{1, 10}, {1, 11}, {2, 10}]

    # Upserting the same attrs again is idempotent (relies on the join table's unique index)
    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Post, attrs_list)
    assert Repo.aggregate("posts_tags", :count) == 3
  end

  test "does not upsert nested belongs_to associations; the foreign key rides along on the parent" do
    Repo.insert!(%Author{id: 1, name: "Alice"})
    Repo.insert!(%Category{id: 5, name: "books"})

    # The post supplies both a category_id field and a nested category association
    attrs_list = [
      %{
        id: 1,
        author_id: 1,
        title: "P1",
        category_id: 5,
        category: %{id: 5, name: "updated books"}
      }
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Post, attrs_list)

    # The foreign key is set on the post, but the nested category data is never upserted
    assert Repo.get!(Post, 1).category_id == 5
    assert Repo.all(from c in Category, select: {c.id, c.name}) == [{5, "books"}]
  end

  test "upserts nested associations recursively (has_many -> has_many)" do
    # Comments hang two levels below the author (author -> posts -> comments)
    attrs_list = [
      %{
        id: 1,
        name: "Alice",
        posts: [
          %{
            id: 101,
            author_id: 1,
            title: "a",
            comments: [
              %{id: 1001, post_id: 101, body: "first"},
              %{id: 1002, post_id: 101, body: "second"}
            ]
          }
        ]
      }
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list)

    assert Repo.all(from c in Comment, order_by: c.id, select: {c.post_id, c.body}) ==
             [{101, "first"}, {101, "second"}]
  end

  test "upserts nested associations recursively (has_many -> many_to_many)" do
    # The posts' tags hang two levels below the author, with tag 10 shared between posts
    attrs_list = [
      %{
        id: 1,
        name: "Alice",
        posts: [
          %{id: 101, author_id: 1, title: "a", tags: [%{id: 10, name: "elixir"}]},
          %{id: 102, author_id: 1, title: "b", tags: [%{id: 10, name: "elixir"}]}
        ]
      }
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list)

    assert Repo.all(from t in Tag, select: {t.id, t.name}) == [{10, "elixir"}]

    join_rows =
      Repo.all(
        from j in "posts_tags", order_by: [j.post_id, j.tag_id], select: {j.post_id, j.tag_id}
      )

    assert join_rows == [{101, 10}, {102, 10}]
  end

  test "sets placeholder fields on parent and association rows" do
    timestamp = ~U[2026-01-01 00:00:00.000000Z]

    attrs_list = [
      %{id: 1, name: "Alice", posts: [%{id: 101, author_id: 1, title: "a"}]}
    ]

    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, attrs_list,
        placeholders: %{
          Author => %{inserted_at: timestamp},
          Post => %{inserted_at: timestamp}
        }
      )

    assert Repo.get!(Author, 1).inserted_at == timestamp
    assert Repo.get!(Post, 101).inserted_at == timestamp
  end

  test "uses changeset_function_atom when provided" do
    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, [%{id: 10, name: "ignored"}],
        changeset_function_atom: :upsert_changeset
      )

    # The alternative changeset only casts :id, so the name never reaches the database
    assert Repo.get!(Author, 10).name == nil
  end

  @tag :capture_log
  test "rejects invalid changesets and reports them as skipped" do
    attrs_list = [
      %{id: 1, name: "valid"},
      %{id: 2}
    ]

    assert {:ok, %{upserted: 1, skipped: 1}} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list)

    assert Repo.all(from a in Author, select: {a.id, a.name}) == [{1, "valid"}]
  end

  @tag :capture_log
  test "recovers configured changeset errors before upsert" do
    attrs_list = [%{id: 1, name: "Alice", phone_number: "INVALID"}]

    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, attrs_list,
        recover_changeset_errors: %{Author => %{phone_number: "555-1234"}}
      )

    assert Repo.get!(Author, 1).phone_number == "555-1234"
  end

  test "applies custom insert_all_opts per schema" do
    insert_all_opts = %{
      Author => [on_conflict: :nothing],
      Post => [on_conflict: {:replace, [:title]}]
    }

    attrs_list = [
      %{id: 1, name: "Alice", posts: [%{id: 101, author_id: 1, title: "a"}]}
    ]

    {:ok, _} = BulkUpsert.bulk_upsert(Repo, Author, attrs_list, insert_all_opts: insert_all_opts)

    updated_attrs_list = [
      %{id: 1, name: "Alicia", posts: [%{id: 101, author_id: 1, title: "b"}]}
    ]

    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, updated_attrs_list, insert_all_opts: insert_all_opts)

    # The author conflict did nothing, while the post conflict replaced the title
    assert Repo.get!(Author, 1).name == "Alice"
    assert Repo.get!(Post, 101).title == "b"
  end

  test "replace_all_except preserves the given fields on conflict" do
    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, [%{id: 1, name: "Alice", phone_number: "555-1234"}])

    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, [%{id: 1, name: "Alicia", phone_number: "555-9999"}],
        replace_all_except: [:name]
      )

    author = Repo.get!(Author, 1)
    assert author.name == "Alice"
    assert author.phone_number == "555-9999"
  end

  test "uses insert_all_function_atom when provided" do
    attrs_list = [
      %{id: 1, name: "Alice", posts: [%{id: 101, author_id: 1, title: "a"}]}
    ]

    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, attrs_list,
        insert_all_function_atom: :insert_all_with_autogenerated_timestamps
      )

    # The custom function (the README recipe) autogenerates the insert timestamps
    assert %DateTime{} = Repo.get!(Author, 1).inserted_at
    assert %DateTime{} = Repo.get!(Post, 101).inserted_at
  end

  test "uses insert_all_function_module when provided, passing conflict opts and timeout" do
    {:ok, _} =
      BulkUpsert.bulk_upsert(Repo, Author, [%{id: 1, name: "Alice"}],
        insert_all_function_module: ProxyRepo,
        timeout: 45_000
      )

    assert Repo.get!(Author, 1).name == "Alice"

    assert_received {:proxy_insert_all, Author, [%{id: 1, name: "Alice"}], opts}
    assert opts[:conflict_target] == [:id]
    assert opts[:on_conflict] == {:replace_all_except, [:id]}
    assert opts[:timeout] == 45_000
  end

  test "rolls back the whole transaction when an association upsert fails" do
    # The post references a nonexistent author, so its foreign key constraint fails
    attrs_list = [
      %{id: 1, name: "Alice", posts: [%{id: 101, author_id: 999, title: "a"}]}
    ]

    assert_raise Postgrex.Error, ~r/foreign_key/, fn ->
      BulkUpsert.bulk_upsert(Repo, Author, attrs_list)
    end

    # The author was upserted before the post failed, but the transaction rolled it back
    assert Repo.aggregate(Author, :count) == 0
  end

  test "rolls back all chunks when a later chunk fails" do
    # With chunk_size: 1, the first author is upserted in its own chunk before the second
    # author's post fails its foreign key constraint
    attrs_list = [
      %{id: 1, name: "Alice"},
      %{id: 2, name: "Bob", posts: [%{id: 101, author_id: 999, title: "a"}]}
    ]

    assert_raise Postgrex.Error, ~r/foreign_key/, fn ->
      BulkUpsert.bulk_upsert(Repo, Author, attrs_list, chunk_size: 1)
    end

    assert Repo.aggregate(Author, :count) == 0
  end
end
