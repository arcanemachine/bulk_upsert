defmodule BulkUpsertDemo.Blog.Post do
  @moduledoc "A blog post, written by an author, optionally categorized and tagged."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "posts" do
    field :author_id, :integer
    field :title, :string
    field :inserted_at, :utc_datetime_usec

    belongs_to :category, BulkUpsertDemo.Blog.Category, type: :integer

    has_many :comments, BulkUpsertDemo.Blog.Comment

    many_to_many :tags, BulkUpsertDemo.Blog.Tag,
      join_through: "posts_tags",
      join_keys: [post_id: :id, tag_id: :id]
  end

  def changeset(post \\ %__MODULE__{}, attrs) do
    post
    |> cast(attrs, [:id, :author_id, :category_id, :title])
    |> validate_required([:id, :author_id, :title])
    |> cast_assoc(:category)
    |> cast_assoc(:comments)
    |> cast_assoc(:tags)
  end
end
