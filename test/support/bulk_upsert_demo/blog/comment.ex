defmodule BulkUpsertDemo.Blog.Comment do
  @moduledoc "A comment on a blog post (`has_many` from the post, two levels below the author)."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "comments" do
    field :post_id, :integer
    field :body, :string
  end

  def changeset(comment \\ %__MODULE__{}, attrs) do
    comment
    |> cast(attrs, [:id, :post_id, :body])
    |> validate_required([:id, :post_id, :body])
  end
end
