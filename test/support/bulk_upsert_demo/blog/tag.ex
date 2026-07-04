defmodule BulkUpsertDemo.Blog.Tag do
  @moduledoc "A post tag (`many_to_many` with posts via the `posts_tags` join table)."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "tags" do
    field :name, :string
  end

  def changeset(tag \\ %__MODULE__{}, attrs) do
    tag
    |> cast(attrs, [:id, :name])
    |> validate_required([:id, :name])
  end
end
