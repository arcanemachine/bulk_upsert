defmodule BulkUpsertDemo.Blog.Category do
  @moduledoc "A post category (`belongs_to` from the post)."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "categories" do
    field :name, :string
  end

  def changeset(category \\ %__MODULE__{}, attrs) do
    category
    |> cast(attrs, [:id, :name])
    |> validate_required([:id, :name])
  end
end
