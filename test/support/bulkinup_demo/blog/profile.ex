defmodule BulkinupDemo.Blog.Profile do
  @moduledoc "An author's profile (`has_one` from the author)."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "profiles" do
    field :author_id, :integer
    field :bio, :string
  end

  def changeset(profile \\ %__MODULE__{}, attrs) do
    profile
    |> cast(attrs, [:id, :author_id, :bio])
    |> validate_required([:id, :author_id, :bio])
  end
end
