defmodule BulkinupDemo.Blog.Address do
  @moduledoc "An author's address (`embeds_one`, stored inline on the author row)."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :street, :string
    field :city, :string
  end

  def changeset(address, attrs) do
    address
    |> cast(attrs, [:street, :city])
    |> validate_required([:street, :city])
  end
end
