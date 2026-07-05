defmodule BulkinupDemo.Blog.Author do
  @moduledoc "A blog author. The parent schema for most demo scenarios."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "authors" do
    field :name, :string
    field :phone_number, :string
    field :inserted_at, :utc_datetime_usec

    has_many :posts, BulkinupDemo.Blog.Post
    has_one :profile, BulkinupDemo.Blog.Profile, foreign_key: :author_id

    embeds_one :address, BulkinupDemo.Blog.Address
    embeds_many :social_links, BulkinupDemo.Blog.SocialLink
  end

  def changeset(author \\ %__MODULE__{}, attrs) do
    author
    |> cast(attrs, [:id, :name, :phone_number])
    |> validate_required([:id, :name])
    |> validate_format(:phone_number, ~r/^\d{3}-\d{4}$/)
    |> cast_assoc(:posts)
    |> cast_assoc(:profile)
    |> cast_embed(:address)
    |> cast_embed(:social_links)
  end

  @doc "An alternative changeset that only accepts an `:id`, for the `:changeset_function_atom` demo."
  def upsert_changeset(author \\ %__MODULE__{}, attrs) do
    author
    |> cast(attrs, [:id])
    |> validate_required([:id])
  end
end
