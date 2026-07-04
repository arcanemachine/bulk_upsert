defmodule BulkUpsertDemo.Blog.SocialLink do
  @moduledoc "An author's social link (`embeds_many`, stored inline on the author row)."

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :label, :string
    field :url, :string
  end

  def changeset(social_link, attrs) do
    social_link
    |> cast(attrs, [:label, :url])
    |> validate_required([:label, :url])
  end
end
