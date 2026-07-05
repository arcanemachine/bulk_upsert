defmodule BulkinupDemo.Repo.Migrations.CreateBlogTables do
  use Ecto.Migration

  def change do
    create table(:authors, primary_key: false) do
      add :id, :bigint, primary_key: true
      add :name, :string
      add :phone_number, :string
      add :address, :map
      add :social_links, {:array, :map}
      add :inserted_at, :utc_datetime_usec
    end

    create table(:profiles, primary_key: false) do
      add :id, :bigint, primary_key: true
      add :author_id, references(:authors, type: :bigint), null: false
      add :bio, :string
    end

    create table(:categories, primary_key: false) do
      add :id, :bigint, primary_key: true
      add :name, :string
    end

    create table(:posts, primary_key: false) do
      add :id, :bigint, primary_key: true
      add :author_id, references(:authors, type: :bigint), null: false
      add :category_id, references(:categories, type: :bigint)
      add :title, :string
      add :inserted_at, :utc_datetime_usec
    end

    create table(:tags, primary_key: false) do
      add :id, :bigint, primary_key: true
      add :name, :string
    end

    create table(:posts_tags, primary_key: false) do
      add :post_id, references(:posts, type: :bigint), null: false
      add :tag_id, references(:tags, type: :bigint), null: false
    end

    # Required by the join table's upsert (`on_conflict: :nothing` with a conflict target)
    create unique_index(:posts_tags, [:post_id, :tag_id])
  end
end
