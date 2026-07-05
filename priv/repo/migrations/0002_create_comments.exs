defmodule BulkinupDemo.Repo.Migrations.CreateComments do
  use Ecto.Migration

  def change do
    create table(:comments, primary_key: false) do
      add :id, :bigint, primary_key: true
      add :post_id, references(:posts, type: :bigint), null: false
      add :body, :string
    end
  end
end
