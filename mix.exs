defmodule BulkUpsert.MixProject do
  use Mix.Project

  @project_name "Bulk Upsert"
  @source_url "https://github.com/arcanemachine/bulk_upsert"
  @version "0.1.1"

  def project do
    [
      app: :bulk_upsert,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Hex
      description:
        "Upsert multiple Ecto schema structs to the database with a single function call.",
      package: package(),

      # Docs
      name: @project_name,
      source_url: @source_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ecto, "~> 3.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: @project_name,
      extras: ["README.md"],
      formatters: ["html"],
      main: "readme"
    ]
  end

  defp package do
    [
      maintainers: ["Nicholas Moen"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(.formatter.exs mix.exs README.md CHANGELOG.md lib)
    ]
  end
end
