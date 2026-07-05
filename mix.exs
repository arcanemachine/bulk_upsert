defmodule BulkUpsert.MixProject do
  use Mix.Project

  @project_name "Bulk Upsert"
  @source_url "https://github.com/arcanemachine/bulk_upsert"
  @version "0.5.0"

  def project do
    [
      app: :bulk_upsert,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),

      # Hex
      description:
        "Upsert multiple nested Ecto schema structs to the database with a single function call.",
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
      # `insert_all`'s `:placeholders` option requires Ecto v3.6.0 or later
      {:ecto, "~> 3.6"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},

      # Used only by the demo app that backs the test suite
      {:ecto_sql, "~> 3.0", only: :test},
      {:jason, "~> 1.0", only: :test},
      {:postgrex, ">= 0.0.0", only: :test}
    ]
  end

  defp aliases do
    [
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end

  # The demo app used by the test suite lives in `test/support/`
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp docs do
    [
      extras: [
        "README.md": [title: "Readme"],
        "CHANGELOG.md": [title: "Changelog"]
      ],
      formatters: ["html"],
      main: "readme",
      source_ref: "v#{@version}"
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
