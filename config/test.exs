import Config

config :bulk_upsert, ecto_repos: [BulkUpsertDemo.Repo]

config :bulk_upsert, BulkUpsertDemo.Repo,
  username: System.get_env("POSTGRES_USER", "postgres"),
  password: System.get_env("POSTGRES_PASSWORD", "postgres"),
  database: "bulk_upsert_demo",
  hostname: System.get_env("POSTGRES_HOST", "localhost"),
  port: System.get_env("POSTGRES_PORT", "5432") |> String.to_integer(),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2,
  telemetry_prefix: [:bulk_upsert_demo, :repo]

config :logger, level: System.get_env("LOGGER_LEVEL", "warning") |> String.to_existing_atom()
