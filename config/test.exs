import Config

config :bulkinup, ecto_repos: [BulkinupDemo.Repo]

config :bulkinup, BulkinupDemo.Repo,
  username: System.get_env("POSTGRES_USER", "postgres"),
  password: System.get_env("POSTGRES_PASSWORD", "postgres"),
  database: "bulkinup_demo",
  hostname: System.get_env("POSTGRES_HOST", "localhost"),
  port: System.get_env("POSTGRES_PORT", "5432") |> String.to_integer(),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2,
  telemetry_prefix: [:bulkinup_demo, :repo]

config :logger, level: System.get_env("LOGGER_LEVEL", "warning") |> String.to_existing_atom()
