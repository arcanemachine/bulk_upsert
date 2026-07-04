# Start the demo application for tests
{:ok, _} = BulkUpsertDemo.Application.start(:normal, [])

ExUnit.start()
Ecto.Adapters.SQL.Sandbox.mode(BulkUpsertDemo.Repo, :manual)
