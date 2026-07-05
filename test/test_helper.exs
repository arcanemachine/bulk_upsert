# Start the demo application for tests
{:ok, _} = BulkinupDemo.Application.start(:normal, [])

ExUnit.start()
Ecto.Adapters.SQL.Sandbox.mode(BulkinupDemo.Repo, :manual)
