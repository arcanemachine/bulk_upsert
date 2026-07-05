defmodule BulkinupDemo.DataCase do
  @moduledoc """
  This module defines the setup for tests requiring access to the demo application's data layer.

  The SQL sandbox reverts all database changes at the end of every test, so tests may run with
  `use BulkinupDemo.DataCase, async: true`.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      alias BulkinupDemo.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import BulkinupDemo.DataCase
    end
  end

  setup tags do
    BulkinupDemo.DataCase.setup_sandbox(tags)
    :ok
  end

  @doc "Sets up the sandbox based on the test tags."
  def setup_sandbox(tags) do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(BulkinupDemo.Repo, shared: not tags[:async])
    ExUnit.Callbacks.on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
  end

  @doc """
  Count the INSERT queries issued against a given table by messaging the test process for each
  matching Ecto telemetry event. Call before acting, then read the count with
  `count_insert_queries/1`.

  Queries run in the calling process (the test itself), so filtering on `self()` keeps
  concurrently-running async tests from polluting each other's counts.
  """
  def attach_insert_counter(source) do
    handler_id = "insert-counter-#{inspect(self())}-#{source}"
    test_pid = self()

    :telemetry.attach(
      handler_id,
      [:bulkinup_demo, :repo, :query],
      fn _event, _measurements, metadata, _config ->
        if self() == test_pid and metadata.source == source and
             String.starts_with?(metadata.query, "INSERT") do
          send(test_pid, {:insert_query, source})
        end
      end,
      nil
    )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  @doc "Read the number of INSERT queries recorded by `attach_insert_counter/1`."
  def count_insert_queries(source) do
    receive do
      {:insert_query, ^source} -> 1 + count_insert_queries(source)
    after
      0 -> 0
    end
  end
end
