defmodule BulkinupDemo.ProxyRepo do
  @moduledoc """
  An `:insert_all_function_module` that records each call by messaging the calling process, then
  delegates to the real repo.

  `Bulkinup.upsert/4` runs its transaction in the calling process, so a test that passes
  this module can `assert_received {:proxy_insert_all, ...}` to inspect the exact arguments given
  to `insert_all/3` — no global state required.
  """

  def insert_all(schema_or_source, entries, opts) do
    send(self(), {:proxy_insert_all, schema_or_source, entries, opts})
    BulkinupDemo.Repo.insert_all(schema_or_source, entries, opts)
  end
end
