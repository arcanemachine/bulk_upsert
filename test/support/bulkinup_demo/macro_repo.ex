defmodule BulkinupDemo.MacroRepo do
  @moduledoc """
  A `use Bulkinup` demo module exercising every kind of `use`-time default: a flat key
  (`:insert_all_module`), a flat key with a dynamic (runtime-evaluated) value
  (`:timeout`), a flat upsert-only key (`:replace_all_except`), a flat key overridden per verb
  (`:chunk_size`), and a per-verb namespace override (`upsert: [replace_all_except: ...]`).

  It is not an Ecto repo itself: `insert_all` calls are routed to `ProxyRepo` (so tests can
  inspect the opts each call received), and `transaction/2` delegates to the real repo.
  """

  use Bulkinup,
    insert_all_module: BulkinupDemo.ProxyRepo,
    timeout: default_timeout(),
    replace_all_except: [:phone_number],
    chunk_size: 3,
    insert: [chunk_size: 2],
    upsert: [replace_all_except: [:inserted_at]]

  def default_timeout, do: 12_345

  defdelegate transaction(fun, opts), to: BulkinupDemo.Repo
end
