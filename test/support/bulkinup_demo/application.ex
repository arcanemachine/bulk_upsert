defmodule BulkinupDemo.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [BulkinupDemo.Repo]

    opts = [strategy: :one_for_one, name: BulkinupDemo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
