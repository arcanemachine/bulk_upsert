import Config

# Import environment-specific config if it exists
env_config = "#{config_env()}.exs"

if File.exists?("#{__DIR__}/#{env_config}") do
  import_config env_config
end
