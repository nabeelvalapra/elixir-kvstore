defmodule KVServer.Command do
  @doc ~S"""
    Parses the given `line` into a command.
    
    iex> KVServer.Command.parse "CREATE shopping\r\n"
    {:ok, {:create, "shopping"}}

    iex> KVServer.Command.parse "PUT shopping eat bread \r\n"
    {:ok, {:put, "shopping", "eat", "bread"}}
  """

  def parse(line) do
    case String.split(line) do
      ["CREATE", bucket] -> {:ok, {:create, bucket}}
      ["GET", bucket, key] -> {:ok, {:get, bucket, key}}
      ["PUT", bucket, key, value] -> {:ok, {:put, bucket, key, value}}
      ["DELETE", bucket, key] -> {:ok, {:delete, bucket, key}}
      _ -> {:error, :unknown_command}
    end
  end

  def run({:create, bucket}, reg_pid) do
    KV.Registry.create(reg_pid, bucket)
    {:ok, "OK\r\n"}
  end

  def run({:get, bucket, key}, reg_pid) do
    lookup reg_pid, bucket, fn pid ->
      value = KV.Bucket.get(pid, key)
      {:ok, "#{value}\r\nOK\r\n"}
    end
  end

  def run({:put, bucket, key, value}, reg_pid) do
   lookup reg_pid, bucket, fn pid ->
      KV.Bucket.put(pid, key, value)
      {:ok, "OK\r\n"}
    end
  end

  def run({:delete, bucket, key}, reg_pid) do
    lookup reg_pid, bucket, fn pid ->
      KV.Bucket.delete(pid, key)
      {:ok, "OK\r\n"}
    end
  end

  defp lookup(reg_pid, bucket, callback) do
    case KV.Registry.lookup(reg_pid, bucket) do 
      {:ok, pid} -> callback.(pid)
      :error -> {:error, :not_found}
    end
  end
end
