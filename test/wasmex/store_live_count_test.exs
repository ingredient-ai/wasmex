defmodule Wasmex.StoreLiveCountTest do
  # Not async: the count is VM-wide, so stores created by concurrent tests would move it.
  use ExUnit.Case, async: false

  test "counts core and component stores until they are dropped" do
    baseline = settled_live_count()

    # Stores live only on the task's heap, so they become garbage when it exits.
    Task.async(fn ->
      {:ok, store} = Wasmex.Store.new()
      {:ok, wasi_store} = Wasmex.Store.new_wasi(%Wasmex.Wasi.WasiOptions{})
      {:ok, component_store} = Wasmex.Components.Store.new()
      {:ok, component_wasi_store} = Wasmex.Components.Store.new_wasi()
      stores = [store, wasi_store, component_store, component_wasi_store]

      # Reading `stores` after the count keeps them reachable until it is taken.
      assert Wasmex.Store.live_count() == baseline + length(stores)
    end)
    |> Task.await()

    :erlang.garbage_collect()
    assert eventually(fn -> Wasmex.Store.live_count() == baseline end)
  end

  # Stores dropped by earlier tests may still be finishing; wait until the count holds still.
  defp settled_live_count do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    count = Wasmex.Store.live_count()
    Process.sleep(50)

    if Wasmex.Store.live_count() == count, do: count, else: settled_live_count()
  end

  defp eventually(check, attempts \\ 200) do
    cond do
      check.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(check, attempts - 1)
    end
  end
end
