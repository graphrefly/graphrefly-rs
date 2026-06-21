import graphrefly


def test_python_callback_sync_wave():
    seen = []
    graph = graphrefly.Graph("py-smoke")
    source = graph.state(1, "source")
    plus_one = graph.derived([source], lambda value: value + 1, "plus_one")
    factories = {node["name"]: node["factory"] for node in graph.describe()["nodes"]}
    assert factories["plus_one"] == "derived"

    sub = plus_one.subscribe(lambda kind, value: seen.append((kind, value)))
    assert plus_one.cache() == 2

    source.set(4)
    assert plus_one.cache() == 5
    assert plus_one.status() in {"settled", "resolved"}

    sub.unsubscribe()
    assert ("DATA", 2) in seen
    assert ("DATA", 5) in seen


def test_callback_none_maps_to_error():
    seen = []
    graph = graphrefly.Graph("py-error-smoke")
    source = graph.state(1, "source")
    bad = graph.derived([source], lambda _value: None, "bad")

    sub = bad.subscribe(lambda kind, value: seen.append((kind, value)))
    sub.unsubscribe()

    assert any(kind == "ERROR" and "None is the binding sentinel" in value for kind, value in seen)


def test_batch_callback_exception_rolls_back():
    graph = graphrefly.Graph("py-batch-smoke")
    source = graph.state(1, "source")

    def mutate_then_raise():
        source.set(9)
        raise ValueError("boom")

    try:
        graph.batch(mutate_then_raise)
    except ValueError:
        pass
    else:
        raise AssertionError("batch should re-raise the Python exception")

    assert source.cache() == 1


def test_graph_panic_maps_to_python_runtime_error():
    graph = graphrefly.Graph("py-runtime-error-smoke")
    graph.state(1, "same")

    try:
        graph.state(2, "same")
    except RuntimeError as error:
        assert "duplicate graph node id" in str(error)
    else:
        raise AssertionError("duplicate node names should become a Python RuntimeError")


if __name__ == "__main__":
    test_python_callback_sync_wave()
    test_callback_none_maps_to_error()
    test_batch_callback_exception_rolls_back()
    test_graph_panic_maps_to_python_runtime_error()
    print("py-smoke ok")
