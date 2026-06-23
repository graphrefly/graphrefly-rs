from graphrefly import _native as graphrefly


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


def test_callback_none_is_data():
    seen = []
    graph = graphrefly.Graph("py-none-smoke")
    source = graph.state(1, "source")
    none_value = graph.derived([source], lambda _value: None, "none_value")

    sub = none_value.subscribe(lambda kind, value: seen.append((kind, value)))
    sub.unsubscribe()

    assert ("DATA", None) in seen


def test_state_empty_cache_entry_distinguishes_absent_from_cached_none():
    graph = graphrefly.Graph("py-no-data-smoke")
    empty = graph.state_empty("empty")
    none_value = graph.state(None, "none_value")

    assert empty.cache_entry() == (False, None)
    assert none_value.cache_entry() == (True, None)


def test_native_control_methods_pause_resume_and_invalidate():
    graph = graphrefly.Graph("py-native-control-smoke")
    source = graph.state(1, "source")
    derived = graph.derived([source], lambda value: value + 1, "derived")
    seen = []
    sub = derived.subscribe(lambda kind, value: seen.append((kind, value)))

    assert derived.cache() == 2
    derived.pause("lock")
    source.set(2)
    assert derived.cache() == 2

    derived.resume("lock")
    assert derived.cache() == 3

    seen.clear()
    derived.invalidate()
    assert derived.cache_entry() == (False, None)
    assert any(kind == "INVALIDATE" for kind, _value in seen)
    sub.unsubscribe()


def test_callback_system_exit_reraises_without_graph_error():
    graph = graphrefly.Graph("py-fatal-smoke")
    source = graph.state(1, "source")

    def boom(_value):
        raise SystemExit("exit")

    bad = graph.derived([source], boom, "bad")

    try:
        bad.subscribe(lambda kind, value: None)
    except SystemExit as error:
        assert str(error) == "exit"
    else:
        raise AssertionError("fatal BaseException should re-raise to the Python caller")

    assert bad.status() != "errored"


def test_callback_keyboard_interrupt_reraises_without_graph_error():
    graph = graphrefly.Graph("py-keyboard-interrupt-smoke")
    source = graph.state(1, "source")

    def boom(_value):
        raise KeyboardInterrupt

    bad = graph.derived([source], boom, "bad")

    try:
        bad.subscribe(lambda kind, value: None)
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("KeyboardInterrupt should re-raise to the Python caller")

    assert bad.status() != "errored"


def test_callback_exception_emits_graph_error():
    seen = []
    graph = graphrefly.Graph("py-exception-error-smoke")
    source = graph.state(1, "source")

    def boom(_value):
        raise ValueError("boom")

    bad = graph.derived([source], boom, "bad")
    sub = bad.subscribe(lambda kind, value: seen.append((kind, value)))
    assert bad.status() == "errored"
    sub.unsubscribe()

    assert any(kind == "ERROR" and "ValueError: boom" in value for kind, value in seen)


def test_callback_system_exit_during_batch_commit_reraises_without_graph_error():
    graph = graphrefly.Graph("py-batch-commit-fatal-smoke")
    source = graph.state_empty("source")
    seen = []

    def boom(_value):
        raise SystemExit("batch commit exit")

    bad = graph.derived([source], boom, "bad")
    sub = bad.subscribe(lambda kind, value: seen.append((kind, value)))
    try:
        graph.batch(lambda: source.set(1))
    except SystemExit as error:
        assert str(error) == "batch commit exit"
    else:
        raise AssertionError("fatal BaseException should re-raise during batch commit")
    finally:
        sub.unsubscribe()

    assert bad.status() != "errored"
    assert not any(kind == "ERROR" for kind, _value in seen)


def test_callback_keyboard_interrupt_during_batch_commit_reraises_without_graph_error():
    graph = graphrefly.Graph("py-batch-commit-keyboard-interrupt-smoke")
    source = graph.state_empty("source")
    seen = []

    def boom(_value):
        raise KeyboardInterrupt

    bad = graph.derived([source], boom, "bad")
    sub = bad.subscribe(lambda kind, value: seen.append((kind, value)))
    try:
        graph.batch(lambda: source.set(1))
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("KeyboardInterrupt should re-raise during batch commit")
    finally:
        sub.unsubscribe()

    assert bad.status() != "errored"
    assert not any(kind == "ERROR" for kind, _value in seen)


def test_native_async_source_hidden_gate_smoke():
    graph = graphrefly.Graph("py-native-async-source-smoke")
    gates = []
    seen = []

    def activate(ctx):
        gates.append(ctx)

    node = graph._async_source(activate, "async_source", "true")
    sub = node.subscribe(lambda kind, value: seen.append((kind, value)))

    assert len(gates) == 1
    gates[0].resolve(7)
    assert node.cache() == 7
    sub.unsubscribe()

    factories = {entry["name"]: entry["factory"] for entry in graph.describe()["nodes"]}
    assert factories["async_source"] == "producer"
    assert ("DATA", 7) in seen
    assert ("COMPLETE", None) in seen


def test_observe_system_exit_during_registration_reraises():
    graph = graphrefly.Graph("py-observe-eager-fatal-smoke")
    source = graph.state(1, "source")
    source.subscribe(lambda kind, value: None)
    calls = 0

    def observer(_path, _kind, _value, _tier, _seq):
        nonlocal calls
        calls += 1
        raise SystemExit("observe eager exit")

    try:
        graph.observe(observer)
    except SystemExit as error:
        assert str(error) == "observe eager exit"
    else:
        raise AssertionError("fatal observe callback should re-raise during registration")

    assert calls == 1


def test_observe_keyboard_interrupt_during_registration_reraises():
    graph = graphrefly.Graph("py-observe-eager-keyboard-interrupt-smoke")
    source = graph.state(1, "source")
    source.subscribe(lambda kind, value: None)
    calls = 0

    def observer(_path, _kind, _value, _tier, _seq):
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt

    try:
        graph.observe(observer)
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("fatal observe callback should re-raise during registration")

    assert calls == 1


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
    test_callback_none_is_data()
    test_native_control_methods_pause_resume_and_invalidate()
    test_callback_keyboard_interrupt_reraises_without_graph_error()
    test_callback_exception_emits_graph_error()
    test_callback_system_exit_during_batch_commit_reraises_without_graph_error()
    test_callback_keyboard_interrupt_during_batch_commit_reraises_without_graph_error()
    test_observe_keyboard_interrupt_during_registration_reraises()
    test_batch_callback_exception_rolls_back()
    test_graph_panic_maps_to_python_runtime_error()
    print("py-smoke ok")
