import pickle_1
import io
import sys
import pytest

def roundtrip(obj, protocol=None):
    data = pickle_1.dumps(obj, protocol=protocol)
    buf = io.BytesIO()
    pickle_1.dump(obj, buf, protocol=protocol)
    assert buf.getvalue() == data
    import pickle
    try:
        std = pickle.loads(data)
        assert std == obj
    except Exception:
        pass  

def test_none():
    roundtrip(None)

def test_bool():
    roundtrip(True)
    roundtrip(False)

def test_int():
    for v in [0, 1, -1, 255, 256, 65535, 65536, 2**31-1, -2**31, 2**63-1, -2**63]:
        roundtrip(v)

def test_float():
    for v in [0.0, 1.5, -2.3, float('inf'), float('-inf'), float('nan')]:
        roundtrip(v)

def test_str():
    for s in ["", "abc", "你好", "a" * 1000]:
        roundtrip(s)

def test_bytes():
    for b in [b"", b"abc", bytes(range(256)), b"a" * 1000]:
        roundtrip(b)

def test_list():
    roundtrip([])
    roundtrip([1, 2, 3])
    roundtrip([None, True, 123, "abc", [1,2], {"a":1}])

def test_tuple():
    roundtrip(())
    roundtrip((1,))
    roundtrip((1,2,3))
    roundtrip((None, True, 1.2, "x", (1,2)))

def test_dict():
    roundtrip({})
    roundtrip({"a": 1, "b": 2})
    roundtrip({1: "a", 2: [1,2], 3: {"x":1}})

def test_nested():
    obj = {"a": [1, (2, 3), {"b": b"bytes"}], "c": (None, True, [1,2,3])}
    roundtrip(obj)

def test_large_bytes():
    b = b"x" * (70 * 1024)
    roundtrip(b)
    s = "y" * (70 * 1024)
    roundtrip(s)

def test_unsupported_type():
    class Foo: pass
    with pytest.raises(pickle_1.PicklingError):
        pickle_1.dumps(Foo())

def test_put_get():
    a = []
    obj = [a, a]
    data = pickle_1.dumps(obj)
    import pickle
    std = pickle.loads(data)
    assert std[0] is std[1]

def test_tuple_memo():
    t = (1,2,3)
    obj = [t, t]
    data = pickle_1.dumps(obj)
    import pickle
    std = pickle.loads(data)
    assert std[0] is std[1]

def test_dict_memo():
    d = {"x":1}
    obj = [d, d]
    data = pickle_1.dumps(obj)
    import pickle
    std = pickle.loads(data)
    assert std[0] is std[1]


def test_invalid_file():
    class Dummy:
        pass
    with pytest.raises(TypeError):
        pickle_1.dump(123, Dummy())

def test_invalid_protocol():
    with pytest.raises(ValueError):
        pickle_1.dumps(1, protocol=999)

def test_buffer_callback():
    if pickle_1.HIGHEST_PROTOCOL >= 5:
        with pytest.raises(ValueError):
            pickle_1.dumps(b"abc", protocol=4, buffer_callback=lambda x: None)

def test_main_flow():
    obj = {"hello": "world"}
    data = pickle_1.dumps(obj)
    assert isinstance(data, bytes)
    buf = io.BytesIO()
    pickle_1.dump(obj, buf)
    assert buf.getvalue() == data
