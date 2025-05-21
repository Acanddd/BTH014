#!/usr/bin/env python3

# -- coding: utf-8 --
import pickle
import hashlib
import platform
import sys
import pytest
import random
import string
import itertools
from collections import defaultdict, OrderedDict
from functools import partial
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from fractions import Fraction
from uuid import UUID
import zlib
import gc
import threading
import time as time_module
from memory_profiler import memory_usage
from pickle import PicklingError, UnpicklingError

# --------------------------
# Test environment config and utility functions
# --------------------------
def get_environment_info():
    """Collect information about the test environment"""
    return {
        "platform": platform.platform(),
        "python_version": sys.version,
        "pickle_highest_protocol": pickle.HIGHEST_PROTOCOL,
        "system_encoding": sys.getdefaultencoding()
    }


def print_environment_info_once():
    """Print environment information only once per run."""
    if not hasattr(print_environment_info_once, "printed"):
        print_environment_info_once.printed = True
        info = get_environment_info()
        print("\n" + "="*50)
        print("Test Environment Info:")
        for k, v in info.items():
            print(f"{k:>20}: {v}")
        print("="*50 + "\n")

def compare_objects(obj1, obj2):
    """Deep compare two objects, handle special values like NaN"""
    if isinstance(obj1, float) and isinstance(obj2, float):
        if obj1 != obj1 and obj2 != obj2:  # both are NaN
            return True
    return obj1 == obj2

def compare_object_graphs(obj1, obj2, memo=None):
    """Compare complex object graphs"""
    if memo is None:
        memo = {id(obj1): id(obj2)}

    if id(obj1) in memo:
        return id(obj2) == memo[id(obj1)]

    if type(obj1) != type(obj2):
        return False

    # Handle container types
    if isinstance(obj1, (list, tuple)):
        if len(obj1) != len(obj2):
            return False
        memo[id(obj1)] = id(obj2)
        return all(compare_object_graphs(x, y, memo) for x, y in zip(obj1, obj2))

    # Other types comparison...
    return compare_objects(obj1, obj2)

# --------------------------
# Improved test data generator
# --------------------------
def generate_safe_random_basic_type(depth=0, max_depth=2):
    """Generate absolutely safe random basic types (all generated types are hashable)"""
    # Basic hashable types
    basic_choices = [
        lambda: random.randint(-100, 100),
        lambda: random.choice([True, False, None]),
        lambda: round(random.random(), 4),
        lambda: ''.join(random.choices(string.ascii_letters, k=random.randint(1, 10))),
        lambda: bytes(random.getrandbits(8) for _ in range(random.randint(0, 10))),
        lambda: datetime.now().replace(microsecond=0),
        lambda: date.today(),
        lambda: UUID(int=random.getrandbits(128)),
        lambda: Decimal(str(round(random.random() * 100, 2))),
        lambda: Fraction(random.randint(1, 10), random.randint(1, 10))
    ]

    # Container types (all elements are hashable)
    container_choices = []
    if depth < max_depth:
        container_choices = [
            lambda: tuple(
                generate_safe_random_basic_type(depth+1, max_depth)
                for _ in range(random.randint(0, 3))
            ),
            lambda: frozenset(
                generate_safe_random_basic_type(depth+1, max_depth)
                for _ in range(random.randint(0, 3))
            ),
            # Convert dict structure to hashable tuple form
            lambda: tuple(
                (str(i), generate_safe_random_basic_type(depth+1, max_depth))
                for i in range(random.randint(0, 3))
            )
        ]

    return random.choice(basic_choices + container_choices)()

def generate_performance_test_object(size=1000):
    """Generate object for performance testing (fully hashable fixed structure)"""
    return {
        "numbers": tuple(random.randint(0, 100) for _ in range(size)),
        "strings": tuple("test" for _ in range(size // 10)),
        "nested": (
            tuple(random.random() for _ in range(size // 20)),
            frozenset(random.randint(0, 10) for _ in range(size // 50))
        ),
        "metadata": (
            datetime.now().replace(microsecond=0).isoformat(),
            "1.0"
        )
    }

def generate_circular_reference(depth=3):
    """Generate circular reference structure"""
    lst = [1, 2, 3]
    current = lst
    for _ in range(depth):
        new_list = [current, 4, 5, 6]
        current.append(new_list)
        current = new_list
    current.append(lst)
    return lst

# --------------------------
# Test cases
# --------------------------
class TestPickleStability:
    """Test pickle serialization stability"""

    @pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
    def test_primitive_types(self, protocol):
        """Test primitive data types"""
        test_cases = [
            # Integers
            0, 1, -1, 2**31-1, -2**31, 2**63-1, -2**63,
            # Floats
            0.0, -0.0, 1.1, -1.1, float('inf'), float('-inf'), float('nan'),
            # Bool and None
            True, False, None,
            # Strings
            "", "hello", "你好", "\x00", "\xff", "a"*1000,
            # Binary data
            b"", b"\x00\xff", b"\x00"*1000,
        ]
        
        for obj in test_cases:
            data1 = pickle.dumps(obj, protocol=protocol)
            data2 = pickle.dumps(obj, protocol=protocol)
            assert data1 == data2, f"Inconsistent serialization result for same object: {obj}"
            
            obj2 = pickle.loads(data1)
            assert compare_objects(obj, obj2), f"Inconsistent deserialization result: {obj}"

class TestPickleCorrectness:
    """Test pickle serialization correctness"""

    @pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
    def test_container_types(self, protocol):
        """Test container types"""
        test_cases = [
            # Lists
            [], [1, 2, 3], [[1, 2], [3, 4]], [1, "a", 3.14, None, True],
            # Tuples
            (), (1, 2), (1, "a", 3.14), ((1, 2), (3, 4)),
            # Dicts
            {}, {"a": 1}, {1: "a", "b": 2.7}, {"x": {"y": {"z": 1}}},
            # Sets
            set(), {1, 2, 3}, {1, "a", 3.14},
            # Special containers
            OrderedDict([(1, "a"), (2, "b")]), defaultdict(int, {"a": 1}),
        ]
        
        for obj in test_cases:
            data = pickle.dumps(obj, protocol=protocol)
            obj2 = pickle.loads(data)
            assert compare_object_graphs(obj, obj2), f"Deserialization failed for container type: {obj}"

class TestPickleEdgeCases:
    """Test edge cases"""

    def test_recursive_structures(self):
        """Test recursive structures"""
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            # Self-referencing list
            lst = [1, 2, 3]
            lst.append(lst)
            data = pickle.dumps(lst, protocol=protocol)
            lst2 = pickle.loads(data)
            assert lst2[3] is lst2
            
            # Cross reference
            a = [1, 2]
            b = [3, 4]
            a.append(b)
            b.append(a)
            data = pickle.dumps((a, b), protocol=protocol)
            a2, b2 = pickle.loads(data)
            assert a2[2] is b2
            assert b2[2] is a2

    def test_special_objects(self):
        """Test special objects"""
        test_cases = [
            datetime.now(),
            date.today(),
            time(12, 34, 56),
            timedelta(days=1, seconds=3600),
            Decimal("3.141592653589793238462643383279"),
            Fraction(22, 7),
            UUID('12345678123456781234567812345678'),
            complex(1, 2),
            range(10),
            slice(1, 10, 2),
        ]
        
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            for obj in test_cases:
                data = pickle.dumps(obj, protocol=protocol)
                obj2 = pickle.loads(data)
                assert obj == obj2, f"Deserialization failed for special object: {obj}"

class TestPickleFuzzing:
    """Fuzz testing"""

    @pytest.mark.parametrize("iterations", [50])  # Reduce iterations for speed
    def test_random_objects(self, iterations):
        """Random object test (ensure all generated objects are hashable)"""
        for _ in range(iterations):
            obj = generate_safe_random_basic_type(max_depth=2)  # Limit max depth
            for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
                try:
                    data = pickle.dumps(obj, protocol=protocol)
                    obj2 = pickle.loads(data)
                    assert compare_objects(obj, obj2), f"Random object test failed: {obj}"
                except (PicklingError, UnpicklingError) as e:
                    pytest.fail(f"Unexpected pickle error: {e} (object: {obj})")

class TestPicklePerformance:
    """Performance testing"""

    def test_serialization_speed(self):
        """Serialization speed test"""
        obj = generate_performance_test_object(size=500)
        results = {}
        
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            # Serialization test
            start = time_module.time()
            for _ in range(50):
                pickle.dumps(obj, protocol=protocol)
            duration = time_module.time() - start
            results[f"protocol_{protocol}_dump"] = duration / 50 * 1000  # ms/op
            
            # Deserialization test
            data = pickle.dumps(obj, protocol=protocol)
            start = time_module.time()
            for _ in range(50):
                pickle.loads(data)
            duration = time_module.time() - start
            results[f"protocol_{protocol}_load"] = duration / 50 * 1000  # ms/op
        
        print("\nSerialization performance results (ms/op):")
        for k, v in results.items():
            print(f"{k:>20}: {v:.2f}")

    def test_memory_usage(self):
        """Memory usage test"""
        obj = generate_performance_test_object(size=1000)
        
        def test_dump():
            return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        
        def test_load():
            data = test_dump()
            return pickle.loads(data)
        
        dump_mem = max(memory_usage(test_dump))
        load_mem = max(memory_usage(test_load))
        
        print(f"\nMemory usage - serialization: {dump_mem:.2f} MiB")
        print(f"Memory usage - deserialization: {load_mem:.2f} MiB")

class TestPickleSecurity:
    """Security testing"""

    def test_malicious_data(self):
        """Test maliciously crafted data"""
        malicious_cases = [
            b"",  # Empty data
            b"invalid pickle data",
            b"\x80\x04\x95\x0b\x00\x00\x00\x00\x00\x00\x00J\xff\xff\xff\xff.",  # Maliciously crafted
            zlib.compress(b"malformed data"),  # Corrupted compressed data
            b"pickle." + b"a"*10**6,  # Oversized invalid data
        ]
        
        for data in malicious_cases:
            with pytest.raises((UnpicklingError, EOFError, ValueError)):
                pickle.loads(data)

# --------------------------
# Main test entry
# --------------------------
if __name__ == "__main__":
    print_environment_info_once()

    # Run tests and generate report
    import os
    import json
    from datetime import datetime

    test_results = {
        "environment": get_environment_info(),
        "timestamp": datetime.now().isoformat(),
        "tests": []
    }

    # Run tests and collect results
    for test_class in [
        TestPickleStability,
        TestPickleCorrectness,
        TestPickleEdgeCases,
        TestPickleFuzzing,
        TestPicklePerformance,
        TestPickleSecurity
    ]:
        class_name = test_class.__name__
        print(f"\nRunning {class_name}...")
        
        # Use pytest API to run tests
        pytest_args = [
            f"{__file__}::{class_name}",
            "-v",
            "--capture=no"
        ]
        exit_code = pytest.main(pytest_args)
        
        test_results["tests"].append({
            "class": class_name,
            "status": "passed" if exit_code == 0 else "failed"
        })
