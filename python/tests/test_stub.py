"""`_native.pyi` is checked against the extension module it describes: the
same public names, and the same parameters and defaults on every function
that carries a `__text_signature__`."""

import ast
import importlib.metadata
import inspect
import sys
from pathlib import Path

import pytest

import vortex_rdf
from vortex_rdf import _native

STUB = Path(vortex_rdf.__file__).with_name("_native.pyi")


def _public(names):
    return {n for n in names if not n.startswith("_")}


def _stub_tree():
    return ast.parse(STUB.read_text(encoding="utf-8"), filename=str(STUB))


def _stub_classes(tree):
    """The stub's public classes and their methods; a private class is a
    typing helper (a `Protocol`), not a runtime name."""
    return {
        node.name: {
            item.name for item in node.body if isinstance(item, ast.FunctionDef)
        }
        for node in tree.body
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_")
    }


def _stub_module_names(tree):
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
    return names


def test_stub_names_match_runtime_and_all():
    tree = _stub_tree()
    stub_names = _stub_module_names(tree)
    runtime_names = _public(dir(_native))
    assert _public(stub_names) == runtime_names
    assert runtime_names == set(vortex_rdf.__all__) - {"__version__"}
    assert "__version__" in stub_names


def test_version_matches_distribution():
    assert vortex_rdf.__version__ == importlib.metadata.version("vortex-rdf")


_ARROW_PROTOCOL = ("__arrow_c_schema__", "__arrow_c_array__", "__arrow_c_stream__")


def _runtime_methods(cls):
    names = _public(dir(cls)) | {"__len__", "__repr__"}
    if sys.version_info >= (3, 12):
        names |= {n for n in ("__buffer__",) if hasattr(cls, n)}
    names |= {n for n in _ARROW_PROTOCOL if hasattr(cls, n)}
    return names


@pytest.mark.parametrize(
    "name", ["VortexRdfStore", "TermDict", "U32Column", "ArrowQuadStream"]
)
def test_stub_class_methods_match_runtime(name):
    stub_methods = _stub_classes(_stub_tree())[name]
    cls = getattr(_native, name)
    runtime = {n for n in _runtime_methods(cls) if hasattr(cls, n)}
    if "__init__" in stub_methods:
        runtime.add("__init__")
    if sys.version_info < (3, 12):
        stub_methods = stub_methods - {"__buffer__"}
    assert stub_methods == runtime


_NO_DEFAULT = inspect.Parameter.empty
_KEYWORD_ONLY = inspect.Parameter.KEYWORD_ONLY


def _stub_signature(func):
    """`(name, default, keyword_only)` per parameter, in declaration order."""
    args = func.args
    positional = args.posonlyargs + args.args
    defaults = [_NO_DEFAULT] * (len(positional) - len(args.defaults)) + list(args.defaults)
    params = []
    for arg, default in zip(positional, defaults):
        if arg.arg == "self":
            continue
        params.append(
            (arg.arg, _NO_DEFAULT if default is _NO_DEFAULT else ast.literal_eval(default), False)
        )
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        params.append(
            (arg.arg, _NO_DEFAULT if default is None else ast.literal_eval(default), True)
        )
    return params


def _runtime_signature(obj):
    params = []
    for p in inspect.signature(obj).parameters.values():
        if p.name == "self":
            continue
        params.append((p.name, p.default, p.kind is _KEYWORD_ONLY))
    return params


def _text_signature_pairs():
    tree = _stub_tree()
    pairs = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            pairs.append((node.name, node, getattr(_native, node.name)))
        elif isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            cls = getattr(_native, node.name)
            for item in node.body:
                if not isinstance(item, ast.FunctionDef):
                    continue
                if item.name == "__init__":
                    if cls.__text_signature__:
                        pairs.append((f"{node.name}.__init__", item, cls))
                    continue
                runtime = getattr(cls, item.name, None)
                if getattr(runtime, "__text_signature__", None):
                    pairs.append((f"{node.name}.{item.name}", item, runtime))
    return pairs


@pytest.mark.parametrize(
    "func", _text_signature_pairs(), ids=lambda pair: pair[0]
)
def test_stub_signatures_match_text_signature(func):
    _, stub_node, runtime = func
    expected = _stub_signature(stub_node)
    actual = _runtime_signature(runtime)
    assert [(n, k) for n, _, k in expected] == [(n, k) for n, _, k in actual]
    for (name, stub_default, _), (_, runtime_default, _) in zip(expected, actual):
        # A runtime default rendered as `...` has no literal spelling; the
        # stub may write any default for it.
        if runtime_default is Ellipsis:
            continue
        assert stub_default == runtime_default, name
