"""Render the dashboard page and run its script in Node against a tiny fake DOM.

The page's panels are built in the browser, so the only way to check what a
reader sees is to run its script. These tests render a page from small result
files, run that script with a fake `document`, and read the footprint panels'
HTML back.
"""

import json
import os
import re
import shutil
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent
RENDERER = SCRIPTS / "render_bench_dashboard.py"
sys.path.insert(0, str(SCRIPTS))

# Under CI a missing node fails these tests instead of skipping them.
pytestmark = pytest.mark.skipif(
    shutil.which("node") is None and os.environ.get("CI") != "true",
    reason="runs the page's script in Node",
)

RUST_LOG = """\
benchmark  fastest │ slowest │ median │ mean │ samples │ iters
╰─ decode_all  1 ms │ 2 ms │ 1.5 ms │ 1.5 ms │ 10 │ 10
#dataset {"quads":64,"subjects":6,"predicates":35,"objects":37,"graphs":11,"terms":89,"matched":{"S":6}}
"""

HOSTS = ("compare-footprint-panels", "js-memory-panels", "py-footprint-panels", "java-footprint-panels")

PAGE_RUNNER = r"""
const vm = require("vm");
const src = require("fs").readFileSync(0, "utf8");
function element(tag){
  const children = [];
  return {
    tag, className: "", innerHTML: "", textContent: "", dataset: {}, style: {}, children,
    classList: { toggle(){} },
    appendChild(c){ children.push(c); return c; },
    addEventListener(){}, setAttribute(){}, remove(){},
    querySelector: () => element("div"), querySelectorAll: () => [],
    get outerHTML(){
      const cls = this.className ? ` class="${this.className}"` : "";
      return `<${tag}${cls}>` + this.innerHTML + children.map(c => c.outerHTML).join("") + `</${tag}>`;
    },
  };
}
const byId = new Map();
const document = {
  getElementById(id){ if (!byId.has(id)) byId.set(id, element("div")); return byId.get(id); },
  createElement: element,
  querySelector: () => element("div"),
  querySelectorAll: () => [],
  documentElement: element("html"),
};
const context = {
  document,
  console: { warn(){}, log(){} },
  location: { hash: "" },
  history: { replaceState(){} },
  window: { addEventListener(){} },
};
vm.runInNewContext(src, context);
const out = {};
for (const host of %s) out[host] = document.getElementById(host).outerHTML;
console.log(JSON.stringify(out));
""" % json.dumps(list(HOSTS))


def render_page(out_dir: Path, **memory: list) -> str:
    """Render a page whose four tabs carry the given memory entries."""
    rust = out_dir / "rust.txt"
    rust.write_text(RUST_LOG, encoding="utf-8")
    flags = []
    for kw, name in (("rust_compare", "rust-compare"), ("js", "js"), ("python", "python"), ("java", "java")):
        if kw not in memory:
            continue
        path = out_dir / f"{name}.json"
        path.write_text(json.dumps({
            "results": [{"id": "ingest::vortex_dict", "median": "1 ms", "median_ns": 1e6}],
            "memory": memory[kw],
        }), encoding="utf-8")
        flags += [f"--{name}", str(path)]
    index = out_dir / "index.html"
    subprocess.run([sys.executable, str(RENDERER), str(rust), str(index), *flags], check=True)
    return index.read_text(encoding="utf-8")


class PanelParser(HTMLParser):
    """Reads a footprint host's HTML into one record per `div.panel`."""

    def __init__(self):
        super().__init__()
        self.panels = []
        self._stack = []  # (tag, classes) of open elements
        self._panel = None
        self._text = None  # (kind, list) being collected
        self._row = None
        self._cell = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        classes = (a.get("class") or "").split()
        self._stack.append((tag, classes))
        if tag == "div" and "panel" in classes:
            self._panel = {"title": "", "note": "", "head": [], "rows": {}}
            self.panels.append(self._panel)
        elif self._panel is None:
            return
        elif tag == "p" and "note" in classes:
            self._text = ("note", [])
        elif tag in ("h3", "th"):
            self._text = (tag, [])
        elif tag == "tr":
            self._row = {"label": None, "cells": []}
        elif tag == "td" and self._row is not None:
            if "rowh" in classes:
                self._text = ("rowh", [])
            else:
                self._cell = {"text": [], "best": "best" in classes, "ratio": None, "spread": None,
                              "title": a.get("title"), "span": 0}
        elif tag == "span" and self._cell is not None:
            self._cell["span"] += 1
            if "spread" in classes:
                self._cell["spread"] = a.get("title")
            if "ratio" in classes:
                self._text = ("ratio", [])

    def handle_endtag(self, tag):
        if self._stack:
            self._stack.pop()
        if self._panel is None:
            return
        if tag == "h3" and self._text and self._text[0] == "h3":
            self._panel["title"] = "".join(self._text[1])
            self._text = None
        elif tag == "p" and self._text and self._text[0] == "note":
            self._panel["note"] = "".join(self._text[1])
            self._text = None
        elif tag == "th" and self._text and self._text[0] == "th":
            self._panel["head"].append("".join(self._text[1]))
            self._text = None
        elif tag == "td" and self._text and self._text[0] == "rowh":
            self._row["label"] = "".join(self._text[1])
            self._text = None
        elif tag == "td" and self._cell is not None:
            c = self._cell
            self._row["cells"].append({"text": "".join(c["text"]), "best": c["best"], "ratio": c["ratio"],
                                       "spread": c["spread"], "title": c["title"]})
            self._cell = None
        elif tag == "span" and self._text and self._text[0] == "ratio":
            self._cell["ratio"] = "".join(self._text[1])
            self._text = None
            self._cell["span"] -= 1
        elif tag == "span" and self._cell is not None:
            self._cell["span"] -= 1
        elif tag == "tr" and self._row is not None:
            if self._row["label"] is not None:
                self._panel["rows"][self._row["label"]] = self._row["cells"]
            self._row = None

    def handle_data(self, data):
        if self._text is not None:
            self._text[1].append(data)
        elif self._cell is not None and self._cell["span"] == 0:
            self._cell["text"].append(data)


def footprint_panels(page: str) -> dict[str, list[dict]]:
    scripts = re.findall(r"<script>(.*?)</script>", page, re.S)
    run = subprocess.run(["node", "-e", PAGE_RUNNER], input=scripts[-1], capture_output=True, text=True, check=True)
    panels = {}
    for host, html in json.loads(run.stdout).items():
        parser = PanelParser()
        parser.feed(html)
        panels[host] = parser.panels
    return panels


def panel(panels, title):
    for p in panels:
        if p["title"] == title:
            return p
    raise AssertionError(f"no {title!r} panel among {[p['title'] for p in panels]}")


STORE, JS_STORE = ("storeMb", "peakAnonMb", "peakFileMb"), ("storeBuiltMb", "storeLoadedMb", "peakLiveMb")
BYREF_FILE = "vortex-rdf Dict+ByRef (file)"  # the page's own label for `vortex_dict_byref`


def mem(slug, obtained, *runs, keys=STORE):  # one run per tuple, figures in `keys` order
    return {"slug": slug, "label": slug, "role": "memory", "obtained": obtained,
            "runs": [dict(zip(keys, run)) for run in runs]}


def query(slug, peak, **extra):
    return {"slug": slug, "label": slug, "role": "query", "peakRssMb": peak, **extra}


PY_NEW = [query("vortex_dict", 900),
    mem("vortex_dict", "mapped", (0.2, 10.0, 40.0), (0.4, 12.0, 41.0), (0.3, 11.0, 39.0)),
    mem("vortex_dict_mem", "loaded", (16.4, 11.5, 0.0), (16.2, 12.5, 0.0), (16.9, 11.0, 0.0)),
    mem("pyoxigraph", "built", (120.0, 300.0, 0.0), (118.0, 310.0, 0.0), (125.0, 305.0, 0.0)),
    mem("pycottas", "mapped", (50.0, 500.0), keys=STORE[:2]),         # its run has no peakFileMb
    mem("rdflib", "built", (405.0, None, 0.0), (400.0, None, 0.0), (410.0, None, 0.0)),
    mem("vortex_dict_byref", None)]                                     # every memory run failed
JS_NEW = [mem("vortex_dict", "built", (23.3, 17.0, 31.0), (23.1, 17.2, 30.0), (23.5, 16.9, 32.0), keys=JS_STORE),
    mem("oxigraph", "built", (60.0, None, 900.0), keys=JS_STORE), mem("hdt", "loaded", (None, 40.0, 45.0), keys=JS_STORE)]
RC_NEW = [mem("vortex_dict", "mapped", (0.5, 8.0, 30.0)), mem("oxigraph_rocksdb", "opened", (50.0, 200.0, 10.0))]
JAVA_NEW = [query("vortex_dict", 700, retainedHeapMb=0.1, retainedNativeMb=16.0),
    mem("vortex_dict", "mapped", (0.4, 30.0, 60.0)), mem("jena_tim", "built", (800.0, 2000.0, 0.0))]


@pytest.fixture(scope="module")
def new_panels(tmp_path_factory):
    return footprint_panels(render_page(tmp_path_factory.mktemp("new"),
                                        rust_compare=RC_NEW, js=JS_NEW, python=PY_NEW, java=JAVA_NEW))


def test_store_cells_show_the_median_and_how_the_store_was_obtained(new_panels):
    rows = panel(new_panels["py-footprint-panels"], "Store memory")["rows"]
    file, inmem = rows["vortex-rdf Dict (file)"][0], rows["vortex-rdf Dict (memory)"][0]
    assert (file["text"], file["best"], file["ratio"]) == ("< 1 MB", True, None)
    assert file["title"] == "median of 3 runs, range 0.2–0.4 MB; store memory-mapped from its file"
    assert (inmem["text"], inmem["ratio"]) == ("16.4 MB", "16.4×")
    assert inmem["title"] == "median of 3 runs, range 16.2–16.9 MB; store loaded from a file into memory"
    assert rows["pyoxigraph (memory)"][0]["ratio"] == "120×"
    assert rows["pycottas (file)"][0]["title"] == "median of 1 run, range 50.0–50.0 MB; store memory-mapped from its file"


def test_a_file_and_memory_pair_within_run_to_run_spread_is_marked(new_panels):
    rows = panel(new_panels["py-footprint-panels"], "Store memory")["rows"]
    pair = [rows["vortex-rdf Dict (file)"], rows["vortex-rdf Dict (memory)"]]
    tip = ("within run-to-run spread: the file and in-memory medians differ by 0.5 MB, "
           "less than the wider min–max range (2.0 MB)")
    assert [cells[1]["spread"] for cells in pair] == [tip, tip]
    assert [cells[i]["spread"] for cells in pair for i in (0, 2)] == [None] * 4


def test_a_missing_figure_reads_a_dash_and_is_not_ranked(new_panels):
    rows = panel(new_panels["py-footprint-panels"], "Store memory")["rows"]
    dash = rows["rdflib (memory)"][1]                                   # null in every run
    assert (dash["text"], dash["best"], dash["ratio"]) == ("—", False, None)
    assert [label for label, cells in rows.items() if cells[1]["best"]] == ["vortex-rdf Dict (file)"]
    assert rows["pycottas (file)"][2]["text"] == "—"                    # key missing


def test_an_adapter_whose_memory_runs_all_failed_reads_dashes(new_panels):
    rows = panel(new_panels["py-footprint-panels"], "Store memory")["rows"]
    failed = rows[BYREF_FILE]
    assert [(c["text"], c["best"], c["ratio"]) for c in failed] == [("—", False, None)] * 3


def test_the_js_tab_has_a_store_column_per_method(new_panels):
    p = panel(new_panels["js-memory-panels"], "Store memory")
    assert p["head"] == ["Store", "Store: built from the triples", "Store: loaded from a file", "Peak: live memory"]
    vortex, oxigraph, hdt = (p["rows"][label] for label in ("Vortex Dict", "oxigraph", "hdt"))
    assert (oxigraph[1]["text"], oxigraph[1]["best"], hdt[0]["text"]) == ("—", False, "—")
    assert vortex[1]["best"] and vortex[1]["title"].endswith("; store loaded from a file into memory")
    assert vortex[2]["title"].endswith("; store built from the triples")
    assert hdt[2]["title"].endswith("; store loaded from a file into memory")
    assert all(cell["spread"] is None for cells in p["rows"].values() for cell in cells)


def test_every_tab_leads_its_footprint_with_the_store_panel(new_panels):
    for host, header in (("compare-footprint-panels", "Library"), ("js-memory-panels", "Store"),
                         ("py-footprint-panels", "Configuration"), ("java-footprint-panels", "Configuration")):
        assert (new_panels[host][0]["title"], new_panels[host][0]["head"][0]) == ("Store memory", header)
    for host in ("compare-footprint-panels", "py-footprint-panels", "java-footprint-panels"):
        assert new_panels[host][0]["head"][1:] == ["Store", "Peak: process memory", "Peak: mapped pages"]  # no Retained
    rocksdb = new_panels["compare-footprint-panels"][0]["rows"]["oxigraph (rocksdb)"][0]
    assert rocksdb["title"].endswith("; store opened from an on-disk database")


def test_the_note_marks_the_spread_sign_only_where_a_pair_exists(new_panels):
    assert "≈ marks a file / in-memory pair" in panel(new_panels["py-footprint-panels"], "Store memory")["note"]
    assert "≈" not in panel(new_panels["js-memory-panels"], "Store memory")["note"]


PY_OLD = [query("vortex_dict", 250), query("pyoxigraph", 900), query("rdflib", None)]
JAVA_OLD = JAVA_NEW[:1]          # a query entry with the retired retained fields


@pytest.fixture(scope="module")
def old_panels(tmp_path_factory):
    return footprint_panels(render_page(tmp_path_factory.mktemp("old"), python=PY_OLD, java=JAVA_OLD))


def test_a_results_file_without_memory_runs_keeps_the_query_peak_table(old_panels):
    py = panel(old_panels["py-footprint-panels"], "Query phase — peak RSS")
    assert py["head"] == ["Configuration", "Peak RSS"]
    assert list(py["rows"]) == ["vortex-rdf Dict (file)", "pyoxigraph (memory)", "rdflib (memory)"]
    assert [(c["text"], c["best"], c["ratio"]) for [c] in py["rows"].values()] == [
        ("250 MB", True, None), ("900 MB", False, "3.6×"), ("—", False, None)]
    assert panel(old_panels["java-footprint-panels"], "Query phase — peak RSS")["head"] == ["Configuration", "Peak RSS"]


def test_the_syntax_check_rejects_a_script_that_does_not_parse(tmp_path):
    import render_bench_dashboard as rbd
    out = tmp_path / "index.html"
    assert rbd.check_script_syntax("<script>const ok = 1;</script>", out) is True
    assert rbd.check_script_syntax("<script>const broken = ;</script>", out) is False  # fails if node is missing
