#!/usr/bin/env python3

import argparse
import json
import re
from pathlib import Path


RUST_FN_PATTERN = re.compile(
    r"""
    (?P<attrs>(?:\s*\#\[[^\]]*\]\s*)*)                  # optional attributes
    (?P<visibility>pub(?:\([^)]*\))?\s+)?               # pub, pub(crate), pub(super), etc.
    (?P<qualifiers>
        (?:
            async\s+|
            const\s+|
            unsafe\s+|
            extern\s+"[^"]+"\s+|
            extern\s+
        )*
    )
    fn\s+
    (?P<name>[A-Za-z_][A-Za-z0-9_]*)                    # function name
    \s*
    (?P<rest>[\s\S]*?)                                  # generics, args, return type, where clause
    (?=\{|;)                                            # stop before body or semicolon
    """,
    re.VERBOSE,
)


def strip_comments(source: str) -> str:
    """
    Removes Rust line comments and block comments.

    This is intentionally lightweight and not a full Rust lexer.
    """
    source = re.sub(r"//.*", "", source)
    source = re.sub(r"/\*[\s\S]*?\*/", "", source)
    return source


def normalize_signature(signature: str) -> str:
    return " ".join(signature.split())


def extract_rust_functions(source: str):
    clean_source = strip_comments(source)
    functions = []

    for match in RUST_FN_PATTERN.finditer(clean_source):
        name = match.group("name")

        raw_signature = clean_source[match.start():match.end()]
        signature = normalize_signature(raw_signature)

        line_number = clean_source[:match.start()].count("\n") + 1

        functions.append(
            {
                "name": name,
                "signature": signature,
                "line": line_number,
            }
        )

    return functions


def crawl_rust_project(project_root: Path):
    target_dirs = ["core", "cli"]
    files = []

    for target_dir in target_dirs:
        folder = project_root / target_dir

        if not folder.exists():
            continue

        for rs_file in sorted(folder.rglob("*.rs")):
            try:
                source = rs_file.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                source = rs_file.read_text(encoding="utf-8", errors="replace")

            functions = extract_rust_functions(source)

            files.append(
                {
                    "path": str(rs_file.relative_to(project_root)),
                    "absolute_path": str(rs_file.resolve()),
                    "function_count": len(functions),
                    "functions": functions,
                }
            )

    return {
        "language": "rust",
        "project_root": str(project_root.resolve()),
        "scanned_folders": target_dirs,
        "file_count": len(files),
        "files": files,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Crawl Rust core/ and cli/ folders and extract function signatures."
    )
    parser.add_argument(
        "project_root",
        nargs="?",
        default=".",
        help="Path to the Rust project root. Default: current directory.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="rust_functions_report.json",
        help="Output JSON file. Default: rust_functions_report.json",
    )

    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    report = crawl_rust_project(project_root)

    output_path = Path(args.output)
    output_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"Rust function report written to: {output_path.resolve()}")


if __name__ == "__main__":
    main()