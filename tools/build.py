#!/usr/bin/env python3
"""Assemble the HTML partials under src/ into the published pages.

Public output (committed):
  docs/index.html    — standalone notes page for GitHub Pages
  dist/artifact.html — same body without the HTML skeleton, for publishing as an
                       Artifact, which supplies its own skeleton

Private output (gitignored):
  private/<name>.html — one standalone page per file in private/src/, sharing the
                        same stylesheet. Holds raw interview answers, grades and the
                        gap list, which stay off GitHub.

Adding a module: create src/modules/NN-name.html, add its entry to src/rail.html, and
run this script. Partials are assembled in filename order; files under src/answers/ are
appended after all modules.
"""
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

SKELETON_HEAD = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<style>
:root{color-scheme:light;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}
html{scroll-padding-top:env(safe-area-inset-top,0px)}
body{margin:0;padding:0;font:14px -apple-system,BlinkMacSystemFont,sans-serif;background:#faf9f5;color:#141413}
img{max-width:100%}
[hidden]:not([hidden="until-found"]){display:none!important}
</style>
"""


def read(path):
    return path.read_text(encoding="utf-8").rstrip() + "\n"


def standalone(head, body, foot):
    return SKELETON_HEAD + head + "</head>\n<body>\n" + body + "\n" + foot + "</body>\n</html>\n"


def main():
    head = read(SRC / "head.html")
    rail = read(SRC / "rail.html")
    foot = read(SRC / "foot.html")

    modules = sorted((SRC / "modules").glob("*.html"))
    if not modules:
        sys.exit("src/modules/ is empty — nothing to assemble")
    answers = sorted((SRC / "answers").glob("*.html")) if (SRC / "answers").is_dir() else []

    parts = [read(p) for p in modules + answers]
    shell = '<div class="shell">\n' + rail + "\n<main>\n" + "\n".join(parts) + "</main>\n</div>\n"

    (ROOT / "dist").mkdir(exist_ok=True)
    (ROOT / "dist" / "artifact.html").write_text(head + "\n" + shell + "\n" + foot, encoding="utf-8")

    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "index.html").write_text(standalone(head, shell, foot), encoding="utf-8")

    print(f"public: {len(modules)} module(s), {len(answers)} answer set(s)")
    for p in modules + answers:
        print(f"  - {p.relative_to(SRC)}")
    print("  -> docs/index.html")
    print("  -> dist/artifact.html")

    private_src = ROOT / "private" / "src"
    if private_src.is_dir():
        pages = sorted(private_src.glob("*.html"))
        for page in pages:
            body = '<main class="solo">\n' + read(page) + "</main>\n"
            (ROOT / "private" / page.name).write_text(standalone(head, body, foot), encoding="utf-8")
        if pages:
            print(f"private: {len(pages)} page(s), not committed")
            for page in pages:
                print(f"  -> private/{page.name}")


if __name__ == "__main__":
    main()
