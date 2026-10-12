#!/usr/bin/env python3
"""Assemble the partials in src/ into two outputs:

  docs/index.html    — standalone page for GitHub Pages (full doctype/head/body)
  dist/artifact.html — page body only, for publishing as a Claude Artifact, which
                       wraps the content in its own HTML skeleton

To add a module: create src/modules/NN-name.html and run this script again.
Partials are assembled in filename order.
"""
import pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

SKELETON_HEAD = """<!doctype html>
<html lang="vi">
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

def read(p):
    return p.read_text(encoding="utf-8").rstrip() + "\n"

def main():
    modules = sorted((SRC / "modules").glob("*.html"))
    if not modules:
        sys.exit("src/modules/ is empty — nothing to assemble")

    head = read(SRC / "head.html")
    rail = read(SRC / "rail.html")
    foot = read(SRC / "foot.html")
    main_html = "\n".join(read(m) for m in modules)

    body = f'{rail}\n<main>\n{main_html}</main>\n'
    shell = f'<div class="shell">\n{body}</div>\n'

    # 1) artifact body: no HTML skeleton
    (ROOT / "dist").mkdir(exist_ok=True)
    (ROOT / "dist" / "artifact.html").write_text(head + "\n" + shell + "\n" + foot, encoding="utf-8")

    # 2) standalone page for GitHub Pages
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "index.html").write_text(
        SKELETON_HEAD + head + "</head>\n<body>\n" + shell + "\n" + foot + "</body>\n</html>\n",
        encoding="utf-8",
    )

    print(f"built {len(modules)} module(s):")
    for m in modules:
        print(f"  - {m.name}")
    print("  -> docs/index.html (GitHub Pages)")
    print("  -> dist/artifact.html (Claude Artifact)")

if __name__ == "__main__":
    main()
