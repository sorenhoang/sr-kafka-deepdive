#!/usr/bin/env python3
"""Assemble the sources under src/ into the published pages and the quiz.

Public output (committed):
  docs/index.html    — standalone notes page for GitHub Pages
  dist/artifact.html — same body without the HTML skeleton, for publishing as an
                       Artifact, which supplies its own skeleton
  QUIZ.md            — every question from every module, Junior to Senior, with the
                       answers collapsed behind <details> toggles

Private output (gitignored):
  private/<name>.html — one standalone page per file in private/src/, sharing the
                        same stylesheet. Holds raw interview answers, grades and the
                        gap list, which stay off GitHub.

Questions have one source of truth: src/quiz/NN-name.json. Each module page carries a
`<!--QUIZ:NN-name-->` placeholder where the checklist is injected, and the same JSON
produces that module's section of QUIZ.md, so the two can never drift apart.

Adding a module: create src/modules/NN-name.html with a QUIZ placeholder, the matching
src/quiz/NN-name.json, add an entry to src/rail.html, and run this script. Files are
assembled in filename order; src/answers/ is appended after all modules.
"""
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
LEVELS = ("Junior", "Mid", "Senior")

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

QUIZ_PREAMBLE = """# Quiz — every module

Every question from every module, Junior through Senior. Answers are collapsed: open one
only after you have answered it out loud, otherwise the recall practice is spent.

Generated from `src/quiz/*.json` by `tools/build.py` — edit the JSON, not this file.

"""


def read(path):
    return path.read_text(encoding="utf-8").rstrip() + "\n"


def load_quizzes():
    d = SRC / "quiz"
    if not d.is_dir():
        return []
    out = []
    for path in sorted(d.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        data["slug"] = path.stem
        for q in data["questions"]:
            if q["level"] not in LEVELS:
                sys.exit(f"{path.name}: unknown level {q['level']!r}")
        out.append(data)
    return out


def checklist_html(quiz):
    rows = []
    for i, q in enumerate(quiz["questions"], start=1):
        cls = " hard" if q["level"] == "Senior" else ""
        label = f"{q['level']} · write it out" if q["level"] == "Senior" else q["level"]
        checked = " checked" if q.get("done") else ""
        rows.append(
            f'    <li><label><input type="checkbox" data-q="{i}"{checked}>'
            f'<span class="qtext"><span class="lvl{cls}">{label}</span>{q["q"]}</span></label></li>'
        )
    done = sum(1 for q in quiz["questions"] if q.get("done"))
    return (
        f'  <h2 id="{quiz["anchor"]}">Interview questions — module {quiz["module"][1:]}</h2>\n'
        f'  <p>Tick what you can answer fluently. The senior tier gets written out and graded; '
        f'every question also appears in <a href="https://github.com/sorenhoang/sr-kafka-deepdive/blob/main/QUIZ.md">QUIZ.md</a> with a collapsed answer.</p>\n'
        f'  <ul class="qlist" data-store="{quiz["store"]}">\n'
        + "\n".join(rows)
        + f'\n  </ul>\n  <p class="qmeta" data-count-for="{quiz["store"]}">{done} / {len(quiz["questions"])}</p>\n'
    )


def quiz_markdown(quizzes):
    parts = [QUIZ_PREAMBLE]
    for quiz in quizzes:
        parts.append(f'## {quiz["module"]} · {quiz["title"]}\n')
        for level in LEVELS:
            picked = [(i, q) for i, q in enumerate(quiz["questions"], start=1) if q["level"] == level]
            if not picked:
                continue
            parts.append(f"### {level}\n")
            for i, q in picked:
                parts.append(f'**{quiz["module"]}.{i}** — {q["q"]}\n')
                parts.append("<details>\n<summary>Answer</summary>\n\n" + q["a"] + "\n\n</details>\n")
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def standalone(head, body, foot):
    return SKELETON_HEAD + head + "</head>\n<body>\n" + body + "\n" + foot + "</body>\n</html>\n"


def main():
    head = read(SRC / "head.html")
    rail = read(SRC / "rail.html")
    foot = read(SRC / "foot.html")
    quizzes = {q["slug"]: q for q in load_quizzes()}

    modules = sorted((SRC / "modules").glob("*.html"))
    if not modules:
        sys.exit("src/modules/ is empty — nothing to assemble")
    answers = sorted((SRC / "answers").glob("*.html")) if (SRC / "answers").is_dir() else []

    parts = []
    used = set()
    for path in modules:
        text = read(path)
        for slug in re.findall(r"<!--QUIZ:([A-Za-z0-9_-]+)-->", text):
            if slug not in quizzes:
                sys.exit(f"{path.name}: no src/quiz/{slug}.json for placeholder {slug}")
            text = text.replace(f"<!--QUIZ:{slug}-->", checklist_html(quizzes[slug]))
            used.add(slug)
        parts.append(text)
    parts += [read(p) for p in answers]

    unused = set(quizzes) - used
    if unused:
        sys.exit(f"quiz files with no placeholder in any module: {sorted(unused)}")

    shell = '<div class="shell">\n' + rail + "\n<main>\n" + "\n".join(parts) + "</main>\n</div>\n"

    (ROOT / "dist").mkdir(exist_ok=True)
    (ROOT / "dist" / "artifact.html").write_text(head + "\n" + shell + "\n" + foot, encoding="utf-8")
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "index.html").write_text(standalone(head, shell, foot), encoding="utf-8")

    ordered = [quizzes[s] for s in sorted(quizzes)]
    (ROOT / "QUIZ.md").write_text(quiz_markdown(ordered), encoding="utf-8")

    total = sum(len(q["questions"]) for q in ordered)
    print(f"public: {len(modules)} module(s), {len(answers)} answer set(s)")
    for p in modules + answers:
        print(f"  - {p.relative_to(SRC)}")
    print("  -> docs/index.html")
    print("  -> dist/artifact.html")
    print(f"  -> QUIZ.md ({len(ordered)} module(s), {total} questions)")

    private_src = ROOT / "private" / "src"
    if private_src.is_dir():
        pages = sorted(private_src.glob("*.html"))
        for page in pages:
            body = '<main class="solo">\n' + read(page) + "</main>\n"
            (ROOT / "private" / page.name).write_text(standalone(head, body, foot), encoding="utf-8")
        if pages:
            print(f"private: {len(pages)} page(s), not committed")


if __name__ == "__main__":
    main()
