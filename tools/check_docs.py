"""Check relative file links in current guides, excluding historical records."""

import re
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
GUIDES = [
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    ROOT / "KEYS.md",
    *[
        ROOT / p / "README.md"
        for p in ("atlas", "maia", "alkyone", "mcp", "dashboard", "tiered_storage")
    ],
    *ROOT.joinpath(".agents/skills").glob("*/SKILL.md"),
    *[
        ROOT / "docs" / f"{name}.md"
        for name in (
            "README",
            "architecture",
            "quickstart",
            "testing",
            "deploy",
            "contributing",
            "adaptive-scheduling",
            "tiered-storage",
            "knowledge-graph",
            "heartbeat",
            "resiliency-strategy",
            "micro-prefect-orchestration",
        )
    ],
    ROOT / "docs/implementation-checklists/cleanup-plan.md",
    ROOT / "docs/audits/OCTOBER_CLEANUP.md",
]


def main() -> int:
    errors = []
    for guide in GUIDES:
        if not guide.is_file():
            errors.append(f"Missing current guide: {guide.relative_to(ROOT)}")
            continue
        for destination in re.findall(r"\[[^\]]*\]\(([^\s)]+)\)", guide.read_text()):
            if destination.startswith(("https://", "http://", "#", "mailto:")):
                continue
            path = unquote(destination.split("#", 1)[0])
            if path and not (guide.parent / path).is_file():
                errors.append(f"{guide.relative_to(ROOT)}: missing link {destination}")
    if errors:
        print("\n".join(errors))
        return 1
    print(f"Checked relative links in {len(GUIDES)} current guides")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
