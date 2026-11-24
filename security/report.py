"""Tiny shared helper: each attack records one result, and the script writes them to JSON and prints a table."""
import json
import sys
from pathlib import Path

RESULTS: list[dict] = []


def record(rid: str, title: str, severity: str, defended: bool, evidence: str) -> None:
    RESULTS.append({"id": rid, "title": title, "severity": severity,
                    "result": "DEFENDED" if defended else "VULNERABLE", "evidence": evidence})
    print(f'{"ok  " if defended else "FAIL"} {rid:<7} [{severity:<6}] {title}  ->  {evidence}')


def finish(name: str) -> None:
    out = Path(__file__).parent / "results" / f"{sys.argv[1] if len(sys.argv) > 1 else 'latest'}-{name}.json"
    out.write_text(json.dumps(RESULTS, indent=2))
    bad = [r for r in RESULTS if r["result"] == "VULNERABLE"]
    print(f"\n{name}: {len(RESULTS) - len(bad)} defended, {len(bad)} vulnerable  (saved {out.name})")
    sys.exit(1 if bad else 0)