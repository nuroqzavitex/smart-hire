"""Parses one PDF with the real AnthropicLlmParser and prints the outcome as JSON.

Run by hand only: needs Anthropic credentials, calls the paid API, never runs in CI.

    uv run python scripts/try_anthropic.py path/to/cv.pdf [model]
"""

import dataclasses
import json
import os
import sys
from pathlib import Path

import anthropic

from ai_service.adapters.llm import AnthropicLlmParser, has_anthropic_credentials
from ai_service.adapters.pdf import PyMuPdfExtractor
from ai_service.domain.models import ParseSuccess
from ai_service.services.cv_parser import parse_cv


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2
    if not has_anthropic_credentials(os.environ, Path.home()):
        print("no Anthropic credentials (set ANTHROPIC_API_KEY or run `ant auth login`)", file=sys.stderr)
        return 1
    model = argv[2] if len(argv) == 3 else "claude-opus-5-5"
    llm = AnthropicLlmParser(anthropic.Anthropic(timeout=60, max_retries=2), model)
    outcome = parse_cv(Path(argv[1]).read_bytes(), PyMuPdfExtractor(), llm)
    result = dataclasses.asdict(outcome)
    if isinstance(outcome, ParseSuccess):
        result["parsed"] = outcome.parsed.model_dump()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
