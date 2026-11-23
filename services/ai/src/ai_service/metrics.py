"""Business metrics of the AI service (ADR 0013).

Components take an optional ``Instruments``; the default reports to the global
meter provider that ``setup_otel`` installs (a no-op one until then).
"""

from opentelemetry import metrics

# Seconds; LLM parses take from under a second (fake) to tens of seconds.
PARSE_BUCKETS = [0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0]


class Instruments:
    """ai.parse.duration, ai.match.score, ai.llm.tokens and dlq.messages."""

    def __init__(self, provider: metrics.MeterProvider | None = None) -> None:
        meter = (provider or metrics.get_meter_provider()).get_meter("ai_service")
        self._parse = meter.create_histogram(
            "ai.parse.duration",
            unit="s",
            description="Time to turn one uploaded CV into a parse outcome",
            explicit_bucket_boundaries_advisory=PARSE_BUCKETS,
        )
        self._score = meter.create_histogram(
            "ai.match.score", description="Scores computed by the matcher (0-100)"
        )
        self._tokens = meter.create_counter("ai.llm.tokens", description="LLM tokens used by CV parsing")
        self._dlq = meter.create_counter("dlq.messages", description="Records written to a dead-letter topic")

    def parse_duration(self, seconds: float, *, outcome: str) -> None:
        self._parse.record(seconds, {"outcome": outcome})

    def match_score(self, score: float) -> None:
        self._score.record(score)

    def llm_tokens(self, *, input_tokens: int, output_tokens: int) -> None:
        self._tokens.add(input_tokens, {"direction": "input"})
        self._tokens.add(output_tokens, {"direction": "output"})

    def dlq(self, topic: str) -> None:
        self._dlq.add(1, {"topic": topic})


_default: Instruments | None = None


def instruments() -> Instruments:
    """The process-wide instruments on the global provider (created on first use)."""
    global _default
    if _default is None:
        _default = Instruments()
    return _default
