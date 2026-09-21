"""Pruebas adicionales: TestStream (late aceptado, duplicado tardío) y
comparación del pipeline batch contra el oráculo sobre el dataset completo."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from .test_assignment import load_events


def _ts(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _event(event_id: str, event_time: str, amount: int, merchant_id: str = "m-a") -> dict:
    return {
        "event_id": event_id,
        "merchant_id": merchant_id,
        "event_time": event_time,
        "amount": amount,
        "status": "CONFIRMED",
    }


def _pane_row(item, window=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
    merchant_id, total = item
    timing = {0: "EARLY", 1: "ON_TIME", 2: "LATE"}.get(pane.timing, "UNKNOWN")
    return (
        merchant_id,
        datetime.fromtimestamp(window.start.micros / 1_000_000, tz=UTC).isoformat(),
        total,
        timing,
    )


def _streaming_totals(solution, pipeline, stream, allowed_lateness_seconds: int):
    return (
        pipeline
        | stream
        | "Policy"
        >> solution.build_trigger_policy(
            window_seconds=60,
            allowed_lateness_seconds=allowed_lateness_seconds,
        )
        | "Key" >> beam.Map(lambda e: (e["merchant_id"], e))
        | "Dedup"
        >> beam.ParDo(
            solution.DeduplicatePayments(allowed_lateness_seconds=allowed_lateness_seconds)
        )
        | "Amount" >> beam.MapTuple(lambda k, e: (k, e["amount"]))
        | "Sum" >> beam.CombinePerKey(sum)
        | "Row" >> beam.Map(_pane_row)
    )


def test_test_stream_late_event_within_lateness_emits_late_pane(solution):
    """Un evento que llega después del cierre de la ventana pero dentro de la
    lateness produce un pane LATE acumulativo con el total revisado."""
    p001 = _event("p-001", "2026-07-24T13:00:05Z", 120_000)
    p004 = _event("p-004", "2026-07-24T13:00:42Z", 50_000)

    stream = (
        TestStream()
        .advance_watermark_to(_ts("2026-07-24T13:00:00Z"))
        .add_elements([TimestampedValue(p001, _ts(p001["event_time"]))])
        # El watermark cruza el fin de la ventana 13:00 → pane ON_TIME.
        .advance_watermark_to(_ts("2026-07-24T13:01:05Z"))
        # p-004 llega 35 s después del cierre: late, pero dentro de 120 s.
        .add_elements([TimestampedValue(p004, _ts(p004["event_time"]))])
        .advance_watermark_to(_ts("2026-07-24T13:03:30Z"))
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=PipelineOptions(streaming=True)) as pipeline:
        output = _streaming_totals(solution, pipeline, stream, 120)
        assert_that(
            output,
            equal_to(
                [
                    ("m-a", "2026-07-24T13:00:00+00:00", 120_000, "ON_TIME"),
                    ("m-a", "2026-07-24T13:00:00+00:00", 170_000, "LATE"),
                ]
            ),
        )


def test_test_stream_late_duplicate_does_not_emit_a_new_pane(solution):
    """Un duplicado que llega tarde es filtrado por el SetState: no genera
    ningún pane adicional ni altera el total."""
    p001 = _event("p-001", "2026-07-24T13:00:05Z", 120_000)

    stream = (
        TestStream()
        .advance_watermark_to(_ts("2026-07-24T13:00:00Z"))
        .add_elements([TimestampedValue(p001, _ts(p001["event_time"]))])
        .advance_watermark_to(_ts("2026-07-24T13:01:05Z"))
        .add_elements([TimestampedValue(dict(p001), _ts(p001["event_time"]))])
        .advance_watermark_to(_ts("2026-07-24T13:03:30Z"))
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=PipelineOptions(streaming=True)) as pipeline:
        output = _streaming_totals(solution, pipeline, stream, 120)
        assert_that(
            output,
            equal_to([("m-a", "2026-07-24T13:00:00+00:00", 120_000, "ON_TIME")]),
        )


def test_test_stream_event_beyond_lateness_is_dropped(solution):
    """Un evento que llega después de window_end + allowed_lateness es
    descartado por Beam: el total ON_TIME no se revisa."""
    p002 = _event("p-002", "2026-07-24T13:00:18Z", 80_000)
    p007 = _event("p-007", "2026-07-24T13:00:51Z", 30_000)

    stream = (
        TestStream()
        .advance_watermark_to(_ts("2026-07-24T13:00:00Z"))
        .add_elements([TimestampedValue(p002, _ts(p002["event_time"]))])
        .advance_watermark_to(_ts("2026-07-24T13:01:05Z"))
        # 13:03:40 > 13:01:00 + 120 s → fuera de tolerancia.
        .advance_watermark_to(_ts("2026-07-24T13:03:40Z"))
        .add_elements([TimestampedValue(p007, _ts(p007["event_time"]))])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=PipelineOptions(streaming=True)) as pipeline:
        output = _streaming_totals(solution, pipeline, stream, 120)
        assert_that(
            output,
            equal_to([("m-a", "2026-07-24T13:00:00+00:00", 80_000, "ON_TIME")]),
        )


def _oracle_rows(solution, events, **kwargs):
    oracle_totals, _ = solution.summarize_payments(events, **kwargs)
    return [
        {key: row[key] for key in ("merchant_id", "window_start", "window_end", "total")}
        for row in oracle_totals
    ]


def test_batch_pipeline_matches_oracle_with_lateness_policy(solution):
    """Con la política de lateness emulada, el pipeline batch descarta p-007
    igual que el oráculo por defecto."""
    events = load_events()
    expected = _oracle_rows(solution, events, allowed_lateness_seconds=120)

    with BeamTestPipeline() as pipeline:
        output = solution.build_windowed_totals_pipeline(
            pipeline, events, window_seconds=60, allowed_lateness_seconds=120
        )
        assert_that(output, equal_to(expected))


def test_batch_pipeline_without_policy_matches_oracle_with_infinite_lateness(solution):
    """Sin política, batch acepta todo (watermark en +∞): equivale al oráculo
    con lateness ilimitada, donde p-007 sí revisa el total de m-verde."""
    events = load_events()
    expected = _oracle_rows(solution, events, allowed_lateness_seconds=10**9)
    assert any(row["merchant_id"] == "m-verde" and row["total"] == 110_000 for row in expected)

    with BeamTestPipeline() as pipeline:
        output = solution.build_windowed_totals_pipeline(pipeline, events, window_seconds=60)
        assert_that(output, equal_to(expected))


def test_oracle_counts_on_default_configuration(solution):
    totals, audit = solution.summarize_payments(load_events())

    assert len(audit) == 9
    assert sum(1 for row in audit if row["accepted"]) == 5
    assert len(totals) == 4
    reasons = sorted(row["reason"] for row in audit)
    assert reasons == ["accepted"] * 5 + ["duplicate", "not_confirmed", "not_confirmed", "too_late"]


def test_parse_utc_rejects_invalid_values(solution):
    import pytest

    for bad in ("", "not-a-date", None, "2026-13-45T99:00:00Z"):
        with pytest.raises(ValueError):
            solution.parse_utc(bad)


def test_deduplicate_flag_can_be_disabled(solution):
    events = [
        _event("dup", "2026-07-24T13:00:05Z", 10),
        _event("dup", "2026-07-24T13:00:06Z", 10),
    ]
    for event in events:
        event["arrival_time"] = event["event_time"]

    with_dedup, _ = solution.summarize_payments(events)
    without_dedup, _ = solution.summarize_payments(events, deduplicate=False)

    assert with_dedup[0]["total"] == 10
    assert without_dedup[0]["total"] == 20


def test_upsert_sink_with_many_results(solution):
    results: list[dict[str, Any]] = [
        {
            "merchant_id": f"m-{i}",
            "window_start": "2026-07-24T13:00:00+00:00",
            "window_end": "2026-07-24T13:01:00+00:00",
            "total": i,
        }
        for i in range(4)
    ]
    materialized, audit = solution.simulate_sink_retries(results, attempts=2, idempotent=True)
    appended, append_audit = solution.simulate_sink_retries(
        results, attempts=2, idempotent=False
    )

    assert len(audit) == 8 and len(materialized) == 4
    assert len(append_audit) == 8 and len(appended) == 8
    assert {row["idempotency_key"] for row in materialized} == {
        f"m-{i}|2026-07-24T13:00:00+00:00" for i in range(4)
    }
