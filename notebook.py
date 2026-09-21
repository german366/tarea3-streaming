import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    from collections.abc import Iterable
    from datetime import datetime
    from pathlib import Path
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        Path,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        json,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.

    > **Nota de implementación.** Los tests compilan únicamente las
    > definiciones de la tarea con un namespace reducido (`beam`, `datetime`,
    > `Any`, etc.). Por eso las funciones importan localmente cualquier otro
    > símbolo que necesitan (`UTC`, `timedelta`, `trigger`, `Duration`).
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC.

        - Acepta el sufijo ``Z`` y offsets explícitos (``+00:00``, ``-03:00``).
        - Un valor sin zona horaria se interpreta como UTC.
        - Cualquier otro valor levanta ``ValueError`` con un mensaje claro.
        """
        from datetime import UTC

        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError(
                f"event_time inválido: se esperaba un string ISO-8601, recibí {raw_value!r}"
            )
        text = raw_value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as error:
            raise ValueError(f"event_time inválido: {raw_value!r} ({error})") from error
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    `parse_utc` produce siempre un `datetime` timezone-aware normalizado a UTC.
    Todo el resto del notebook (oráculo puro y pipeline Beam) usa esta función
    para construir el timestamp del dominio; nunca se usa `arrival_time` para
    asignar ventanas, solo para medir el atraso.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija.

        Las ventanas están alineadas al epoch Unix, igual que
        ``beam.window.FixedWindows``: ``start = floor(t / size) * size``.
        """
        from datetime import UTC, timedelta

        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser un entero positivo")
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        timestamp = timestamp.astimezone(UTC)
        epoch_seconds = int(timestamp.timestamp())
        start_seconds = epoch_seconds - (epoch_seconds % size_seconds)
        start = datetime.fromtimestamp(start_seconds, tz=UTC)
        return start, start + timedelta(seconds=size_seconds)

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar ``(totals, audit)``.

        Decisiones (en este orden) para cada evento:

        1. ``status != CONFIRMED`` → descartado, ``reason="not_confirmed"``.
        2. Llega después de ``window_end + allowed_lateness`` → ``too_late``.
           Este es el mismo criterio que usa Beam: la lateness se mide
           respecto al cierre de la ventana (watermark), no respecto al
           ``event_time``. ``delay_seconds`` sí se informa como
           ``arrival_time - event_time`` para que la auditoría sea legible.
        3. ``event_id`` ya visto en el mismo comercio → ``duplicate``.
        4. Aceptado. Si llegó después de ``window_end`` es una ``revision``
           (equivale a un pane LATE que corrige un total ya emitido).
        """
        from datetime import timedelta

        totals: dict[tuple[str, str], dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        lateness = timedelta(seconds=allowed_lateness_seconds)

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(event_time, window_seconds)
            delay_seconds = int((arrival_time - event_time).total_seconds())

            row: dict[str, Any] = {
                "event_id": event_id,
                "merchant_id": merchant_id,
                "status": event["status"],
                "event_time": event_time.isoformat(),
                "arrival_time": arrival_time.isoformat(),
                "window_start": window_start.isoformat(),
                "delay_seconds": delay_seconds,
                "duplicate": False,
                "too_late": False,
                "accepted": False,
                "revision": False,
                "reason": "",
            }

            if event["status"] != "CONFIRMED":
                row["reason"] = "not_confirmed"
                audit.append(row)
                continue

            if arrival_time > window_end + lateness:
                row["too_late"] = True
                row["reason"] = "too_late"
                audit.append(row)
                continue

            dedup_key = (merchant_id, event_id)
            if deduplicate and dedup_key in seen:
                row["duplicate"] = True
                row["reason"] = "duplicate"
                audit.append(row)
                continue
            seen.add(dedup_key)

            row["accepted"] = True
            row["revision"] = arrival_time > window_end
            row["reason"] = "accepted"
            audit.append(row)

            total_key = (merchant_id, window_start.isoformat())
            total = totals.setdefault(
                total_key,
                {
                    "merchant_id": merchant_id,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "total": 0,
                    "events": 0,
                    "revisions": 0,
                },
            )
            total["total"] += event["amount"]
            total["events"] += 1
            if row["revision"]:
                total["revisions"] += 1

        ordered_totals = [totals[key] for key in sorted(totals)]
        return ordered_totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    `summarize_payments` es el oráculo puro de Python contra el que se compara
    el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso (`delay_seconds`) se calcula con `arrival_time - event_time`;
    - la tolerancia se evalúa como en Beam: `arrival_time <= window_end +
      allowed_lateness`;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    La celda siguiente ejecuta el oráculo sobre `data/payments.jsonl` con la
    configuración por defecto.
    """)
    return


@app.cell
def _(Path, json):
    DATA_PATH = Path(__file__).parent / "data" / "payments.jsonl"

    def load_events(path: Path = DATA_PATH) -> list[dict]:
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    events = load_events()
    return (events,)


@app.cell
def _(events, mo, summarize_payments):
    oracle_totals, oracle_audit = summarize_payments(events)

    _accepted = sum(1 for row in oracle_audit if row["accepted"])
    _reasons = {}
    for _row in oracle_audit:
        _reasons[_row["reason"]] = _reasons.get(_row["reason"], 0) + 1

    mo.vstack(
        [
            mo.md(
                f"""
                ### Resultado del oráculo (ventana 60 s, lateness 120 s)

                | Métrica | Valor |
                |---|---:|
                | Eventos de entrada | {len(oracle_audit)} |
                | Eventos aceptados | {_accepted} |
                | Totales producidos | {len(oracle_totals)} |
                | Desglose por razón | `{_reasons}` |
                """
            ),
            mo.md("**Totales**"),
            mo.ui.table(oracle_totals, selection=None),
            mo.md("**Auditoría**"),
            mo.ui.table(oracle_audit, selection=None),
        ]
    )
    return (oracle_totals,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio.

        El estado es un ``SetState`` de ``event_id`` por (clave, ventana). Un
        timer de event time programado en ``window.end + allowed_lateness``
        limpia el conjunto: después de ese instante Beam ya no aceptará datos
        para la ventana, así que no hay nada más que deduplicar.

        Sin ese timer el conjunto de ids de cada comercio y ventana viviría
        para siempre en el backend de estado del runner: cada minuto agrega
        una ventana nueva y ninguna se libera, por lo que la memoria crece
        linealmente con el tiempo de ejecución aunque el tráfico sea constante.
        """

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            super().__init__()
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            from apache_beam.utils.timestamp import Duration

            _merchant_id, payload = element
            event_id = str(payload["event_id"])

            if event_id in set(seen_ids.read()):
                return

            seen_ids.add(event_id)
            expiry.set(window.end + Duration(seconds=self.allowed_lateness_seconds))
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, DeduplicatePayments, assign_fixed_window, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int | None = None,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Create → TimestampedValue(event_time) → Filter(CONFIRMED) →
        WindowInto(FixedWindows) → (merchant_id, evento) →
        DeduplicatePayments (estado por clave) → CombinePerKey(sum) →
        fila con límites de ventana tomados de WindowParam.

        En batch el watermark arranca en +∞, por lo que Beam nunca considera
        tardío a un elemento. ``allowed_lateness_seconds`` permite emular la
        política de streaming: si se indica, se descartan los eventos cuyo
        ``arrival_time`` supera ``window_end + allowed_lateness``, igual que
        haría el runner en streaming antes de que el dato toque el estado.
        """
        from datetime import UTC, datetime, timedelta

        def within_lateness(event: dict[str, Any]) -> bool:
            if allowed_lateness_seconds is None or "arrival_time" not in event:
                return True
            _start, end = assign_fixed_window(parse_utc(event["event_time"]), window_seconds)
            return parse_utc(event["arrival_time"]) <= end + timedelta(
                seconds=allowed_lateness_seconds
            )

        def with_event_time(event: dict[str, Any]):
            return beam.window.TimestampedValue(
                event, parse_utc(event["event_time"]).timestamp()
            )

        def to_row(item, window=beam.DoFn.WindowParam):
            merchant_id, total = item
            start = datetime.fromtimestamp(window.start.micros / 1_000_000, tz=UTC)
            end = datetime.fromtimestamp(window.end.micros / 1_000_000, tz=UTC)
            return {
                "merchant_id": merchant_id,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "total": total,
            }

        return (
            pipeline
            | "Create" >> beam.Create(events)
            | "EventTime" >> beam.Map(with_event_time)
            | "OnlyConfirmed" >> beam.Filter(lambda event: event["status"] == "CONFIRMED")
            | "WithinLateness" >> beam.Filter(within_lateness)
            | "FixedWindows" >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "KeyByMerchant" >> beam.Map(lambda event: (event["merchant_id"], event))
            | "Deduplicate" >> beam.ParDo(DeduplicatePayments())
            | "Amount" >> beam.MapTuple(lambda merchant_id, event: (merchant_id, event["amount"]))
            | "SumPerWindow" >> beam.CombinePerKey(sum)
            | "ToRow" >> beam.Map(to_row)
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        - pane ON_TIME cuando el watermark supera el fin de la ventana;
        - estimaciones EARLY cada 10 s de processing time mientras la ventana
          sigue abierta;
        - una revisión LATE por cada elemento que llegue dentro de la
          lateness permitida;
        - modo ACCUMULATING: cada pane trae el total completo, lo que permite
          escribirlo con UPSERT sobre la misma clave lógica.

        Nota: ``apache_beam.utils.timestamp.Duration`` no expone ``.seconds``.
        Para que las duraciones sean legibles (``policy.windowing.windowfn
        .size.seconds``) se usa una subclase mínima que solo agrega esa
        propiedad; el comportamiento del pipeline no cambia.
        """
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        class SecondsDuration(Duration):
            """Duration con acceso legible a los segundos enteros."""

            @property
            def seconds(self) -> int:
                return self.micros // 1_000_000

        return beam.WindowInto(
            beam.window.FixedWindows(SecondsDuration(seconds=window_seconds)),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(delay=10),
                late=trigger.AfterCount(1),
            ),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=SecondsDuration(seconds=allowed_lateness_seconds),
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    - `build_windowed_totals_pipeline` arma el pipeline batch completo,
      incluyendo el `ParDo` con estado. La clave es `merchant_id` **antes** de
      usar estado, porque Beam particiona el estado por (clave, ventana).
    - `DeduplicatePayments` guarda un `SetState` de `event_id` y programa un
      timer de event time en `window.end + allowed_lateness`.
    - `build_trigger_policy` define el contrato temporal para streaming.

    ### Expiración

    Sin timer, el estado de cada (comercio, ventana) nunca se libera. Como
    cada minuto crea ventanas nuevas y ninguna se borra, la memoria del
    runner crece linealmente con el tiempo de ejecución, incluso con tráfico
    constante. El timer en `window.end + allowed_lateness` es el último
    instante en el que Beam acepta datos para esa ventana: después de él no
    puede llegar ningún duplicado que necesitemos detectar.

    La celda siguiente ejecuta el pipeline batch sobre el dataset con
    `DirectRunner` y lo compara con el oráculo. En batch el watermark está
    en +∞ desde el inicio, así que Beam no descarta nada por lateness; por
    eso el pipeline recibe `allowed_lateness_seconds=120` para emular la
    política de streaming usando `arrival_time`.
    """)
    return


@app.cell
def _(beam, json):
    def collect_rows(build):
        """Ejecutar `build(pipeline)` y devolver las filas emitidas.

        El DirectRunner serializa las funciones del pipeline, por lo que un
        `list.append` capturado en un closure escribiría en una copia. Los
        elementos se vuelcan a un archivo temporal en JSON Lines y se leen al
        terminar, lo que funciona igual en batch y con `TestStream`.
        """
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            sink_path = Path(tmp) / "rows.jsonl"

            def dump(row):
                with sink_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(row, default=str) + "\n")

            with beam.Pipeline(options=build.options) as pipeline:
                _ = build(pipeline) | "CollectRows" >> beam.Map(dump)

            if not sink_path.exists():
                return []
            return [
                json.loads(line)
                for line in sink_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

    return (collect_rows,)


@app.cell
def _(build_windowed_totals_pipeline, collect_rows, events, mo, oracle_totals):
    def _batch_build(pipeline):
        return build_windowed_totals_pipeline(pipeline, events, allowed_lateness_seconds=120)

    _batch_build.options = None
    beam_totals = sorted(
        collect_rows(_batch_build), key=lambda row: (row["merchant_id"], row["window_start"])
    )
    _oracle_view = [
        {key: row[key] for key in ("merchant_id", "window_start", "window_end", "total")}
        for row in oracle_totals
    ]
    _matches = beam_totals == _oracle_view

    mo.vstack(
        [
            mo.md(
                "### Totales del pipeline Beam (DirectRunner, batch)\n\n"
                + (
                    "✅ Coinciden con el oráculo puro."
                    if _matches
                    else "❌ Difieren del oráculo puro."
                )
            ),
            mo.ui.table(beam_totals, selection=None),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Evidencia en streaming: `TestStream` y un late aceptado

    La celda siguiente reproduce con `TestStream` la historia del comercio
    `m-azul` en la ventana `13:00`:

    1. llega `p-001` (120 000) y el watermark cruza `13:01:00` → pane
       **ON_TIME** con 120 000;
    2. llega `p-004` (50 000, `event_time` 13:00:42) cuando el watermark ya
       está en `13:01:35` → pane **LATE** acumulativo con 170 000;
    3. se reenvía `p-004` duplicado → el `SetState` lo descarta, no hay pane;
    4. el watermark cruza `13:03:00` (= fin + lateness) → dispara el timer de
       expiración; un `p-007`-like que llegara después sería descartado por
       Beam antes de tocar el estado.
    """)
    return


@app.cell
def _(DeduplicatePayments, beam, build_trigger_policy, collect_rows, mo, parse_utc):
    def _run_test_stream() -> list[dict]:
        from datetime import UTC, datetime

        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.testing.test_stream import TestStream
        from apache_beam.transforms.window import TimestampedValue

        def ts(text: str) -> float:
            return parse_utc(text).timestamp()

        def event(event_id: str, event_time: str, amount: int) -> dict:
            return {
                "event_id": event_id,
                "merchant_id": "m-azul",
                "event_time": event_time,
                "amount": amount,
                "status": "CONFIRMED",
            }

        p001 = event("p-001", "2026-07-24T13:00:05Z", 120_000)
        p004 = event("p-004", "2026-07-24T13:00:42Z", 50_000)

        stream = (
            TestStream()
            .advance_watermark_to(ts("2026-07-24T13:00:00Z"))
            .add_elements([TimestampedValue(p001, ts(p001["event_time"]))])
            .advance_watermark_to(ts("2026-07-24T13:01:05Z"))
            .add_elements([TimestampedValue(p004, ts(p004["event_time"]))])
            .advance_watermark_to(ts("2026-07-24T13:01:40Z"))
            .add_elements([TimestampedValue(dict(p004), ts(p004["event_time"]))])
            .advance_watermark_to(ts("2026-07-24T13:03:30Z"))
            .advance_watermark_to_infinity()
        )

        def to_row(item, window=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
            merchant_id, total = item
            timing = {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}[pane.timing]
            return {
                "merchant_id": merchant_id,
                "window_start": datetime.fromtimestamp(
                    window.start.micros / 1_000_000, tz=UTC
                ).isoformat(),
                "total": total,
                "pane_timing": timing,
                "pane_index": pane.index,
                "is_last": pane.is_last,
            }

        def build(pipeline):
            return (
                pipeline
                | stream
                | "Policy" >> build_trigger_policy(window_seconds=60, allowed_lateness_seconds=120)
                | "Key" >> beam.Map(lambda e: (e["merchant_id"], e))
                | "Dedup" >> beam.ParDo(DeduplicatePayments(allowed_lateness_seconds=120))
                | "Amount" >> beam.MapTuple(lambda k, e: (k, e["amount"]))
                | "Sum" >> beam.CombinePerKey(sum)
                | "Row" >> beam.Map(to_row)
            )

        build.options = PipelineOptions(streaming=True)
        return sorted(collect_rows(build), key=lambda row: row["pane_index"])

    stream_panes = _run_test_stream()
    mo.vstack(
        [
            mo.md("### Panes emitidos por el `TestStream`"),
            mo.ui.table(stream_panes, selection=None),
        ]
    )
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        missing = [key for key in ("merchant_id", "window_start") if key not in result]
        if missing:
            raise KeyError(f"El resultado no tiene las claves {missing}")
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts < 1:
            raise ValueError("attempts debe ser >= 1")

        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        operation = "UPSERT" if idempotent else "POST"

        for attempt in range(1, attempts + 1):
            for result in results:
                key = make_idempotency_key(result)
                row = {**result, "idempotency_key": key}
                if idempotent:
                    upsert_sink[key] = row
                else:
                    append_sink.append(row)
                audit.append(
                    {
                        "attempt": attempt,
                        "operation": operation,
                        "idempotency_key": key,
                        "merchant_id": result["merchant_id"],
                        "window_start": result["window_start"],
                        "total": result.get("total"),
                        "materialized_rows": (
                            len(upsert_sink) if idempotent else len(append_sink)
                        ),
                    }
                )

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    Con panes acumulativos, cada pane (EARLY, ON_TIME, LATE) trae el total
    completo de la ventana, así que un UPSERT por `merchant_id|window_start`
    converge siempre al último valor correcto aunque el runner reintente la
    escritura o llegue una revisión tardía.
    """)
    return


@app.cell
def _(make_idempotency_key, mo, oracle_totals, simulate_sink_retries):
    _results = [
        {key: row[key] for key in ("merchant_id", "window_start", "window_end", "total")}
        for row in oracle_totals
    ]
    upsert_rows, upsert_audit = simulate_sink_retries(_results, attempts=2, idempotent=True)
    append_rows, append_audit = simulate_sink_retries(_results, attempts=2, idempotent=False)

    mo.vstack(
        [
            mo.md(
                f"""
                ### Reintentos del sink ({len(_results)} resultados × 2 intentos)

                | Modo | Intentos auditados | Filas materializadas |
                |---|---:|---:|
                | `UPSERT` idempotente | {len(upsert_audit)} | {len(upsert_rows)} |
                | `POST` append-only | {len(append_audit)} | {len(append_rows)} |

                Claves idempotentes: `{[make_idempotency_key(r) for r in _results]}`
                """
            ),
            mo.md("**Materializado (UPSERT)**"),
            mo.ui.table(upsert_rows, selection=None),
            mo.md("**Auditoría (UPSERT)**"),
            mo.ui.table(upsert_audit, selection=None),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Pruebas

    ```bash
    uv run pytest
    ```

    Garantías cubiertas por `tests/test_assignment.py` (provisto) y
    `tests/test_streaming.py` (agregado):

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde;
    - [x] con `TestStream`, un late dentro de la tolerancia produce un pane
      LATE acumulativo y un duplicado tardío no lo altera;
    - [x] el pipeline batch coincide con el oráculo sobre el dataset completo.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Ver `README.md` para instrucciones con Docker o `uv`, decisiones de
    diseño y trade-offs.
    """)
    return


if __name__ == "__main__":
    app.run()
