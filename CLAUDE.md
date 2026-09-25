# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and test

```bash
cmake -S . -B build            # configure (once)
cmake --build build             # build all targets
cd build && ctest --output-on-failure  # run all 3 test suites
```

Single test suite: `./build/test_test1` (EXERCISE requirements, 12 tests) or `./build/test_private_test1` (edge cases, 38 tests).

Sample feed: `./build/test1 market_inputs.csv` (stdout: CSV, stderr: audit trail). Queue modes: `--queue size` (default), `--queue time`, `--queue freshest` (or `--freshest`).

Requires C++26 (`-std=c++26`), Apple Clang 21+ or equivalent. No external dependencies.

## Architecture

A single-threaded pipeline that processes a market data CSV as a stream, computes `derived_value = base_rate + spread + adjustment` per instrument, and publishes updates through a bounded output queue to a rate-limited consumer.

**test1.h** — all library code, organized in five sections:

- **configuration** — compile-time limits (`DefaultLimits`, swappable as a template policy), hard timestamp bounds, queue capacity/staleness constants, `Options` for runtime consumer knobs.
- **parsing** — `parse_line()` turns a CSV line into an `Input` or a `ParseError`. Pure, no state. `kInputTypeNames` is the single source of truth for type name / enum mapping.
- **state** — `InstrumentState` holds three named `Component` fields and owns the derived value calculation, readiness check, and timestamp tracking. `Instrument` wraps it with `last_seen`, discard stats, and audit counters. `Update` carries a state snapshot through the queue. Publisher has no knowledge of individual components or the derived value formula.
- **output** — three queue classes, each a class template with compile-time capacity/staleness:
  - `SizeBoundedQueue<Capacity>` — `std::vector` min-heap, bounded by count, evicts oldest.
  - `TimeBoundedQueue<MaxStalenessMs, HardCap>` — `std::map`, evicts entries staler than a threshold vs the freshest.
  - `FreshestQueue` — `std::optional`, holds a single element, keeps only the newest.
- **pipeline** — `Publisher<Limits, Queue>` owns instrument state, validates timestamps, delegates readiness checks to `InstrumentState::ready()`, and manages the consumer (periodic delivery on feed time).

**test1.cpp** — program entry point (~100 lines). Reads the file, feeds lines to `Publisher::on_line()`, writes output CSV to stdout, audit to stderr.

## Key design decisions

- `Publisher` is a class template parameterized on `Limits` (timestamp thresholds) and `Queue` (output queue type). Tests swap in custom limit policies like `TightLimits`, `ExactLimits`, etc.
- `last_seen` updates on every input regardless of acceptance — this is intentional (resync after a gap costs one input, not one per instrument).
- Rejected lines don't create instrument slots — only accepted inputs allocate state.
- Incomplete instruments (missing a component type) never publish; instruments whose components go stale (older than `max_component_age`) are withheld via `InstrumentState::ready()`.
- Update timestamps use `InstrumentState::latest_timestamp()` (max across all components), not the triggering input's timestamp, so queue ordering reflects actual data freshness.
- The consumer runs on feed time so replays are deterministic.
- Known limitation: two far-future rejected inputs can advance `last_seen` and allow a subsequent input that jumps `feed_clock_`, potentially stalling the consumer until the feed catches up. A proper fix requires separating the "trusted feed clock" from `last_seen`.

## Style

- C++26, standard library only, no Boost.
- Braces on all control flow bodies, opening brace on the same line.
- Comments only for WHY, never WHAT.
- No `using namespace` in the header; chrono literals used only in .cpp files.
