# LineageIQ

LineageIQ is an evaluation-first BI-sprawl auditor. It combines a dbt project,
BI dashboard metadata, and warehouse query logs into a column-level lineage
graph, then reports duplicate, stale, orphaned, broken, and semantically
inconsistent assets with traceable evidence.

The full pipeline runs end to end today: `generate -> build -> audit -> eval`.
Deterministic parsing, the model- and column-level lineage graph, structural
defect checks, the semantic judge, and manifest-based scoring are all
implemented and covered by the numbers below.

## Results

These numbers are from an independent clean-room verification run of the
synthetic fixture (`synthetic/manifest.yaml`, seed `424242`), scored by
`lineageiq eval`.

- **7 of 7 planted defects caught.** `recall: 1.0`. Every defect D1-D7 is
  matched by defect type *and* manifest evidence location, including both
  semantic-judge catches: D1 (gross vs. net revenue formulas) and D6 (7-day
  vs. 30-day active-user windows), both scored as `metric_definition_conflict`.
- **0 of 5 healthy negative controls flagged.** `flagged_negative_ids: []`.
- **0 unmatched findings.** The auditor emitted exactly 7 findings
  (`finding_count: 7`) and all 7 matched a planted defect
  (`unmatched_finding_ids: []`). This is the stronger precision claim: zero
  spurious findings across the entire run, not just across the five
  look-alike negative controls.
- **88.1% column-level lineage coverage** (`coverage_pct: 88.0952380952381`).
- **Byte-identical across runs.** `audit.json` and the scorecard are
  `cmp`-identical across two clean runs. The lineage layer also carries a
  deterministic fingerprint that hashes topology, unresolved-edge causes, and
  coverage, so reproducibility is a checkable property, not just an
  observation. A live rebuild today confirms the model-level
  `graph_fingerprint` for the 73-node / 73-edge graph (10 sources, 30 models,
  33 tiles):
  `74c57581f422fe58fe914fc26a67de54c3043ca00a28e908b150540f3867cee5`

Two honesty caveats, stated plainly:

- The semantic stage in this run used the **deterministic reference backend**
  (`semantic_backend: "reference"`, `model: "deterministic-reference-v2"`),
  not a live model. This proves the plumbing and the schema/evidence contract
  end to end — candidate selection, prompt construction, strict response
  validation, caching — but it is not a live model's judgment. The OpenAI
  backend path in `lineageiq/agents/semantic_judge.py` is written but has
  never been executed.
- The judge's contract enforcement is tested
  (`test_rejects_non_json_and_non_verbatim_evidence` covers rejection of
  non-JSON responses and of findings that cite evidence not verbatim in the
  evidence packet), but that is 1 of only 2 tests in `test_semantic_judge.py`.
  The contract enforcement is tested; the live backend is written but unrun.

One more property worth stating because it is enforced by the type system,
not just by convention: every unresolved lineage edge is reason-coded.
`UnresolvedReason` (`lineageiq/parse/sql_lineage.py`) is a 6-value enum —
`missing_upstream_column`, `unknown_upstream_relation`, `ambiguous_column`,
`row_level_aggregation`, `unsupported_construct`, `parse_error` — and
`reason_code` is a required field on `UnresolvedLineageEdge`, so an unresolved
edge cannot be constructed without one.

## Architecture and contracts

### Acceptance gates

These rules define "done" for every feature:

1. Results must be scored against the planted-defect manifest. Passing unit
   tests alone is not validation.
2. Every LLM response is parsed through a strict runtime schema. Malformed,
   incomplete, or extra fields are rejected; prompt instructions are not
   treated as enforcement.
3. Long-running commands emit stage/item progress. Commands that can call an
   LLM also report request count, input/output tokens, and estimated cost as
   they run and in their final summary.

### Data flow

```text
synthetic inputs / real exports
          |
          v
deterministic parsers (dbt + dashboards + DuckDB query logs)
          |
          v
validated Models, Columns, Tiles, and Edges
          |
          v
NetworkX directed acyclic lineage graph
          |
          +----> deterministic structural checks
          |
          +----> deterministic semantic candidate selection
                            |
                            v
                    read-only evidence packet
                            |
                            v
                         LLM judge
                            |
                            v
                 strict schema validation
                            |
                            v
                    validated Findings
          |
          v
manifest-based evaluation (precision, recall, and per-defect results)
```

### Trust boundary

**Hard design rule: the LLM never writes to the graph, the config, or the
manifest. It only produces `Finding` values that deterministic code validates.**

Deterministic code is the sole owner of input parsing, graph construction and
mutation, configuration, the planted-defect manifest, candidate selection, and
evaluation. The LLM receives only a read-only evidence packet for semantic
comparisons and may return only proposed `Finding` values.

The LLM is never given a graph mutation API, filesystem write path,
configuration writer, or manifest writer. Its response is untrusted input:
code validates it with a schema that forbids unknown fields
(`SemanticJudgeResponse`), verifies every cited SQL quote is a verbatim,
differentiating substring of the candidate's SQL, and rejects responses whose
`candidate_id` doesn't match the requested candidate. Validated findings do
not mutate the graph.

### Core data contracts

All contracts are strict, immutable Pydantic models. Unknown fields are
rejected. Identifiers are stable strings owned by deterministic ingestion
code.

| Model | Contract |
| --- | --- |
| `Column` | A named column belonging to one model/source, with optional type and SQL expression plus source evidence. |
| `Model` | A dbt model/source relation and the stable IDs of its columns. |
| `Tile` | A BI tile/query, its dashboard, metric labels, referenced columns, and metadata evidence. |
| `Edge` | A directed, evidenced dependency between two stable asset IDs. |
| `Finding` | A defect type, affected asset IDs, at least one evidence pointer, detector kind, and confidence in `[0, 1]`. |

An `EvidencePointer` identifies an immutable observation using a source kind,
URI, locator, and optional content hash/excerpt. Findings cite observations
rather than free-form claims. Graph edges also carry evidence so
graph-derived findings can be traced back to an input artifact.

Defect types (`lineageiq/models.py`, `DefectType`):

- `duplicate_dashboard`
- `stale_asset`
- `orphaned_model`
- `metric_definition_conflict`
- `broken_lineage`
- `unused_column_propagation`

Structural defects (`duplicate_dashboard`, `stale_asset`, `orphaned_model`,
`broken_lineage`, `unused_column_propagation`) are produced entirely by
deterministic checks. The semantic judge is restricted, by a model validator
on `Finding`, to emitting only `metric_definition_conflict`: a deterministic
stage first selects tiles (or aggregate model columns) that share a
normalized metric label or column name and have different SQL, then supplies
their SQL and lineage evidence to the judge.

The unused-column propagation check is deliberately materiality-gated: it
emits a finding when one `SELECT *` model propagates at least 10 columns that
no downstream consumer reads. The threshold is an explicit function parameter;
the default catches the planted 12-column D7 case without classifying small
convenience projections as BI sprawl.

### Package boundaries

```text
synthetic/
  generate.py           # seeded generator + ground-truth verification
  dbt_project/          # generated 30-model dbt fixture
  dashboards.json       # generated Looker-style BI metadata
  query_logs/           # generated Parquet query logs
  manifest.yaml         # positives and healthy negative controls
lineageiq/
  models.py             # shared validated contracts
  parse/
    dbt.py               # strict Jinja ref/source resolution
    dashboards.py        # Looker-style JSON loader
    query_logs.py        # DuckDB-backed Parquet loader
    verify.py            # manifest-backed parser verification
    sql_lineage.py       # schema-aware SQLGlot column lineage
  graph/
    build.py             # deterministic source -> model -> tile DAG
    queries.py            # cycle/orphan checks and layer summaries
  agents/
    auditor.py            # audit orchestration: structural checks + semantic judge
    semantic_judge.py     # candidate selection, prompting, strict schema validation
    qa.py                 # read-only lineage Q&A (not yet implemented)
  evals/
    defect_manifest.json  # planted-defect ground truth placeholder
    scoring.py             # manifest-based precision/recall scoring
  cli.py                  # generate/build/audit/ask/eval command surface
```

## Command surface

```bash
lineageiq generate
lineageiq build
lineageiq audit [--semantic-backend {auto,openai,reference}] [--semantic-model MODEL]
lineageiq ask "Where does revenue come from?"
lineageiq eval
```

`generate`, `build`, `audit`, and `eval` are implemented and form the working
pipeline described above. `generate` verifies all planted positives and
negative controls before it succeeds. `audit` defaults `--semantic-backend`
to `auto`, which uses the `openai` backend when `OPENAI_API_KEY` is set and
otherwise falls back to the deterministic `reference` backend used for the
Results numbers above.

`ask` (read-only lineage Q&A) is not yet implemented; it currently exits with
a clear "not implemented" status rather than a stack trace.

## Setup

Requires **Python 3.11 or newer**. On many machines the system `python3` is
older (3.9 or 3.10) — a plain `python3 -m venv .venv` will silently produce
an environment that cannot install this project. Invoke `python3.11`
explicitly:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
pytest          # 18 tests
ruff check .
```

Then run the full pipeline:

```bash
lineageiq generate
lineageiq build
lineageiq audit --semantic-backend reference
lineageiq eval
```

`--semantic-backend reference` reproduces the Results numbers above without
an API key. Omit it (or pass `--semantic-backend openai` with
`OPENAI_API_KEY` set) to exercise the live-model path, which has not been
run as part of this project's verification to date.
