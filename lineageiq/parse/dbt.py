"""Deterministic dbt project loading and restricted Jinja dependency resolution."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from lineageiq.models import (
    EvidencePointer,
    EvidenceSource,
    Model,
    StrictModel,
)

ProgressCallback = Callable[[str], None]

_REF_PATTERN = re.compile(
    r"\{\{\s*ref\s*\(\s*(?P<quote>['\"])(?P<name>[A-Za-z0-9_.-]+)"
    r"(?P=quote)\s*\)\s*\}\}"
)
_SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\s*\(\s*(?P<source_quote>['\"])"
    r"(?P<source>[A-Za-z0-9_.-]+)(?P=source_quote)\s*,\s*"
    r"(?P<table_quote>['\"])(?P<table>[A-Za-z0-9_.-]+)"
    r"(?P=table_quote)\s*\)\s*\}\}"
)


class DbtParseError(ValueError):
    """Raised when a dbt project violates the deterministic parser contract."""


class _YamlModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _SourceColumnYaml(_YamlModel):
    name: str = Field(min_length=1)


class _SourceTableYaml(_YamlModel):
    name: str = Field(min_length=1)
    columns: tuple[_SourceColumnYaml, ...] = ()


class _SourceYaml(_YamlModel):
    name: str = Field(min_length=1)
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    tables: tuple[_SourceTableYaml, ...]


class _SourcesFileYaml(_YamlModel):
    version: int
    sources: tuple[_SourceYaml, ...]


class DeclaredSource(StrictModel):
    """A source table declared in dbt YAML."""

    id: str
    source_name: str
    table_name: str
    database: str | None = None
    schema_name: str | None = None
    relation_name: str
    columns: tuple[str, ...]
    path: str
    evidence: tuple[EvidencePointer, ...]


class ParsedDbtModel(StrictModel):
    """A dbt model with restricted Jinja resolved to deterministic relations."""

    asset: Model
    layer: str
    relation_name: str
    raw_sql: str
    resolved_sql: str
    refs: tuple[str, ...]
    sources: tuple[str, ...]

    @property
    def name(self) -> str:
        return self.asset.name


class ParsedDbtProject(StrictModel):
    """Validated parser output; it does not contain or mutate a graph."""

    name: str
    root: str
    models: tuple[ParsedDbtModel, ...]
    sources: tuple[DeclaredSource, ...]

    def model(self, name: str) -> ParsedDbtModel:
        matches = [model for model in self.models if model.name == name]
        if len(matches) != 1:
            raise KeyError(f"expected one dbt model named {name!r}, found {len(matches)}")
        return matches[0]

    def source(self, source_name: str, table_name: str) -> DeclaredSource:
        matches = [
            source
            for source in self.sources
            if source.source_name == source_name and source.table_name == table_name
        ]
        if len(matches) != 1:
            raise KeyError(
                f"expected one dbt source {source_name}.{table_name}, found {len(matches)}"
            )
        return matches[0]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def _load_yaml(path: Path) -> Any:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise DbtParseError(f"cannot read dbt YAML {path}: {exc}") from exc


def _relation_name(*parts: str | None) -> str:
    return ".".join(part for part in parts if part)


def _load_sources(model_roots: tuple[Path, ...]) -> tuple[DeclaredSource, ...]:
    declared: list[DeclaredSource] = []
    seen: set[tuple[str, str]] = set()
    yaml_paths = sorted(
        path
        for model_root in model_roots
        for path in (*model_root.rglob("*.yml"), *model_root.rglob("*.yaml"))
    )
    for path in yaml_paths:
        raw = _load_yaml(path)
        if not isinstance(raw, dict) or "sources" not in raw:
            continue
        try:
            source_file = _SourcesFileYaml.model_validate(raw)
        except ValueError as exc:
            raise DbtParseError(f"invalid source declaration {path}: {exc}") from exc
        for source in source_file.sources:
            for table in source.tables:
                key = (source.name, table.name)
                if key in seen:
                    raise DbtParseError(f"duplicate dbt source {source.name}.{table.name}")
                seen.add(key)
                columns = tuple(column.name for column in table.columns)
                if len(columns) != len(set(columns)):
                    raise DbtParseError(f"duplicate column in source {source.name}.{table.name}")
                locator = f"source={source.name}.{table.name}"
                evidence = EvidencePointer(
                    source=EvidenceSource.DBT_METADATA,
                    uri=path.as_posix(),
                    locator=locator,
                    content_hash=_sha256(path.read_text(encoding="utf-8")),
                )
                declared.append(
                    DeclaredSource(
                        id=f"source.{source.name}.{table.name}",
                        source_name=source.name,
                        table_name=table.name,
                        database=source.database,
                        schema_name=source.schema_name,
                        relation_name=_relation_name(
                            source.database, source.schema_name, table.name
                        ),
                        columns=columns,
                        path=path.as_posix(),
                        evidence=(evidence,),
                    )
                )
    return tuple(
        sorted(declared, key=lambda source: (source.source_name, source.table_name))
    )


def _materialization(config: dict[str, Any], project_name: str, layer: str) -> str | None:
    models = config.get("models")
    if not isinstance(models, dict):
        return None
    project_config = models.get(project_name)
    if not isinstance(project_config, dict):
        return None
    layer_config = project_config.get(layer)
    if not isinstance(layer_config, dict):
        return None
    value = layer_config.get("materialized")
    return value if isinstance(value, str) else None


def load_dbt_project(
    project_root: Path,
    *,
    target_schema: str = "analytics",
    target_database: str | None = None,
    progress: ProgressCallback | None = None,
) -> ParsedDbtProject:
    """Load a dbt project and resolve only literal ``ref`` and ``source`` calls.

    Unknown macros, control-flow Jinja, unresolved references, and duplicate
    resources fail closed. This resolver intentionally does not execute Jinja.
    """

    project_root = Path(project_root)
    project_file = project_root / "dbt_project.yml"
    if not project_file.is_file():
        raise DbtParseError(f"missing dbt project file: {project_file}")
    config = _load_yaml(project_file)
    if not isinstance(config, dict):
        raise DbtParseError(f"dbt project config must be a mapping: {project_file}")
    project_name = config.get("name")
    if not isinstance(project_name, str) or not project_name.strip():
        raise DbtParseError(f"dbt project name is missing: {project_file}")
    model_path_values = config.get("model-paths", ["models"])
    if not isinstance(model_path_values, list) or not all(
        isinstance(value, str) and value for value in model_path_values
    ):
        raise DbtParseError("model-paths must be a non-empty list of strings")
    model_roots = tuple(project_root / value for value in model_path_values)
    if progress:
        progress(f"dbt config={project_file.as_posix()} model_roots={len(model_roots)}")

    sql_paths = sorted(path for root in model_roots for path in root.rglob("*.sql"))
    name_to_path: dict[str, Path] = {}
    for path in sql_paths:
        if path.stem in name_to_path:
            raise DbtParseError(
                f"duplicate dbt model {path.stem}: {name_to_path[path.stem]} and {path}"
            )
        name_to_path[path.stem] = path
    sources = _load_sources(model_roots)
    source_index = {(source.source_name, source.table_name): source for source in sources}

    parsed_models: list[ParsedDbtModel] = []
    for index, (name, path) in enumerate(sorted(name_to_path.items()), start=1):
        try:
            raw_sql = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise DbtParseError(f"cannot read dbt model {path}: {exc}") from exc
        refs: list[str] = []
        source_ids: list[str] = []

        def replace_ref(
            match: re.Match[str],
            *,
            current_path: Path = path,
            found_refs: list[str] = refs,
        ) -> str:
            ref_name = match.group("name")
            if ref_name not in name_to_path:
                raise DbtParseError(f"unresolved ref({ref_name!r}) in {current_path}")
            found_refs.append(ref_name)
            return _relation_name(target_database, target_schema, ref_name)

        def replace_source(
            match: re.Match[str],
            *,
            current_path: Path = path,
            found_sources: list[str] = source_ids,
        ) -> str:
            key = (match.group("source"), match.group("table"))
            source = source_index.get(key)
            if source is None:
                raise DbtParseError(
                    f"unresolved source({key[0]!r}, {key[1]!r}) in {current_path}"
                )
            found_sources.append(source.id)
            return source.relation_name

        resolved_sql = _REF_PATTERN.sub(replace_ref, raw_sql)
        resolved_sql = _SOURCE_PATTERN.sub(replace_source, resolved_sql)
        if "{{" in resolved_sql or "{%" in resolved_sql or "{#" in resolved_sql:
            raise DbtParseError(f"unsupported Jinja remains in {path}")

        relative = next(
            (
                path.relative_to(model_root)
                for model_root in model_roots
                if path.is_relative_to(model_root)
            ),
            Path(path.name),
        )
        layer = relative.parts[0] if len(relative.parts) > 1 else "models"
        evidence = EvidencePointer(
            source=EvidenceSource.DBT_SQL,
            uri=path.as_posix(),
            locator=f"model={name}",
            content_hash=_sha256(raw_sql),
            excerpt=raw_sql,
        )
        asset = Model(
            id=f"model.{name}",
            name=name,
            path=path.as_posix(),
            database=target_database,
            schema_name=target_schema,
            materialization=_materialization(config, project_name, layer),
            evidence=(evidence,),
        )
        parsed_models.append(
            ParsedDbtModel(
                asset=asset,
                layer=layer,
                relation_name=_relation_name(target_database, target_schema, name),
                raw_sql=raw_sql,
                resolved_sql=resolved_sql,
                refs=_ordered_unique(refs),
                sources=_ordered_unique(source_ids),
            )
        )
        if progress and (index == len(name_to_path) or index % 10 == 0):
            progress(f"dbt models={index}/{len(name_to_path)}")

    return ParsedDbtProject(
        name=project_name,
        root=project_root.as_posix(),
        models=tuple(parsed_models),
        sources=sources,
    )
