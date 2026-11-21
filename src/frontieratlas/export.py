from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from .core import WINDOW, iso

COMMON = ["id", "schemaVersion", "recordType", "source.name", "source.url", "collectedAt"]
SHEETS = {
    "Startups": ("STARTUP", [
        "content.entityName", "content.data.employeeCount", "content.website", "content.batch", "content.tags",
        "evidence.ycId", "evidence.snapshotUrl", "evidence.rawSha256"]),
    "Products": ("PRODUCT", [
        "content.entityName", "content.startupName", "content.pricingModel", "content.website",
        "content.category", "content.pricesCheckedAt", "evidence.vendorMatch", "evidence.sourcePricingModel",
        "evidence.attribution", "evidence.snapshotUrl", "evidence.rawSha256"]),
    "Research Papers": ("RESEARCH_PAPER", [
        "content.title", "content.authors", "content.paper_url", "content.github_url", "content.github_stars",
        "content.published_date", "evidence.publicationPrecision", "evidence.repositoryIsOfficial",
        "evidence.githubMetricSource", "evidence.githubMetricObservedAt", "evidence.upstreamRepositoryUpdatedDate",
        "evidence.githubMetricUrl", "evidence.repositoryLinkUrl", "evidence.catalogRecordUrl",
        "evidence.githubMetricSha256"]),
    "Jobs": ("JOB", [
        "content.title", "content.company", "content.date", "content.is_remote", "content.role_family",
        "content.location", "content.department", "evidence.dateMethod", "evidence.rawDate",
        "evidence.earliest", "evidence.latest", "evidence.boardUrl", "evidence.roleFamilyEvidence",
        "evidence.bodySha256", "bodyCharacters"]),
    "News": ("NEWS", [
        "content.title", "content.date", "evidence.dateMethod", "evidence.rawDate", "evidence.earliest",
        "evidence.latest", "evidence.discoveryUrl", "evidence.bodySha256", "bodyCharacters"]),
}


def value(obj, path):
    for key in path.split("."):
        obj = obj.get(key) if isinstance(obj, dict) else None
    return obj


def write_row(sheet, values):
    rendered = [json.dumps(item, ensure_ascii=False) if isinstance(item, (dict, list)) else item for item in values]
    sheet.append(rendered)
    for cell in sheet[sheet.max_row]:
        if isinstance(cell.value, str):
            cell.data_type = "s"
            if cell.value.startswith("https://") and len(cell.value) < 1000:
                cell.hyperlink = cell.value
                cell.font = Font(color="126A80", underline="single")


def style(sheet, columns):
    sheet.freeze_panes = "D2" if sheet.title != "Entity Mapping Log" else "C2"
    sheet.auto_filter.ref = sheet.dimensions
    sheet.sheet_view.showGridLines = False
    sheet.row_dimensions[1].height = 30
    for index, header in enumerate(columns, 1):
        cell = sheet.cell(1, index)
        cell.font = Font(bold=True, color="FFFFFF", size=10)
        cell.fill = PatternFill("solid", fgColor="163448")
        cell.alignment = Alignment(vertical="center", wrap_text=True)
        width = max(17, min(30, len(header) + 6))
        if any(keyword in header.casefold() for keyword in ("url", "website", "sha256", "title", "authors")):
            width = 48
        sheet.column_dimensions[get_column_letter(index)].width = width
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            if cell.row % 2 == 0:
                cell.fill = PatternFill("solid", fgColor="EEF5F8")


def publish(store, target, as_of):
    target = Path(target)
    target.mkdir(exist_ok=True, parents=True)
    book = Workbook()
    book.remove(book.active)
    counts = {}
    for sheet_name, (kind, fields) in SHEETS.items():
        sheet = book.create_sheet(sheet_name)
        columns = COMMON + fields
        write_row(sheet, columns)
        path = target / (sheet_name.lower().replace(" ", "_") + ".jsonl")
        with path.open("w", encoding="utf-8") as stream:
            for identity, item in store.rows(kind):
                if kind in {"JOB", "NEWS"}:
                    try:
                        low, high = (datetime.fromisoformat(item["evidence"][field])
                                     for field in ("earliest", "latest"))
                    except (KeyError, ValueError):
                        continue
                    if not as_of - WINDOW <= low <= high <= as_of:
                        continue
                item["id"] = identity
                full_text = item["content"].pop("full_text", "")
                if full_text:
                    item["bodyCharacters"] = len(full_text)
                write_row(sheet, [value(item, field) for field in columns])
                stream.write(json.dumps(item, ensure_ascii=False) + "\n")
        counts[sheet_name] = sheet.max_row - 1
        style(sheet, columns)
    log = book.create_sheet("Entity Mapping Log")
    columns = ["entityType", "raw", "source", "canonical", "decision", "sourceUrl"]
    write_row(log, columns)
    for row in store.db.execute("SELECT * FROM mapping ORDER BY kind,raw,source,url"):
        write_row(log, row)
    counts[log.title] = log.max_row - 1
    style(log, columns)
    book.properties.title = "FrontierAtlas | AI Intelligence Dataset"
    book.properties.description = f"Publication window ending {iso(as_of)}"
    book.save(target / "FrontierAtlas_Data.xlsx")
    return counts
