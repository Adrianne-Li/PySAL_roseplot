#!/usr/bin/env python3
"""
Build a PySAL interactive rose-plot website with:
- PyPI last week downloads
- PyPI last month downloads
- Conda total downloads
- GitHub stars/forks/contributors/age

Outputs:
- docs/pysal_metrics.json
- docs/interactive_pysal_rose_plot.html

Environment variables:
- GITHUB_TOKEN: optional GitHub PAT for higher API rate limits
- OUTPUT_DIR: optional output directory, default "docs"
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ============================================================
# Configuration
# ============================================================

PYPISTATS_BASE = "https://pypistats.org/api/packages"
GITHUB_API_BASE = "https://api.github.com"
ANACONDA_API_BASE = "https://api.anaconda.org/package"
DEFAULT_OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "docs"))
REQUEST_TIMEOUT = 30
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

MODULES: List[Dict[str, str]] = [
    {"module": "access", "pypi": "access", "owner": "pysal", "repo": "access", "conda_channel": "conda-forge", "conda_package": "access"},
    {"module": "esda", "pypi": "esda", "owner": "pysal", "repo": "esda", "conda_channel": "conda-forge", "conda_package": "esda"},
    {"module": "giddy", "pypi": "giddy", "owner": "pysal", "repo": "giddy", "conda_channel": "conda-forge", "conda_package": "giddy"},
    {"module": "inequality", "pypi": "inequality", "owner": "pysal", "repo": "inequality", "conda_channel": "conda-forge", "conda_package": "inequality"},
    {"module": "libpysal", "pypi": "libpysal", "owner": "pysal", "repo": "libpysal", "conda_channel": "conda-forge", "conda_package": "libpysal"},
    {"module": "mapclassify", "pypi": "mapclassify", "owner": "pysal", "repo": "mapclassify", "conda_channel": "conda-forge", "conda_package": "mapclassify"},
    {"module": "mgwr", "pypi": "mgwr", "owner": "pysal", "repo": "mgwr", "conda_channel": "conda-forge", "conda_package": "mgwr"},
    {"module": "pointpats", "pypi": "pointpats", "owner": "pysal", "repo": "pointpats", "conda_channel": "conda-forge", "conda_package": "pointpats"},
    {"module": "pysal", "pypi": "pysal", "owner": "pysal", "repo": "pysal", "conda_channel": "conda-forge", "conda_package": "pysal"},
    {"module": "segregation", "pypi": "segregation", "owner": "pysal", "repo": "segregation", "conda_channel": "conda-forge", "conda_package": "segregation"},
    {"module": "splot", "pypi": "splot", "owner": "pysal", "repo": "splot", "conda_channel": "conda-forge", "conda_package": "splot"},
    {"module": "spopt", "pypi": "spopt", "owner": "pysal", "repo": "spopt", "conda_channel": "conda-forge", "conda_package": "spopt"},
    {"module": "spreg", "pypi": "spreg", "owner": "pysal", "repo": "spreg", "conda_channel": "conda-forge", "conda_package": "spreg"},
    {"module": "spaghetti", "pypi": "spaghetti", "owner": "pysal", "repo": "spaghetti", "conda_channel": "conda-forge", "conda_package": "spaghetti"},
    {"module": "spglm", "pypi": "spglm", "owner": "pysal", "repo": "spglm", "conda_channel": "conda-forge", "conda_package": "spglm"},
]

MODULE_COLORS = {
    "access": "#1f77b4",
    "esda": "#aec7e8",
    "giddy": "#ff7f0e",
    "inequality": "#ffbb78",
    "libpysal": "#2ca02c",
    "mapclassify": "#98df8a",
    "mgwr": "#d62728",
    "pointpats": "#ff9896",
    "pysal": "#9467bd",
    "segregation": "#c5b0d5",
    "splot": "#8c564b",
    "spopt": "#c49c94",
    "spreg": "#e377c2",
    "spaghetti": "#f7b6d2",
    "spglm": "#7f7f7f",
}


# ============================================================
# Data model
# ============================================================

@dataclass
class ModuleMetrics:
    module: str
    pypi: str
    owner: str
    repo: str
    conda_channel: str
    conda_package: str
    pypi_last_week: int
    pypi_last_month: int
    conda_total_downloads: int
    stars: int
    forks: int
    age_years: float
    contributors: int
    color: str
    visible: bool
    repo_url: str
    pypi_url: str
    conda_url: str


# ============================================================
# HTTP helpers
# ============================================================

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "pysal-rose-plot-builder/2.0",
        "Accept": "application/json",
    })
    if GITHUB_TOKEN:
        session.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
        session.headers["X-GitHub-Api-Version"] = "2022-11-28"
    return session


def request_json_with_retry(
    session: requests.Session,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 6,
    base_sleep: float = 2.0,
    retry_on: Tuple[int, ...] = (429, 500, 502, 503, 504),
) -> Any:
    for attempt in range(max_retries + 1):
        try:
            response = session.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code in retry_on:
                if attempt == max_retries:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    sleep_seconds = float(retry_after)
                else:
                    sleep_seconds = base_sleep * (2 ** attempt) + random.uniform(0, 0.5)

                print(
                    f"[retry] {url} -> {response.status_code}, sleeping {sleep_seconds:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(sleep_seconds)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as exc:
            if attempt == max_retries:
                raise
            sleep_seconds = base_sleep * (2 ** attempt) + random.uniform(0, 0.5)
            print(
                f"[retry] {url} -> {exc.__class__.__name__}, sleeping {sleep_seconds:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Request failed unexpectedly: {url}")


# ============================================================
# Fetchers
# ============================================================

def fetch_pypi_recent_downloads(
    session: requests.Session,
    package: str,
    *,
    inter_request_sleep: float = 2.2,
) -> Tuple[int, int]:
    url = f"{PYPISTATS_BASE}/{package}/recent"
    payload = request_json_with_retry(
        session,
        url,
        headers={"Accept": "application/json"},
        max_retries=7,
        base_sleep=3.0,
        retry_on=(429, 500, 502, 503, 504),
    )
    recent = payload.get("data", {})
    last_week = int(recent.get("last_week", 0) or 0)
    last_month = int(recent.get("last_month", 0) or 0)

    # throttle to reduce pypistats rate-limit issues
    time.sleep(inter_request_sleep)
    return last_week, last_month


def fetch_github_repo_metadata(session: requests.Session, owner: str, repo: str) -> Dict[str, Any]:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"
    return request_json_with_retry(
        session,
        url,
        headers={"Accept": "application/vnd.github+json"},
        max_retries=4,
        base_sleep=1.5,
        retry_on=(403, 429, 500, 502, 503, 504),
    )


def count_github_contributors(session: requests.Session, owner: str, repo: str) -> int:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contributors"
    contributors = 0
    page = 1

    while True:
        batch = request_json_with_retry(
            session,
            url,
            headers={"Accept": "application/vnd.github+json"},
            params={"per_page": 100, "anon": 1, "page": page},
            max_retries=4,
            base_sleep=1.5,
            retry_on=(403, 429, 500, 502, 503, 504),
        )

        if not isinstance(batch, list) or not batch:
            break

        contributors += len(batch)

        if len(batch) < 100:
            break

        page += 1
        time.sleep(0.3)

    return contributors


def compute_age_years(created_at_iso: str) -> float:
    created = datetime.fromisoformat(created_at_iso.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    age_days = (now - created).total_seconds() / 86400.0
    return round(age_days / 365.25, 1)


def parse_conda_total_downloads(payload: Dict[str, Any]) -> int:
    """
    Defensive parser for Anaconda package payloads.

    Tries:
    1. top-level total fields
    2. sum of per-file download fields in 'files'
    """
    top_level_candidates = [
        "ndownloads",
        "download_count",
        "total_downloads",
        "downloads",
    ]
    for key in top_level_candidates:
        value = payload.get(key)
        if isinstance(value, (int, float)):
            return int(value)

    files = payload.get("files", [])
    if isinstance(files, list) and files:
        total = 0
        seen_any = False
        for file_obj in files:
            if not isinstance(file_obj, dict):
                continue
            for key in ("ndownloads", "download_count", "downloads"):
                value = file_obj.get(key)
                if isinstance(value, (int, float)):
                    total += int(value)
                    seen_any = True
                    break
        if seen_any:
            return total

    raise ValueError("Could not parse conda total downloads from Anaconda API payload.")


def fetch_conda_total_downloads(
    session: requests.Session,
    channel: str,
    package: str,
    *,
    inter_request_sleep: float = 0.8,
) -> int:
    url = f"{ANACONDA_API_BASE}/{channel}/{package}"
    payload = request_json_with_retry(
        session,
        url,
        headers={"Accept": "application/json"},
        max_retries=5,
        base_sleep=2.0,
        retry_on=(403, 429, 500, 502, 503, 504),
    )
    total = parse_conda_total_downloads(payload)
    time.sleep(inter_request_sleep)
    return total


def fetch_one_module(session: requests.Session, config: Dict[str, str]) -> ModuleMetrics:
    module = config["module"]
    print(f"[fetch] {module}", file=sys.stderr)

    pypi_last_week, pypi_last_month = fetch_pypi_recent_downloads(session, config["pypi"])
    conda_total_downloads = fetch_conda_total_downloads(session, config["conda_channel"], config["conda_package"])
    repo_meta = fetch_github_repo_metadata(session, config["owner"], config["repo"])
    contributors = count_github_contributors(session, config["owner"], config["repo"])

    stars = int(repo_meta.get("stargazers_count", 0) or 0)
    forks = int(repo_meta.get("forks_count", 0) or 0)
    created_at = repo_meta.get("created_at")
    age_years = compute_age_years(created_at) if created_at else 0.0

    return ModuleMetrics(
        module=module,
        pypi=config["pypi"],
        owner=config["owner"],
        repo=config["repo"],
        conda_channel=config["conda_channel"],
        conda_package=config["conda_package"],
        pypi_last_week=pypi_last_week,
        pypi_last_month=pypi_last_month,
        conda_total_downloads=conda_total_downloads,
        stars=stars,
        forks=forks,
        age_years=age_years,
        contributors=contributors,
        color=MODULE_COLORS[module],
        visible=(module == "pysal"),
        repo_url=f"https://github.com/{config['owner']}/{config['repo']}",
        pypi_url=f"https://pypi.org/project/{config['pypi']}/",
        conda_url=f"https://anaconda.org/{config['conda_channel']}/{config['conda_package']}",
    )


# ============================================================
# Payload building
# ============================================================

def build_summary(rows: List[ModuleMetrics]) -> Dict[str, Any]:
    if not rows:
        return {"total_modules": 0}

    top_pypi_month = max(rows, key=lambda x: x.pypi_last_month)
    top_conda = max(rows, key=lambda x: x.conda_total_downloads)
    most_starred = max(rows, key=lambda x: x.stars)
    oldest_module = max(rows, key=lambda x: x.age_years)

    return {
        "total_modules": len(rows),
        "top_pypi_month": top_pypi_month.module,
        "top_pypi_month_value": top_pypi_month.pypi_last_month,
        "top_conda_total": top_conda.module,
        "top_conda_total_value": top_conda.conda_total_downloads,
        "most_starred": most_starred.module,
        "most_starred_value": most_starred.stars,
        "oldest_module": oldest_module.module,
        "oldest_module_value": oldest_module.age_years,
    }


def build_payload(rows: List[ModuleMetrics]) -> Dict[str, Any]:
    total_pypi_last_month = sum(r.pypi_last_month for r in rows)
    total_conda_total = sum(r.conda_total_downloads for r in rows)

    sorted_pypi_month = sorted(rows, key=lambda x: x.pypi_last_month, reverse=True)
    sorted_conda_total = sorted(rows, key=lambda x: x.conda_total_downloads, reverse=True)

    pypi_ranks = {row.module: i + 1 for i, row in enumerate(sorted_pypi_month)}
    conda_ranks = {row.module: i + 1 for i, row in enumerate(sorted_conda_total)}

    enriched_rows = []
    max_stars = max((r.stars for r in rows), default=1)
    max_forks = max((r.forks for r in rows), default=1)
    max_age = max((r.age_years for r in rows), default=1)
    max_contributors = max((r.contributors for r in rows), default=1)

    for row in rows:
        normalized_values = [
            (row.pypi_last_month / total_pypi_last_month) if total_pypi_last_month else 0.0,
            (row.conda_total_downloads / total_conda_total) if total_conda_total else 0.0,
            (row.stars / max_stars) if max_stars else 0.0,
            (row.forks / max_forks) if max_forks else 0.0,
            (row.age_years / max_age) if max_age else 0.0,
            (row.contributors / max_contributors) if max_contributors else 0.0,
        ]
        item = asdict(row)
        item["values"] = normalized_values
        item["pypi_rank"] = pypi_ranks[row.module]
        item["conda_rank"] = conda_ranks[row.module]
        item["size"] = sum(normalized_values)
        enriched_rows.append(item)

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "feature_labels": [
            "PIP Install\n(Last month, %)",
            "Conda Install\n(Total, %)",
            "GitHub\nStars",
            "GitHub\nForks",
            "Age\n(Years)",
            "Contributors",
        ],
        "data": enriched_rows,
        "summary": build_summary(rows),
        "totals": {
            "pypi_last_month_total": total_pypi_last_month,
            "conda_total_downloads_total": total_conda_total,
        },
    }


def write_json(rows: List[ModuleMetrics], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "pysal_metrics.json"
    payload = build_payload(rows)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


# ============================================================
# HTML renderer
# ============================================================

def render_html(rows: List[ModuleMetrics]) -> str:
    payload_json = json.dumps(build_payload(rows), ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Interactive PySAL Ecosystem Rose Plot</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background-color: #f9fafb;
            margin: 0;
            padding: 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .container {{
            max-width: 1600px;
            width: 100%;
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
        }}
        .controls, .chart, .legend, .metrics {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .controls {{
            flex: 1;
            min-width: 250px;
        }}
        .chart {{
            flex: 4;
            min-width: 700px;
        }}
        .legend, .metrics {{
            flex: 1;
            min-width: 250px;
        }}
        .slider-container {{
            margin-bottom: 20px;
        }}
        .error-message {{
            color: red;
            display: none;
        }}
        .text-xl {{ font-size: 1.25rem; }}
        .font-bold {{ font-weight: 700; }}
        .mb-4 {{ margin-bottom: 1rem; }}
        .mb-2 {{ margin-bottom: 0.5rem; }}
        .mt-4 {{ margin-top: 1rem; }}
        .text-sm {{ font-size: 0.875rem; }}
        .text-gray-600 {{ color: #4b5563; }}
        .bg-blue-500 {{ background-color: #3b82f6; }}
        .text-white {{ color: white; }}
        .px-4 {{ padding-left: 1rem; padding-right: 1rem; }}
        .py-2 {{ padding-top: 0.5rem; padding-bottom: 0.5rem; }}
        .rounded {{ border-radius: 0.375rem; }}
        .hover\\:bg-blue-600:hover {{ background-color: #2563eb; }}
        .w-full {{ width: 100%; }}
        .flex {{ display: flex; }}
        .items-center {{ align-items: center; }}
        .cursor-pointer {{ cursor: pointer; }}
        .hover\\:bg-gray-200:hover {{ background-color: #e5e7eb; }}
        .px-2 {{ padding-left: 0.5rem; padding-right: 0.5rem; }}
        .py-1 {{ padding-top: 0.25rem; padding-bottom: 0.25rem; }}
        .tooltip {{
            position: absolute;
            background: rgba(0, 0, 0, 0.8);
            color: white;
            padding: 12px;
            border-radius: 6px;
            font-size: 14px;
            pointer-events: none;
            max-width: 320px;
            z-index: 10;
            line-height: 1.45;
        }}
        a {{
            color: #dbeafe;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="controls">
            <h2 class="text-xl font-bold mb-4">Interactive Controls</h2>
            <div class="slider-container">
                <label for="scale-factor">Scale Factor: <span id="scale-value">0.9</span></label>
                <input type="range" id="scale-factor" min="0.5" max="2.0" step="0.1" value="0.9" class="w-full">
            </div>
            <div class="slider-container">
                <label for="line-width">Line Width: <span id="line-width-value">2.5</span></label>
                <input type="range" id="line-width" min="0.5" max="5.0" step="0.1" value="2.5" class="w-full">
            </div>
            <div class="slider-container">
                <label for="opacity">Opacity: <span id="opacity-value">0.8</span></label>
                <input type="range" id="opacity" min="0.1" max="1.0" step="0.1" value="0.8" class="w-full">
            </div>
            <button id="reset-view" class="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600">Reset View</button>
            <p class="mt-4 text-sm text-gray-600">
                <strong>Scale Factor:</strong> Adjusts the overall size of all module polygons<br>
                <strong>Line Width:</strong> Controls the thickness of polygon outlines<br>
                <strong>Opacity:</strong> Changes transparency of lines and points<br>
                <strong>Mouse Wheel:</strong> Zoom in/out when hovering over the chart<br>
                <strong>Reset View:</strong> Returns to initial view (pysal only)<br>
                <strong>Hover:</strong> Hover over markers or lines to view raw module data
            </p>
            <p class="mt-4 text-sm text-gray-600" id="updated-at"></p>
            <p id="error-message" class="error-message">An error occurred while rendering the plot. Please try resetting the view.</p>
        </div>

        <div class="chart">
            <h2 class="text-xl font-bold mb-4">PySAL Ecosystem: Multi-Dimensional Performance Rose Plot</h2>
            <svg id="rose-plot" width="100%" height="600"></svg>
        </div>

        <div class="legend">
            <h2 class="text-xl font-bold mb-4">PySAL Modules</h2>
            <p class="text-sm text-gray-600 mb-2">Click module buttons to show or hide sub-modules (initially showing pysal)</p>
            <div id="module-legend"></div>
        </div>

        <div class="metrics">
            <h2 class="text-xl font-bold mb-4">Performance Metrics Overview</h2>
            <p id="metrics-content" class="text-sm"></p>
        </div>
    </div>

    <div class="tooltip" id="tooltip" style="display: none;"></div>

    <script>
        const payload = {payload_json};
        const rawData = payload.data;

        document.getElementById("updated-at").textContent = "Last updated: " + payload.generated_at_utc;

        const totalPypiLastMonth = payload.totals.pypi_last_month_total;
        const totalConda = payload.totals.conda_total_downloads_total;

        const featureLabels = payload.feature_labels;
        const modules = rawData.map(d => {{
            return {{
                name: d.module,
                values: d.values,
                raw: d,
                color: d.color,
                visible: d.visible,
                pypi_rank: d.pypi_rank,
                conda_rank: d.conda_rank,
                size: d.size
            }};
        }});

        const width = 600, height = 600, radius = Math.min(width, height) / 2 - 50;
        const svgRoot = d3.select("#rose-plot").attr("viewBox", `0 0 ${{width}} ${{height}}`);
        const svg = svgRoot.append("g").attr("transform", `translate(${{width / 2}}, ${{height / 2}})`);

        const numVars = featureLabels.length;
        const angleSlice = 2 * Math.PI / numVars;
        const angles = d3.range(numVars).map(i => i * angleSlice);

        const getX = (angle, r) => r * Math.cos(angle - Math.PI / 2);
        const getY = (angle, r) => r * Math.sin(angle - Math.PI / 2);

        let scaleFactor = 0.9;
        let lineWidth = 2.5;
        let opacity = 0.8;

        const tooltip = d3.select("#tooltip");

        function showTooltip(event, module) {{
            const raw = module.raw;
            const pypiPct = totalPypiLastMonth ? (raw.pypi_last_month / totalPypiLastMonth * 100).toFixed(2) : "0.00";
            const condaPct = totalConda ? (raw.conda_total_downloads / totalConda * 100).toFixed(2) : "0.00";

            const content = `
                <strong>${{module.name}}</strong><br>
                PyPI Install (Last week): ${{Number(raw.pypi_last_week).toLocaleString()}}<br>
                PyPI Install (Last month): ${{Number(raw.pypi_last_month).toLocaleString()}} (${{pypiPct}}%, Rank: ${{module.pypi_rank}}/${{rawData.length}})<br>
                Conda Install (Total): ${{Number(raw.conda_total_downloads).toLocaleString()}} (${{condaPct}}%, Rank: ${{module.conda_rank}}/${{rawData.length}})<br>
                GitHub Stars: ${{Number(raw.stars).toLocaleString()}}<br>
                GitHub Forks: ${{Number(raw.forks).toLocaleString()}}<br>
                Age (Years): ${{Number(raw.age_years).toFixed(1)}}<br>
                Contributors: ${{Number(raw.contributors).toLocaleString()}}<br>
                Repo: <a href="${{raw.repo_url}}" target="_blank">GitHub</a><br>
                PyPI: <a href="${{raw.pypi_url}}" target="_blank">Package</a><br>
                Conda: <a href="${{raw.conda_url}}" target="_blank">Package</a>
            `;

            tooltip.html(content)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 10) + "px")
                .style("display", "block");
        }}

        function hideTooltip() {{
            tooltip.style("display", "none");
        }}

        function updateMetricsPanel() {{
            const s = payload.summary;
            document.getElementById("metrics-content").innerHTML =
                `• Total Modules: ${{s.total_modules}}<br>` +
                `• Top PIP Install (Last month): ${{s.top_pypi_month}} (${{Number(s.top_pypi_month_value).toLocaleString()}})<br>` +
                `• Top Conda Install (Total): ${{s.top_conda_total}} (${{Number(s.top_conda_total_value).toLocaleString()}})<br>` +
                `• Most Starred: ${{s.most_starred}} (${{Number(s.most_starred_value).toLocaleString()}} stars)<br>` +
                `• Oldest Module: ${{s.oldest_module}} (${{Number(s.oldest_module_value).toFixed(1)}} years)`;
        }}

        function drawPlot() {{
            try {{
                svg.selectAll("*").remove();

                const currentRadius = radius * scaleFactor;

                [0.2, 0.4, 0.6, 0.8, 1.0].forEach(level => {{
                    svg.append("circle")
                        .attr("r", currentRadius * level)
                        .attr("fill", "none")
                        .attr("stroke", "gray")
                        .attr("stroke-width", 0.8)
                        .attr("opacity", 0.3);
                }});

                svg.selectAll(".level-label")
                    .data([0.2, 0.4, 0.6, 0.8, 1.0])
                    .join("text")
                    .attr("class", "level-label")
                    .attr("x", -currentRadius - 10)
                    .attr("y", d => -currentRadius * d)
                    .attr("dy", ".35em")
                    .attr("text-anchor", "end")
                    .attr("font-size", 10)
                    .attr("opacity", 0.8)
                    .text(d => d.toFixed(1));

                svg.selectAll(".axisLine")
                    .data(angles)
                    .join("line")
                    .attr("class", "axisLine")
                    .attr("x1", 0)
                    .attr("y1", 0)
                    .attr("x2", d => getX(d, currentRadius))
                    .attr("y2", d => getY(d, currentRadius))
                    .attr("stroke", "gray")
                    .attr("stroke-width", 1);

                svg.selectAll(".axisLabel")
                    .data(featureLabels)
                    .join("text")
                    .attr("class", "axisLabel")
                    .attr("x", (d, i) => getX(angles[i], currentRadius * 1.3))
                    .attr("y", (d, i) => {{
                        let y = getY(angles[i], currentRadius * 1.3);
                        if (Math.sin(angles[i] - Math.PI / 2) > 0) y += 18;
                        return y;
                    }})
                    .attr("dy", ".35em")
                    .attr("text-anchor", (d, i) => {{
                        const a = angles[i] - Math.PI / 2;
                        return Math.cos(a) > 0.15 ? "start" : Math.cos(a) < -0.15 ? "end" : "middle";
                    }})
                    .attr("font-size", 12)
                    .attr("font-weight", "bold")
                    .each(function(d) {{
                        const text = d3.select(this);
                        const lines = d.split("\\n");
                        text.text(null);
                        lines.forEach((line, idx) => {{
                            text.append("tspan")
                                .attr("x", text.attr("x"))
                                .attr("dy", idx === 0 ? 0 : "1.1em")
                                .text(line);
                        }});
                    }});

                const visibleModules = modules.filter(d => d.visible).sort((a, b) => b.size - a.size);

                visibleModules.forEach(module => {{
                    const dataPoints = module.values.map((v, i) => ({{
                        angle: angles[i],
                        value: v
                    }}));

                    const lineGen = d3.line()
                        .x(p => getX(p.angle, p.value * currentRadius))
                        .y(p => getY(p.angle, p.value * currentRadius))
                        .defined(p => isFinite(p.value));

                    const group = svg.append("g");

                    group.append("path")
                        .attr("d", lineGen(dataPoints) + "Z")
                        .attr("fill", module.color)
                        .attr("fill-opacity", 0.2)
                        .attr("stroke", "none")
                        .on("mouseover", event => showTooltip(event, module))
                        .on("mousemove", event => showTooltip(event, module))
                        .on("mouseout", hideTooltip);

                    group.append("path")
                        .attr("d", lineGen(dataPoints) + "Z")
                        .attr("fill", "none")
                        .attr("stroke", module.color)
                        .attr("stroke-width", lineWidth + 1)
                        .attr("opacity", 0.4)
                        .on("mouseover", event => showTooltip(event, module))
                        .on("mousemove", event => showTooltip(event, module))
                        .on("mouseout", hideTooltip);

                    group.append("path")
                        .attr("d", lineGen(dataPoints) + "Z")
                        .attr("fill", "none")
                        .attr("stroke", module.color)
                        .attr("stroke-width", lineWidth)
                        .attr("opacity", 0.9)
                        .on("mouseover", event => showTooltip(event, module))
                        .on("mousemove", event => showTooltip(event, module))
                        .on("mouseout", hideTooltip);

                    group.selectAll("circle")
                        .data(dataPoints)
                        .join("circle")
                        .attr("cx", p => getX(p.angle, p.value * currentRadius))
                        .attr("cy", p => getY(p.angle, p.value * currentRadius))
                        .attr("r", 5)
                        .attr("fill", module.color)
                        .attr("opacity", opacity)
                        .on("mouseover", event => showTooltip(event, module))
                        .on("mousemove", event => showTooltip(event, module))
                        .on("mouseout", hideTooltip);
                }});
            }} catch (error) {{
                console.error("Error rendering plot:", error);
                d3.select("#error-message").style("display", "block");
            }}
        }}

        const zoom = d3.zoom()
            .scaleExtent([0.5, 3])
            .on("zoom", (event) => {{
                svg.attr("transform", event.transform);
            }});

        svgRoot.call(zoom);

        const legend = d3.select("#module-legend")
            .selectAll("button")
            .data(modules)
            .join("button")
            .attr("class", "w-full flex items-center mb-2 cursor-pointer rounded px-2 py-1 hover:bg-gray-200")
            .on("click", function(event, d) {{
                d.visible = !d.visible;
                d3.select(this).selectAll("span").style("opacity", d.visible ? 1 : 0.3);
                drawPlot();
            }});

        legend.append("span")
            .style("width", "20px")
            .style("height", "20px")
            .style("background-color", d => d.color)
            .style("display", "inline-block")
            .style("margin-right", "8px")
            .style("opacity", d => d.visible ? 1 : 0.3);

        legend.append("span")
            .text(d => d.name)
            .style("opacity", d => d.visible ? 1 : 0.3);

        d3.select("#scale-factor").on("input", function() {{
            scaleFactor = +this.value;
            d3.select("#scale-value").text(scaleFactor.toFixed(1));
            drawPlot();
        }});

        d3.select("#line-width").on("input", function() {{
            lineWidth = +this.value;
            d3.select("#line-width-value").text(lineWidth.toFixed(1));
            drawPlot();
        }});

        d3.select("#opacity").on("input", function() {{
            opacity = +this.value;
            d3.select("#opacity-value").text(opacity.toFixed(1));
            drawPlot();
        }});

        d3.select("#reset-view").on("click", () => {{
            scaleFactor = 0.9;
            lineWidth = 2.5;
            opacity = 0.8;
            modules.forEach(d => d.visible = false);
            const pysalModule = modules.find(d => d.name === "pysal");
            if (pysalModule) pysalModule.visible = true;

            d3.select("#scale-factor").property("value", scaleFactor);
            d3.select("#scale-value").text(scaleFactor.toFixed(1));
            d3.select("#line-width").property("value", lineWidth);
            d3.select("#line-width-value").text(lineWidth.toFixed(1));
            d3.select("#opacity").property("value", opacity);
            d3.select("#opacity-value").text(opacity.toFixed(1));
            svgRoot.call(zoom.transform, d3.zoomIdentity);
            d3.select("#module-legend").selectAll("span").style("opacity", d => d.visible ? 1 : 0.3);
            drawPlot();
        }});

        updateMetricsPanel();
        drawPlot();
    </script>
</body>
</html>
"""


def write_html(rows: List[ModuleMetrics], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "interactive_pysal_rose_plot_updated.html"
    path.write_text(render_html(rows), encoding="utf-8")
    return path


# ============================================================
# Main
# ============================================================

def main() -> int:
    session = build_session()
    rows: List[ModuleMetrics] = []
    errors: List[str] = []

    for config in MODULES:
        try:
            rows.append(fetch_one_module(session, config))
        except Exception as exc:
            msg = f"{config['module']}: {exc}"
            errors.append(msg)
            print(f"[error] {msg}", file=sys.stderr)

    if not rows:
        print("No module data fetched successfully.", file=sys.stderr)
        return 1

    rows.sort(key=lambda x: x.module.lower())

    json_path = write_json(rows, DEFAULT_OUTPUT_DIR)
    html_path = write_html(rows, DEFAULT_OUTPUT_DIR)

    print(f"Wrote JSON: {json_path}")
    print(f"Wrote HTML: {html_path}")

    if errors:
        print("\\nSome modules failed, but output files were still generated:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    main()
