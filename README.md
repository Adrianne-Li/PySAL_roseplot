# PySAL Rose Plot

An interactive rose plot visualization of PySAL ecosystem metrics, rebuilt weekly by a GitHub Action.

**Live site:** [https://adrianne-li.github.io/PySAL_roseplot/](https://adrianne-li.github.io/PySAL_roseplot/)

---

## What it does

Every Monday, this repo automatically:

1. Fetches the latest metrics for each package in the PySAL ecosystem (downloads, contributors, stars, etc.)
2. Renders an interactive rose plot summarizing those metrics
3. Commits the refreshed HTML and JSON back to `docs/`, which GitHub Pages serves as the live site

Each petal of the rose corresponds to a PySAL module, with its length and shading driven by the underlying metrics. Hover and click interactions let you drill into individual packages.

## Repo structure

```
.
├── .github/workflows/
│   └── weekly-pysal-update.yml            # Runs every Monday
├── docs/
│   ├── index.html                         # Landing page
│   ├── interactive_pysal_rose_plot.html   # Generated — the interactive plot
│   └── pysal_metrics.json                 # Generated — raw metrics data
├── scripts/
│   └── build_pysal_rose_site.py           # The builder script
├── requirements.txt
└── README.md
```

Files marked "generated" are produced by the workflow on each run — you don't need to edit them by hand.

## Running locally

```bash
pip install -r requirements.txt

# Optional but recommended — raises GitHub API rate limit from 60/hr to 5000/hr
export GITHUB_TOKEN=ghp_your_personal_access_token

python scripts/build_pysal_rose_site.py
```

Outputs land in `docs/interactive_pysal_rose_plot.html` and `docs/pysal_metrics.json`. Open the HTML file in any browser to view.

## Triggering a manual update

Go to the [Actions tab](../../actions), pick **"Weekly PySAL plot update"**, and click **Run workflow**. Useful after merging changes to the builder script, or when you just want a fresh snapshot without waiting for Monday.

## Configuration

The workflow uses the repo's built-in `GITHUB_TOKEN` secret automatically — no setup required. For the scheduled cron to work, make sure:

- **Settings → Pages** is set to "Deploy from a branch", branch `main`, folder `/docs`
- **Settings → Actions → General → Workflow permissions** is set to "Read and write permissions"

## Related

See also the companion repo [PySAL_network-plot](https://github.com/adrianne-li/PySAL_network-plot) for an interactive dependency network visualization of the same ecosystem.

## License

MIT
