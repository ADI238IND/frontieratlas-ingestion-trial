# FrontierAtlas ingestion trial

This pipeline builds a source-backed AI intelligence dataset across startups, products, research papers, jobs and news. It accepts time-sensitive signals only when publication evidence supports the rolling 24-hour window. The three-page [architecture](architecture.pdf) covers scaling, deduplication, rate limits and storage.

[Open the six-tab trial dataset](https://docs.google.com/spreadsheets/d/1G42w9-BTF1JcAEs-V8Dd4CGfANE_dEknFmOeTyETPzk/edit)

## Recorded trial run

The export window ended at **09:08 IST on 21 November 2026** (03:38 UTC). The three bulk tabs met their minimum counts.

| Tab | Records | Evidence |
| --- | ---: | --- |
| Startups | 1,000 | Active AI-tagged YC companies with YC profiles and source IDs |
| Products | 1,000 | AGIPicks tool records with reported pricing and dated attribution |
| Research Papers | 1,014 | Distinct arXiv IDs with associated GitHub repositories and star counts |
| Jobs | 11 | Original publication time verified and job description collected |
| News | 2 | Publication time verified and article text collected |
| Entity Mapping Log | 3,011 | Original value, canonical result, rule and source URL |

Twenty paper rows use direct GitHub REST observations. The other 994 use GitHub metrics from the Papers with Code API snapshot. All exported repository records reported a source update date of November 21; the catalog update date does not establish the exact time each GitHub count was refreshed. The workbook includes the metric origin and observation time. With a GitHub token, the crawler attempts to refresh each selected repository directly.

The product catalogue publishes product names and pricing but does not reliably identify the underlying vendor. The run linked 73 product vendors by exact, unambiguous company-domain evidence and left 927 vendor cells blank. The startup count uses directory-reported team size where positive. `contact-sales` is normalized to `ENTERPRISE`, an interpretation of the catalogue pricing label. Paper publication dates supplied at day precision are represented as UTC midnight with the precision recorded separately.

The final monitor queried five AI news feeds and five AI employer boards. It held back 19 Ashby listings whose page provided no timezone-qualified original publication timestamp. Ashby `publishedAt` can be the time a posting was last published. Jobs with a verified date retain the real board timestamp, and unknown remote eligibility stays blank. The two news stories and 11 jobs reflected the run's rolling window. Their timestamps age out on subsequent live exports.

## Run it

Python 3.11 or later.

```bash
python -m venv .venv
source .venv/bin/activate      # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e .
python -m unittest discover -s tests -v
frontieratlas all --require-targets
```

The `directories`, `papers` and `signals` commands can run independently. Run `frontieratlas signals` on a scheduler to refresh jobs and news, then `frontieratlas export --require-targets` immediately before review. For a historical export with existing state, specify `--as-of` as a timezone-qualified ISO timestamp. Set `--db` to select the SQLite state file, `--output` for exports, and `--concurrency`, `--page-budget` and `--target` to control collection.

```bash
# Optional environment variables (set them using your local secret manager)
# GITHUB_TOKEN
# GEMINI_API_KEY, GROQ_API_KEY, DEEPSEEK_API_KEY
# GEMINI_MODEL, GROQ_MODEL, DEEPSEEK_MODEL
# GEMINI_RPM, GEMINI_TPM, GROQ_RPM, GROQ_TPM, DEEPSEEK_RPM, DEEPSEEK_TPM

frontieratlas papers --target 1000 --direct-github 20
frontieratlas signals
frontieratlas export --require-targets
```

Outputs are the six-tab `output/FrontierAtlas_Data.xlsx`, canonical JSON Lines files, and a run manifest. `state/frontieratlas.sqlite` holds source responses and extracted full text for local audit. The shareable spreadsheet contains article and job metadata, each source URL, body length and SHA-256. Raw publisher text is retained in state.

## Modules

| Module | Responsibility |
| --- | --- |
| `http.py` | Async transport, per-host lanes, bounded response bytes, retry and cooldown handling |
| `bulk.py` | YC and AGIPicks acquisition, arXiv identity, batched Papers with Code reads, GitHub star observations |
| `signals.py` | RSS/Atom and job-board adapters, full-text acquisition, publication evidence and quarantine |
| `llm.py` | Gemini Flash → Groq Llama 3 → DeepSeek, evidence-ranked chunking, quotas and quote validation |
| `core.py` | Name resolution, timezone normalization, audit log and idempotent persistence |
| `export.py` | Six-tab workbook and canonical row export with freshness checked again at export |
| `config/` | Sources and 50 curated startup/company names, plus product aliases |

The default Gemini model is `gemini-3.5-flash-lite`. Groq lists Llama 3.3 70B as an enterprise model. Set `GROQ_MODEL=openai/gpt-oss-20b` if your Groq project has access to GPT OSS rather than Llama.

The LLM path proposes an evidence-grounded company mention for news, or a job function where the title and department rules did not match. Exact source facts such as timestamps and star counts are collected outside the model. The provider test injects 413, 429 and invalid JSON, then checks that a grounded answer from the final provider is accepted. Live model calls require credentials and were not part of the recorded run.

## Sources

- [YC OSS](https://github.com/yc-oss/api) mirrors the public YC directory. Record URLs identify the original YC company profiles.
- [AGIPicks](https://agipicks.com/data/) supplies tool and pricing facts under CC BY 4.0. The trial normalizes names and pricing categories, and adds provenance.
- [Papers with Code](https://github.com/huggingface/paperswithcode) and [arXiv](https://arxiv.org/) supply paper identity, author metadata and repository links. The assignment's [example](https://paperswithcode.co/paper/98456) is a catalogued external resource. The bulk cohort uses arXiv IDs as the stable paper identity.
- [GitHub REST](https://docs.github.com/en/rest/repos/repos#get-a-repository) provides direct `stargazers_count` when available.
- [Ashby](https://developers.ashbyhq.com/docs/public-job-posting-api) and [Greenhouse](https://developers.greenhouse.io/job-board.html) provide employer postings.

Protected or client-rendered sources should be integrated through documented APIs, feeds, licensed exports or publisher-approved browser access. Challenge and access-denied outcomes stop collection on the affected route.
