# Airport Operations Analytics | Microsoft Fabric, Power BI & AI Assistant

An end-to-end airport analytics portfolio project combining data engineering, interactive business intelligence, and a natural-language AI assistant. The solution brings operational data into a Microsoft Fabric analytics workflow, presents KPIs in Power BI, and supports guided dashboard exploration and hypothetical scenario analysis.

> **Project scope:** Demonstration/portfolio solution. Scenario results are illustrative calculations based on the available reporting-period baseline, not forecasts or recommendations.

## Business objective

Airport teams need a consolidated view of operational performance across flights, passengers, baggage, commercial activity, sustainability, and passenger experience. This project explores how a unified data platform and an AI-assisted interface can make those measures easier to monitor, explain, and explore.

## Solution overview

```text
Operational data sources (Azure SQL, APIs and project datasets)
                         |
                         v
              Microsoft Fabric ingestion
                         |
                         v
            Bronze -> Silver -> Gold Lakehouses
                         |
                         v
                Power BI semantic model
                         |
                  Power BI dashboards
                         |
                         v
           Streamlit Airport Analytics Assistant
             |          |             |
       Navigation   KPI questions   What-if scenarios
             |          |             |
             +----------+-------------+
                        |
        Ollama interpretation + validated KPI registry
                        |
          Deterministic scenario calculation engine
```

### Main components

| Component | Purpose |
| --- | --- |
| **Azure SQL / APIs / datasets** | Supply operational and supporting data |
| **Microsoft Fabric** | Orchestrate ingestion and Bronze, Silver, Gold transformations |
| **Power BI** | Provide a semantic model and interactive operational dashboards |
| **Streamlit** | Present the embedded dashboard and conversational interface |
| **Ollama (`llama3.2:3b`)** | Interpret natural-language questions and scenario assumptions locally |
| **Pydantic + KPI registry** | Validate structured scenario requests against approved metrics |
| **Scenario engine** | Apply deterministic adjustments to retrieved actual baselines |

## Analytics domains

The solution organizes reporting across six dashboard areas:

- **Flights:** Flight volume, on-time performance, delays, cancellations and load factor.
- **Passenger:** Passenger throughput and security wait times.
- **Baggage:** Bags processed, handling performance and bag delivery times.
- **Commercial:** Commercial revenue, transaction value and revenue per passenger.
- **Sustainability:** Electricity, water, waste, estimated emissions and energy per passenger.
- **Experience:** Passenger feedback and related measures.

The available KPIs and supported aliases are defined in [`ai-assistant/metric_registry.py`](ai-assistant/metric_registry.py).

## AI assistant capabilities

The AI assistant complements the Power BI dashboards with:

1. **Dashboard navigation:** Open a relevant report page from a natural-language request.
2. **KPI explanations:** Explain operational metrics and their business meaning.
3. **Actual-data questions:** Retrieve available KPI values for a specified reporting period.
4. **What-if scenarios:** Interpret a hypothetical change, retrieve the actual baseline, calculate the adjusted value, and show the difference.
5. **Input validation:** Check the reporting period, approved metric, operation, and numerical adjustment; report unavailable baselines rather than inventing values.

### Illustrative scenario examples

| Question | Baseline | Adjustment | Projected value |
| --- | ---: | ---: | ---: |
| What is the impact of a 10% increase in commercial revenue in September 2026? | 4,650.39 | +10% | **5,115.43** |
| What is the impact of a 12% reduction in electricity consumption in August 2026? | 2,599,305.40 kWh | −12% | **2,287,388.75 kWh** |
| What if on-time performance improves by 5 percentage points in August 2026? | 61.83% | +5 percentage points | **66.83%** |

These examples reflect values observed during project testing. They are **hypothetical projections, not statistical forecasts**. Results depend on the baseline returned by the connected semantic model.

## Repository structure

The repository includes Fabric artifacts, Power BI report and semantic-model definitions, source-data and API folders, and the AI assistant:

```text
airport-operations-data-analytics/
├── README.md
├── requirements.txt               # Existing API dependencies
├── api/
├── data/
├── [Microsoft Fabric and Power BI project artifacts]
└── ai-assistant/
    ├── app.py                     # Backend / Power BI integration
    ├── chat.py                    # Streamlit interface
    ├── metric_registry.py         # Approved KPI definitions
    ├── scenario_interpreter.py    # Ollama + Pydantic interpretation
    ├── scenario_engine.py         # Deterministic calculations
    ├── AI_RAG_Documentation.docx   # Assistant reference material
    ├── requirements.txt           # Assistant dependencies
    └── tests/                     # Integration and scenario test scripts
```

## Run the AI assistant locally

### Prerequisites

- Python 3.11 or a compatible version
- Access to the Power BI semantic model and any required Microsoft authentication configuration
- [Ollama](https://ollama.com/) installed and running locally
- The `llama3.2:3b` model available in Ollama

### Setup

From the repository root:

```bash
git clone https://github.com/guimardsyvrain/airport-operations-data-analytics.git
cd airport-operations-data-analytics/ai-assistant
python -m venv .venv
```

Activate the virtual environment:

```bash
# Git Bash on Windows
source .venv/Scripts/activate

# macOS / Linux
# source .venv/bin/activate
```

Install the assistant dependencies:

```bash
pip install -r requirements.txt
```

Prepare Ollama:

```bash
ollama pull llama3.2:3b
```

Configure the required environment variables in a local `.env` file, following the settings referenced by `app.py` and `chat.py`. **Do not commit `.env`, authentication tokens, or client secrets.** The exact required variable names should be verified against the code before publishing a reusable `.env.example`.

Launch the application:

```bash
streamlit run chat.py
```

> The assistant is designed for local execution with a running Ollama service and configured access to Power BI. Cloning the repository alone does not provide access to the connected semantic model.

## Design principles and limitations

- **Validated metrics:** Scenario assumptions are restricted to approved KPI names and supported operations.
- **Separation of concerns:** The LLM interprets user intent; the scenario engine performs calculations.
- **Grounded baselines:** Calculations use actual values retrieved for the requested reporting period.
- **Transparent scenarios:** The interface shows the baseline, assumption, projected value, and difference.
- **No invented baselines:** A missing or nonnumeric baseline produces a user-facing error.
- **Not a forecasting model:** Percentage changes and targets are user-supplied assumptions, not learned predictions.
- **Access requirements:** Live KPI queries depend on configured Power BI connectivity and permissions.

## Next improvements

- Add automated regression tests for percentage versus absolute changes, missing baselines, and unsupported metrics.
- Publish a sanitized `.env.example` after confirming the application's configuration variables.
- Verify and pin assistant dependency versions for reproducibility.
- Add dashboard screenshots and an architecture diagram to the portfolio documentation.

## Project links

- **Repository:** https://github.com/guimardsyvrain/airport-operations-data-analytics
- **Author's GitHub:** https://github.com/guimardsyvrain
