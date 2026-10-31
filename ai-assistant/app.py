import os
import msal
import requests
import ollama
from dotenv import load_dotenv
from docx import Document
import json
import re
import calendar
from datetime import date
from scenario_interpreter import interpret_scenario
from scenario_engine import extract_value, run_scenario, format_projection

# --------------------------------------------------
# Configuration

load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
DATASET_ID = os.getenv("DATASET_ID")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]

# --------------------------------------------------
# Semantic Model Documentation

def load_semantic_documentation():
    document = Document("AI_RAG_Documentation.docx")

    content = []

    for paragraph in document.paragraphs:
        if paragraph.text.strip():
            content.append(paragraph.text.strip())

    for table in document.tables:
        for row in table.rows:
            values = [
                cell.text.strip()
                for cell in row.cells
                if cell.text.strip()
            ]

            if values:
                content.append(" | ".join(values))

    return "\n".join(content)

# --------------------------------------------------
# AI Capability Router — Hybrid Approach
# --------------------------------------------------

def identify_question_intent(question):

    # Normalize the user's question
    normalized_question = " ".join(question.lower().split())

    # ------------------------------------------
    # Step 1: Detect navigation requests
    # ------------------------------------------

    navigation_phrases = (
        "where can i",
        "where do i",
        "where should i",
        "which page",
        "what page",
        "which dashboard page",
        "navigate to",
        "take me to",
        "switch to",
        "go to",
        "open the",
        "open page",
        "show me the page",
        "show me the dashboard page"
    )

    if any(
        phrase in normalized_question
        for phrase in navigation_phrases
    ):
        return "navigation"

    # ------------------------------------------
    # Step 2: Detect scenario projection requests
    # ------------------------------------------

    simulation_phrases = (
        "what if",
        "hypothetically",
        "suppose",
        "assuming",
        "assume that",
        "scenario projection",
        "simulate",
        "simulation",
        "projected value",
        "projected volume",
        "projected revenue",
        "what would happen if",
        "what would be the impact if"
    )

    if any(
        phrase in normalized_question
        for phrase in simulation_phrases
    ):
        return "simulation"

    # ------------------------------------------
    # Step 3: Use Ollama for other questions
    # ------------------------------------------

    response = ollama.chat(
        model="llama3.2:3b",
        format="json",
        options={"temperature": 0},
        messages=[
            {
                "role": "system",
                "content": """
You classify questions about an Airport Operations
Analytics dashboard.

Return ONLY valid JSON:

{
    "intent": "analysis"
}

Allowed intents:
- navigation
- explanation
- analysis
- simulation

SIMULATION:
Questions asking for hypothetical KPI values,
what-if scenarios, assumed changes, projected
outcomes, or target-based calculations.

Examples:

"If passenger traffic increases by 15%, what
would be the projected volume?"
-> simulation

"What if OTP improves by 10 percentage points?"
-> simulation

"How much revenue would we generate if commercial
revenue increased by 12%?"
-> simulation

Simulation means calculating hypothetical outcomes
using stated assumptions. It does not mean forecasting.

NAVIGATION:
Questions asking where to find information,
which page contains a KPI, or requesting
dashboard page navigation.

EXPLANATION:
Questions asking about KPI definitions,
formulas, dashboard purposes, or business concepts.

ANALYSIS:
Questions requesting actual KPI values,
numerical comparisons, rankings, or trends.

Examples:

"Where can I analyze flight delays?"
-> navigation

"Open the Baggage page."
-> navigation

"What is On-Time Performance?"
-> explanation

"What was OTP in August 2026?"
-> analysis

Do not answer the question.
Return only the JSON object.
"""
            },
            {
                "role": "user",
                "content": question
            }
        ]
    )

    result = json.loads(response["message"]["content"])

    intent = result.get("intent", "analysis")

    if intent not in ("navigation", "explanation", "analysis", "simulation"):
        return "analysis"

    return intent

def understand_navigation(question, documentation):

    response = ollama.chat(
        model="llama3.2:3b",
        format="json",
        options={"temperature": 0},
        messages=[
            {
                "role": "system",
                "content": f"""
You identify dashboard navigation requests.

Available pages:
Overview, Flights, Passenger, Baggage,
Commercial, Sustainability, Experience.

Use the documentation to identify the page
associated with the user's question.

Return ONLY JSON:

{{
    "page": null,
    "action": "recommend"
}}

Rules:
- "recommend": user asks where to find information.
- "open": user explicitly requests opening a page.
- Never invent page names.
- If no page matches, return null.
- Do not answer the question.

DOCUMENTATION:
{documentation}
"""
            },
            {
                "role": "user",
                "content": question
            }
        ]
    )

    result = json.loads(response["message"]["content"])

    page = result.get("page")
    action = result.get("action", "recommend")

    if page not in DASHBOARD_PAGES:
        page = None

    if action not in ("recommend", "open"):
        action = "recommend"

    return {
        "page": page,
        "action": action
    }

# --------------------------------------------------
# Identify Relevant Dashboard Page
# --------------------------------------------------

def identify_relevant_dashboard_page(question):

    normalized_question = " ".join(question.lower().split())

    # Map business concepts and KPIs to dashboard pages
    page_keywords = {
        "Flights": [
            "otp",
            "on-time performance",
            "on time performance",
            "flight delay",
            "flight delays",
            "delayed flight",
            "cancellation",
            "cancelled flights",
            "load factor",
            "flight punctuality",
            "flight operations",
        ],
        "Passenger": [
            "passenger traffic",
            "passenger volume",
            "passenger flow",
            "passenger throughput",
            "security wait",
            "screening time",
            "security screening",
        ],
        "Baggage": [
            "baggage",
            "mishandled bags",
            "first bag",
            "last bag",
            "bag delivery",
        ],
        "Commercial": [
            "commercial revenue",
            "revenue per passenger",
            "commercial transactions",
            "concessions",
            "retail revenue",
            "parking revenue",
        ],
        "Sustainability": [
            "sustainability",
            "electricity",
            "energy consumption",
            "energy per passenger",
            "water consumption",
            "co2",
            "co₂",
            "emissions",
            "waste",
        ],
        "Experience": [
            "passenger feedback",
            "passenger satisfaction",
            "customer feedback",
            "passenger comments",
        ],
        "Overview": [
            "overall performance",
            "airport overview",
            "airport performance summary",
        ],
    }

    for page, keywords in page_keywords.items():
        if any(keyword in normalized_question for keyword in keywords):
            return page

    return None

# --------------------------------------------------
# Dashboard Navigation
# --------------------------------------------------

DASHBOARD_PAGES = {
    "Overview": "354d64ce02501b777833",
    "Flights": "4665b987bb098ad61807",
    "Passenger": "2fd07b57ddd0005375b0",
    "Baggage": "ce6b1ee2893baa78e358",
    "Commercial": "09a77c8845fc0b3e147f",
    "Sustainability": "42912f4618872578390d",
    "Experience": "0e7271f37334c744c66b"
}

# --------------------------------------------------
# Dashboard Explanation
# --------------------------------------------------

def explain_dashboard(question, documentation):

    response = ollama.chat(
        model="llama3.2:3b",
        options={"temperature": 0},
        messages=[
            {
                "role": "system",
                "content": f"""
You are an Airport Operations Analytics Assistant.

Your responsibility is to explain the dashboard,
its pages, KPIs, measures, and business objectives.

Use ONLY the documentation provided below.

Rules:
1. Do not invent dashboard pages, KPIs, or formulas.
2. Do not calculate or fabricate operational values.
3. Explain concepts clearly and concisely.
4. If the documentation does not contain the answer,
   explicitly state that the information is unavailable.
5. Do not generate DAX or SQL.
6. Do not claim to have inspected live dashboard data.

DOCUMENTATION:

{documentation}
"""
            },
            {
                "role": "user",
                "content": question
            }
        ]
    )

    return response["message"]["content"]

# --------------------------------------------------
# Power BI Authentication

def authenticate_powerbi():
    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY
    )

    result = app.acquire_token_interactive(scopes=SCOPES)

    if "access_token" not in result:
        raise Exception(
            f"Power BI authentication failed: "
            f"{result.get('error_description')}"
        )

    return result["access_token"]


# --------------------------------------------------
# Power BI Query

def query_powerbi(access_token, dax_query):
    url = (
        f"https://api.powerbi.com/v1.0/myorg/"
        f"datasets/{DATASET_ID}/executeQueries"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    body = {
        "queries": [
            {
                "query": dax_query
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    response = requests.post(
        url,
        headers=headers,
        json=body
    )

    response.raise_for_status()

    return response.json()

def explain_result(
    question,
    resolved_request,
    powerbi_result,
    documentation
):

    prompt = f"""
You are the response layer of an Airport Operations Analytics AI Assistant.

Your job is to answer the user's question using ONLY:
1. The actual result returned by the Power BI semantic model.
2. The semantic model documentation provided below.

RULES:
- Power BI results are the source of truth for numerical values.
- Do not invent, estimate, or modify values.
- Do not invent explanations or causes that are not supported by the result.
- Follow the documented KPI definitions and business rules.
- Convert decimal rates to percentages when appropriate.
- Round displayed values to two decimal places when appropriate.
- Use the period labels from the resolved request when explaining comparisons.
- For comparisons, clearly state the values for each period and describe
  whether the metric increased or decreased.
- Keep the answer concise and business-friendly.
- Do not mention DAX, JSON, APIs, Python, or internal implementation details.
- If the result does not contain enough information to answer the question,
  say that the available data is insufficient.

USER QUESTION:
{question}

RESOLVED REQUEST:
{json.dumps(resolved_request, indent=2)}

POWER BI RESULT:
{json.dumps(powerbi_result, indent=2)}

SEMANTIC MODEL DOCUMENTATION:
{documentation}
"""

    response = ollama.chat(
        model="llama3.2:3b",
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        options={
            "temperature": 0
        }
    )

    return response["message"]["content"].strip()

# --------------------------------------------------
# Ollama

def ask_ollama(prompt, documentation):
    response = ollama.chat(
        model="llama3.2:3b",
        messages=[
            {
                "role": "system",
                "content": f"""
You are an Airport Operations Analytics Assistant connected to a
Power BI semantic model.

The documentation below is the authoritative source for:
- semantic model structure
- measures and KPIs
- business definitions
- terminology
- filtering rules
- analytical rules

When answering questions about this airport analytics solution:
1. Follow the documentation exactly.
2. Prefer documented Power BI measures over creating new calculations.
3. Do not invent or redefine measures, columns, tables, relationships,
   business rules, or KPI definitions.
4. Use the documented dimensions for filtering and analysis.
5. If the documentation does not support something, say that it is
   unavailable rather than guessing.
6. Keep responses concise.
7. Answer only questions related to airport operations topic.

SEMANTIC MODEL DOCUMENTATION:

{documentation}
"""
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return response["message"]["content"]

# --------------------------------------------------
# Question Understanding
# --------------------------------------------------

def understand_question(question, documentation):
    response = ollama.chat(
        model="llama3.2:3b",
        format="json",
        options={"temperature": 0},
        messages=[
            {
                "role": "system",
                "content": f"""
You identify business concepts in questions about an
Airport Operations Analytics semantic model.

Use the documentation below as your only source of
airport analytics terminology.

Your task is ONLY to identify:

- whether the question is related to this semantic model
- the KPI or measure being discussed
- the business dimension being discussed, if any
- the time expression written by the user, if any

Do NOT:
- generate DAX
- calculate dates
- calculate values
- answer the question
- create Power BI objects
- create additional JSON fields

Return ONLY this JSON structure:

{{
  "supported": true,
  "metric": null,
  "dimension": null,
  "time_expression": null
}}

Use the business concept as the metric.
For example, "OTP" and "punctuality" refer to the documented
on-time performance concept.

Preserve the user's time wording instead of interpreting
calendar dates.

If the question is unrelated to the documented semantic model,
set supported to false.

SEMANTIC MODEL DOCUMENTATION:

{documentation}
"""
            },
            {
                "role": "user",
                "content": question
            }
        ]
    )

    return json.loads(response["message"]["content"])

def validate_request(question, request):

    if not request.get("supported"):
        return {
            "supported": False,
            "message": (
                "I can only assist with questions related to "
                "the Airport Operations Analytics semantic model."
            )
        }

    metric = request.get("metric")
    dimension = request.get("dimension")
    time_expression = request.get("time_expression")

    # Determine analytical intent deterministically
    text = question.lower()

    if any(word in text for word in ["compare", "versus", " vs "]):
        intent = "compare"

    elif any(word in text for word in ["highest", "lowest", "top", "bottom"]):
        intent = "ranking"

    elif dimension:
        intent = "breakdown"

    else:
        intent = "lookup"

    return {
        "supported": True,
        "intent": intent,
        "metric": metric,
        "dimension": dimension,
        "time_expression": time_expression
    }

# --------------------------------------------------
# Semantic Resolver
# --------------------------------------------------

VALID_MEASURES = {
    "on-time performance %": "On-Time Performance %",
    "average delay minutes": "Average Delay Minutes",
    "average security wait time": "Average Security Wait Time",
    "total flights": "Total Flights",
    "total passengers": "Total Passengers",
    "load factor %": "Load Factor %",
    "cancellation rate %": "Cancellation Rate %",
    "total passengers processed": "Total Passengers Processed",
    "total bags processed": "Total Bags Processed",
    "mishandled baggage rate %": "Mishandled Baggage Rate %",
    "average first bag time": "Average First Bag Time",
    "average last bag time": "Average Last Bag Time",
    "total commercial revenue": "Total Commercial Revenue",
    "average transaction value": "Average Transaction Value",
    "revenue per passenger": "Revenue per Passenger",
    "total electricity consumption": "Total Electricity Consumption",
    "total water consumption": "Total Water Consumption",
    "total waste generated": "Total Waste Generated",
    "total estimated co2e emissions": "Total Estimated CO2e Emissions",
    "energy per passenger": "Energy per Passenger",
    "total feedback": "Total Feedback"
}


VALID_DIMENSIONS = {
    "dimairline": "DimAirline",
    "airline": "DimAirline",
    "airline_name": "DimAirline",

    "dimflight": "DimFlight",
    "flight": "DimFlight",

    "dimgate": "DimGate",
    "gate": "DimGate",

    "dimterminalzone": "DimTerminalZone",
    "terminal": "DimTerminalZone",
    "terminal zone": "DimTerminalZone",
    "terminal_zone": "DimTerminalZone"
}

DIMENSION_COLUMNS = {
    "DimAirline": "dim_airline[airline_name]",
    "DimFlight": "dim_flight[flight_id]",
    "DimGate": "dim_gate[gate_code]",
    "DimTerminalZone": "dim_terminal_zone[terminal_zone]"
}

DATE_COLUMN = "dim_date[date]"

def resolve_request(question, request):

    if not request.get("supported"):
        return request

    resolved = request.copy()

    # -----------------------------
    # Resolve measure
    # -----------------------------

    metric = request.get("metric")

    if metric:
        metric_key = str(metric).strip().lower()

        resolved["metric"] = VALID_MEASURES.get(
            metric_key
        )

    # -----------------------------
    # Resolve dimension
    # -----------------------------

    dimension = request.get("dimension")

    if isinstance(dimension, str):

        dimension_key = dimension.strip().lower()

        resolved["dimension"] = VALID_DIMENSIONS.get(
            dimension_key
        )

    else:
        # Reject malformed or invented dimensions
        resolved["dimension"] = None

    # -----------------------------
    # Preserve time expression
    # -----------------------------

    time_expression = request.get("time_expression")

    if isinstance(time_expression, str):
        resolved["time_expression"] = time_expression.strip()
    else:
        resolved["time_expression"] = None

    # -----------------------------
    # Recalculate intent
    # -----------------------------

    text = question.lower()

    if any(term in text for term in ["compare", "versus", " vs "]):
        resolved["intent"] = "compare"

    elif any(
        term in text
        for term in ["highest", "lowest", "top", "bottom"]
    ):
        resolved["intent"] = "ranking"

    elif resolved["dimension"]:
        resolved["intent"] = "breakdown"

    else:
        resolved["intent"] = "lookup"

    resolved["periods"] = resolve_time(question)

    return resolved

# --------------------------------------------------
# Time Resolver
# --------------------------------------------------

MONTHS = {
    month.lower(): number
    for number, month in enumerate(calendar.month_name)
    if month
}


def make_period(label, year, month=None, period_type="month"):

    if period_type == "year":
        start = date(year, 1, 1)
        end = date(year, 12, 31)

    elif period_type == "month":
        last_day = calendar.monthrange(year, month)[1]
        start = date(year, month, 1)
        end = date(year, month, last_day)

    elif period_type == "first_week":
        start = date(year, month, 1)
        end = date(year, month, 7)

    elif period_type in ("last_week", "end_of_month"):
        last_day = calendar.monthrange(year, month)[1]
        start = date(year, month, last_day - 6)
        end = date(year, month, last_day)

    else:
        return None

    return {
        "label": label,
        "start_date": start.isoformat(),
        "end_date": end.isoformat()
    }

# --------------------------------------------------
# Controlled DAX Builder
# --------------------------------------------------

def build_dax_query(request):

    if not request.get("supported"):
        return None

    metric = request.get("metric")
    dimension = request.get("dimension")
    periods = request.get("periods", [])
    intent = request.get("intent")

    if not metric:
        raise ValueError("No valid semantic-model measure was identified.")

    measure = f"[{metric}]"

    # --------------------------------
    # LOOKUP
    # --------------------------------

    if intent == "lookup":

        if periods:
            period = periods[0]

            return f"""
EVALUATE
ROW(
    "Value",
    CALCULATE(
        {measure},
        DATESBETWEEN(
            {DATE_COLUMN},
            DATE({period["start_date"][0:4]},
                 {int(period["start_date"][5:7])},
                 {int(period["start_date"][8:10])}),
            DATE({period["end_date"][0:4]},
                 {int(period["end_date"][5:7])},
                 {int(period["end_date"][8:10])})
        )
    )
)
""".strip()

        return f"""
EVALUATE
ROW(
    "Value",
    {measure}
)
""".strip()

    # --------------------------------
    # COMPARE
    # --------------------------------

    if intent == "compare":

        if len(periods) < 2:
            raise ValueError(
                "A comparison requires at least two valid periods."
            )

        values = []

        for index, period in enumerate(periods, start=1):

            values.append(
                f'''
    "Period {index}",
    CALCULATE(
        {measure},
        DATESBETWEEN(
            {DATE_COLUMN},
            DATE({period["start_date"][0:4]},
                 {int(period["start_date"][5:7])},
                 {int(period["start_date"][8:10])}),
            DATE({period["end_date"][0:4]},
                 {int(period["end_date"][5:7])},
                 {int(period["end_date"][8:10])})
        )
    )'''
            )

        return (
            "EVALUATE\nROW(\n"
            + ",\n".join(values)
            + "\n)"
        )

    # --------------------------------
    # BREAKDOWN
    # --------------------------------

    if intent == "breakdown":

        column = DIMENSION_COLUMNS.get(dimension)

        if not column:
            raise ValueError(
                "No valid grouping dimension was identified."
            )

        return f"""
EVALUATE
SUMMARIZECOLUMNS(
    {column},
    "Value", {measure}
)
""".strip()

    # --------------------------------
    # RANKING
    # --------------------------------

    if intent == "ranking":

        column = DIMENSION_COLUMNS.get(dimension)

        if not column:
            raise ValueError(
                "No valid ranking dimension was identified."
            )

        return f"""
EVALUATE
TOPN(
    1,
    SUMMARIZECOLUMNS(
        {column},
        "Value", {measure}
    ),
    [Value],
    DESC
)
""".strip()

    raise ValueError(
        f"Unsupported analytical intent: {intent}"
    )

def resolve_time(question):

    text = question.lower()

    periods = []

    # Find month + year combinations
    matches = re.findall(
        r"(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+(\d{4})",
        text
    )

    for month_name, year_text in matches:

        month = MONTHS[month_name]
        year = int(year_text)

        # First week
        if "first week" in text:
            periods.append(
                make_period(
                    f"First week of {month_name.title()} {year}",
                    year,
                    month,
                    "first_week"
                )
            )

        # Last week
        if "last week" in text:
            periods.append(
                make_period(
                    f"Last week of {month_name.title()} {year}",
                    year,
                    month,
                    "last_week"
                )
            )

        # End of month
        if "end of" in text:
            periods.append(
                make_period(
                    f"End of {month_name.title()} {year}",
                    year,
                    month,
                    "end_of_month"
                )
            )

        # Entire month
        if not any(
            phrase in text
            for phrase in [
                "first week",
                "last week",
                "end of"
            ]
        ):
            periods.append(
                make_period(
                    f"{month_name.title()} {year}",
                    year,
                    month,
                    "month"
                )
            )

    return periods



# --------------------------------------------------
# Capability 3 — Scenario Projection
# --------------------------------------------------

def execute_scenario_projection(question, access_token):
    """
    Interpret a hypothetical scenario, retrieve actual
    KPI baselines from Power BI, and calculate projections.

    Returns:
        {
            "answer": str,
            "page": str | None,
            "results": list
        }
    """

    # ------------------------------------------
    # Step 1: Interpret the scenario
    # ------------------------------------------

    scenario = interpret_scenario(question)

    # ------------------------------------------
    # Step 2: Define the baseline provider
    # ------------------------------------------

    def baseline_provider(metric_name, time_expression):

        if time_expression:

            periods = resolve_time(time_expression)

            if len(periods) != 1:
                raise ValueError(
                    "Please specify one supported reporting period, "
                    "such as August 2026."
                )

        else:
            periods = []

        # Reuse the existing controlled DAX builder
        request = {
            "supported": True,
            "intent": "lookup",
            "metric": metric_name,
            "dimension": None,
            "periods": periods
        }

        dax_query = build_dax_query(request)

        # Retrieve actual KPI baseline
        powerbi_result = query_powerbi(
            access_token,
            dax_query
        )

        # Extract a validated numerical value
        baseline = extract_value(powerbi_result)

        return baseline

    # ------------------------------------------
    # Step 3: Execute the generic scenario engine
    # ------------------------------------------

    projections = run_scenario(
        scenario,
        baseline_provider
    )

    # ------------------------------------------
    # Step 4: Format scenario results
    # ------------------------------------------

    answers = [
        format_projection(projection)
        for projection in projections
    ]

    answer = "\n\n---\n\n".join(answers)

    # ------------------------------------------
    # Step 5: Identify the relevant dashboard
    # ------------------------------------------

    pages = {
        projection.page
        for projection in projections
    }

    # Automatically navigate only when all
    # projections belong to the same dashboard.
    relevant_page = (
        next(iter(pages))
        if len(pages) == 1
        else None
    )

    return {
        "answer": answer,
        "page": relevant_page,
        "results": projections
    }


# --------------------------------------------------
# Application

if __name__ == "__main__":

    print("Starting Airport Analytics AI Agent...\n")

    # Load semantic model documentation
    documentation = load_semantic_documentation()
    print("Semantic model documentation loaded")

    # Authenticate with Power BI
    token = authenticate_powerbi()
    print("✓ Power BI connected")

    # Test questions
    test_questions = [
        "What is the OTP in August 2026?",
        "Compare OTP between the first week and the end of August 2026.",
        "Show average delay by airline.",
        "What is the average security wait time?",
        "Which airline has the highest OTP?",
        "What is the capital of Australia?"
    ]

    # Run test questions
    for question in test_questions:

        print("\n--------------------------------")
        print("Question:", question)

        # Step 1: Understand the question
        raw_request = understand_question(
            question,
            documentation
        )

        # Step 2: Validate the request
        request = validate_request(
            question,
            raw_request
        )

        # Step 3: Resolve semantic concepts and time periods
        resolved_request = resolve_request(
            question,
            request
        )

        print(json.dumps(resolved_request, indent=2))

        # Step 4: Query Power BI only for supported questions
        if resolved_request.get("supported"):

            dax_query = build_dax_query(
                resolved_request
            )

            print("\nGenerated DAX:")
            print(dax_query)

            try:
                result = query_powerbi(
                    token,
                    dax_query
                )

                print("\nPower BI result:")
                print(json.dumps(result, indent=2))

                # Step 5: Generate user-friendly answer
                answer = explain_result(
                    question,
                    resolved_request,
                    result,
                    documentation
                )

                print("\nAI Answer:")
                print(answer)

            except Exception as error:
                print("\nPower BI query failed:")
                print(error)

        else:
            print("\nAI Answer:")
            print(resolved_request.get("message"))