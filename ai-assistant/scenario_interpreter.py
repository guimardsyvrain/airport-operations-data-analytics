"""
Airport Analytics AI Assistant
Natural-language scenario interpretation using Ollama.

Responsibilities:
- Interpret hypothetical KPI scenarios.
- Extract KPI adjustments and reporting periods.
- Validate structured output against approved KPIs.

Scenario calculations are handled by scenario_engine.py.
"""

import calendar
import logging
import re
from typing import Literal

import ollama
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from metric_registry import METRICS, ALIASES, resolve_metric


logger = logging.getLogger(__name__)


# --------------------------------------------------
# 1. Structured scenario models
# --------------------------------------------------

class Assumption(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric: str

    operation: Literal[
        "percentage_change",
        "absolute_change",
        "set_target",
        "percentage_point_change",
    ]

    value: float = Field(allow_inf_nan=False)


class ScenarioRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent: Literal["simulation"]

    time_expression: str | None = None

    assumptions: list[Assumption] = Field(
        min_length=1,
        max_length=5,
    )

    @model_validator(mode="after")
    def validate_metrics(self):

        seen = set()

        for item in self.assumptions:

            metric = resolve_metric(item.metric)

            # Normalize KPI to its approved name
            item.metric = metric.name

            # Prevent duplicate adjustments to one KPI
            if metric.name in seen:
                raise ValueError(
                    "Multiple adjustments to one KPI "
                    "are not supported yet."
                )

            seen.add(metric.name)

            # Percentage-point adjustments apply to rates only
            if (
                item.operation == "percentage_point_change"
                and metric.kind != "rate"
            ):
                raise ValueError(
                    "Percentage-point changes apply "
                    "only to rate KPIs."
                )

            # Rate KPIs require percentage-based adjustments
            if (
                item.operation == "absolute_change"
                and metric.kind == "rate"
            ):
                raise ValueError(
                    "For rate KPIs, specify a percentage "
                    "or percentage-point change."
                )

        return self


# --------------------------------------------------
# 2. Reporting-period validation
# --------------------------------------------------

_MONTHS = {
    name.lower(): name
    for name in calendar.month_name
    if name
}

_MONTHS.update({
    name[:3].lower(): name
    for name in calendar.month_name
    if name
})

_MONTH_PATTERN = "|".join(
    sorted(_MONTHS, key=len, reverse=True)
)


def _extract_period(question: str) -> str:
    """
    Extract and normalize a reporting period.

    Supported examples:
    - September 2026
    - Sep 2026
    - 2026-09
    - 2026/09

    This function validates date formatting.
    It does not interpret scenario assumptions.
    """

    month_first = re.search(
        rf"\b({_MONTH_PATTERN})\s*,?\s*(20\d{{2}})\b",
        question,
        re.IGNORECASE,
    )

    year_first = re.search(
        r"\b(20\d{2})[/-](0?[1-9]|1[0-2])\b",
        question,
    )

    if month_first:
        month = _MONTHS[month_first.group(1).lower()]
        year = month_first.group(2)

        return f"{month} {year}"

    if year_first:
        year = year_first.group(1)
        month = calendar.month_name[int(year_first.group(2))]

        return f"{month} {year}"

    raise ValueError(
        "Please specify a month and year, "
        "such as August 2026."
    )


# --------------------------------------------------
# 3. Natural-language scenario interpretation
# --------------------------------------------------

def interpret_scenario(question: str) -> ScenarioRequest:
    """
    Interpret a hypothetical airport KPI scenario
    using Ollama and validate the structured output.

    Natural-language understanding is performed
    by the LLM, not by deterministic regex rules.
    """

    if not question or not question.strip():
        raise ValueError(
            "Please provide a scenario question."
        )

    # Validate reporting period
    period = _extract_period(question)

    # Approved KPI names
    approved_metrics = ", ".join(METRICS.keys())

    # Approved KPI aliases
    approved_aliases = ", ".join(
        f"{alias} => {name}"
        for alias, name in ALIASES.items()
        if alias.lower() != name.lower()
    )

    # ----------------------------------------------
    # LLM system instructions
    # ----------------------------------------------

    system_prompt = (
        "You are an airport operations analytics "
        "scenario interpreter. "

        "Your task is to understand hypothetical "
        "KPI scenarios expressed in natural language. "

        "Return only structured JSON matching "
        "the provided schema. "

        "Always set intent to 'simulation'. "

        "Each assumption must contain: "
        "metric, operation, and numeric value. "

        "Interpret the user's meaning rather than "
        "requiring exact keywords or sentence patterns. "

        "Different expressions of the same scenario "
        "must produce equivalent structured assumptions. "

        "Examples: "

        "'What if commercial revenue increases by 10%?' "
        "and 'Calculate the impact of a 10% increase "
        "in commercial revenue' both mean: "
        "metric='Total Commercial Revenue', "
        "operation='percentage_change', value=10. "

        "'What if electricity consumption decreases "
        "by 12%?' and 'Calculate the impact of a 12% "
        "reduction in electricity consumption' both mean: "
        "metric='Total Electricity Consumption', "
        "operation='percentage_change', value=-12. "

        "'OTP improves by 5 percentage points' "
        "and 'Calculate the impact of a 5 percentage-point "
        "improvement in OTP' both mean: "
        "metric='On-Time Performance %', "
        "operation='percentage_point_change', value=5. "

        "'Set commercial revenue to 10000' means: "
        "metric='Total Commercial Revenue', "
        "operation='set_target', value=10000. "

        "Use percentage_change for relative percentage "
        "increases or decreases. "

        "Use percentage_point_change only when the user "
        "explicitly specifies percentage points. "

        "Use positive values for increases, improvements, "
        "or growth. "

        "Use negative values for decreases, reductions, "
        "or declines. "

        "Do not confuse a 10% increase with an increase "
        "of 10 percentage points. "

        "Create exactly one assumption for each distinct "
        "KPI adjustment requested by the user. "

        "Never create duplicate KPI assumptions. "

        "Do not invent missing KPIs or adjustment values. "

        "Keep assumptions in the same order as the "
        "user's question. "

        "Allowed operations: "
        "percentage_change, absolute_change, "
        "set_target, percentage_point_change. "

        "CRITICAL OPERATION RULES: "

        "When the user explicitly specifies a percentage "
        "using the % symbol or the word percent, "
        "always use operation='percentage_change', "
        "unless the user explicitly says percentage points. "

        "Never interpret a percentage reduction as an "
        "absolute change in the KPI's measurement unit. "

        "For example, 'a 12% reduction in electricity "
        "consumption' means operation='percentage_change' "
        "and value=-12, NOT operation='absolute_change'. "

        "A 12% reduction is different from a reduction "
        "of 12 kWh. "

        "For 'a reduction of 12 kWh', use "
        "operation='absolute_change' and value=-12. "

        "When a question contains a percentage adjustment, "
        "the numeric value represents the percentage, "
        "not the absolute KPI measurement unit. "

        "Example structured output for "
        "'Calculate the impact of a 12% reduction "
        "in electricity consumption in August 2026': "

        '{"intent":"simulation",'
        '"time_expression":"August 2026",'
        '"assumptions":[{"metric":"Total Electricity Consumption",'
        '"operation":"percentage_change","value":-12}]}. '

        f"Approved KPIs: {approved_metrics}. "

        f"Approved KPI aliases: {approved_aliases}. "

        "Use approved KPI names in the output. "

        "Do not generate DAX, calculations, predictions, "
        "forecasts, or explanations."
    )

    last_error = None

    # ----------------------------------------------
    # Initial attempt + one validation retry
    # ----------------------------------------------

    for attempt in range(2):

        messages = [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": question,
            },
        ]

        if attempt == 1:
            messages.append({
                "role": "user",
                "content": (
                    "Your previous response failed validation. "
                    "Please interpret the original question again. "
                    "Return exactly one assumption per requested KPI. "
                    "Use only approved KPI names and operations. "
                    "Include the correct sign and numerical value. "
                    "Do not invent or duplicate assumptions. "
                    "Return valid JSON only."
                ),
            })

        # Ollama performs natural-language interpretation
        response = ollama.chat(
            model="llama3.2:3b",
            format=ScenarioRequest.model_json_schema(),
            options={
                "temperature": 0,
            },
            messages=messages,
        )

        # ------------------------------------------
        # Validate LLM output
        # ------------------------------------------

        try:

            scenario = ScenarioRequest.model_validate_json(
                response["message"]["content"]
            )

            # Use the validated reporting period
            scenario.time_expression = period

            return scenario

        except (ValidationError, ValueError) as error:

            last_error = error

            logger.warning(
                "Scenario interpretation attempt %s failed: %s",
                attempt + 1,
                error,
            )

    raise ValueError(
        "I couldn't reliably interpret the scenario assumptions. "
        "Please specify each KPI and its adjustment clearly."
    ) from last_error