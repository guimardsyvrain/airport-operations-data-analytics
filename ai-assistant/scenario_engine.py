"""Deterministic scenario projections over approved Power BI KPI baselines."""
from dataclasses import dataclass
import math
from metric_registry import resolve_metric
from scenario_interpreter import ScenarioRequest

@dataclass(frozen=True)
class Projection:
    metric: str
    page: str
    unit: str
    period: str
    baseline: float
    projected: float
    absolute_change: float
    relative_change_percent: float | None
    assumption: str


def extract_value(powerbi_response: dict) -> float:
    """Expect exactly one non-null numeric [Value] from EVALUATE ROW('Value', ...)."""
    try:
        results = powerbi_response['results']
        if len(results) != 1 or 'error' in results[0]:
            raise ValueError('Power BI returned an error or unexpected result count.')
        tables = results[0]['tables']
        if len(tables) != 1 or len(tables[0]['rows']) != 1:
            raise ValueError('Power BI did not return a single baseline row.')
        row = tables[0]['rows'][0]
        matches = [v for k, v in row.items() if k.strip('[]').lower() == 'value']
        if len(matches) != 1 or isinstance(matches[0], bool) or not isinstance(matches[0], (int,float)):
            raise ValueError('Baseline is missing or not numeric.')
        value = float(matches[0])
        if not math.isfinite(value):
            raise ValueError('Baseline must be finite.')
        return value
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError('Unexpected Power BI response structure.') from exc


def calculate_projection(baseline: float, operation: str, value: float, kind: str) -> float:
    if not all(math.isfinite(x) for x in (baseline, value)):
        raise ValueError('Non-finite values are not supported.')
    # Power BI rate measures conventionally return decimal fractions, e.g. 0.6182.
    if operation == 'percentage_change':
        projected = baseline * (1 + value / 100)
    elif operation == 'absolute_change':
        if kind == 'rate':
            raise ValueError('Use percentage_point_change for rate KPI absolute adjustments.')
        projected = baseline + value
    elif operation == 'set_target':
        projected = value / 100 if kind == 'rate' else value
    elif operation == 'percentage_point_change':
        if kind != 'rate':
            raise ValueError('Percentage-point changes require a rate KPI.')
        projected = baseline + value / 100
    else:
        raise ValueError('Unsupported projection operation.')
    if kind == 'rate' and not (0 <= projected <= 1):
        raise ValueError('Projected rate must be between 0% and 100%.')
    if kind == 'nonnegative' and projected < 0:
        raise ValueError('Projected KPI cannot be negative.')
    return projected


def run_scenario(request: ScenarioRequest, baseline_provider) -> list[Projection]:
    """baseline_provider(metric_name, time_expression) -> numeric baseline.

    Multi-KPI assumptions produce independent projections, NOT cascading effects.
    """
    results = []
    for assumption in request.assumptions:
        metric = resolve_metric(assumption.metric)
        baseline = float(baseline_provider(metric.name, request.time_expression))
        if metric.kind == 'rate' and not 0 <= baseline <= 1:
            raise ValueError(f'{metric.name} baseline must be a decimal rate in [0, 1].')
        projected = calculate_projection(baseline, assumption.operation, assumption.value, metric.kind)
        difference = projected - baseline
        relative = difference / baseline * 100 if baseline != 0 else None
        results.append(Projection(metric.name, metric.page, metric.unit,
                                  request.time_expression or 'all available dates',
                                  baseline, projected, difference, relative,
                                  f'{assumption.operation}: {assumption.value:g}'))
    return results


def format_projection(result):
    """
    Format scenario projections into business-friendly responses.

    Supports assumptions stored as:
    - Strings: "percentage_change: 10"
    - Objects: Assumption(operation="percentage_change", value=10)
    - Dictionaries: {"operation": "percentage_change", "value": 10}
    """

    # ------------------------------------------
    # 1. Extract scenario results
    # ------------------------------------------

    def get_field(obj, field, default=None):
        if isinstance(obj, dict):
            return obj.get(field, default)
        return getattr(obj, field, default)

    metric = get_field(result, "metric")
    unit = get_field(result, "unit", "")
    baseline = float(get_field(result, "baseline"))
    projected = float(get_field(result, "projected"))
    change = projected - baseline
    period = get_field(result, "period", "Not specified")

    assumption = get_field(result, "assumption")

    # ------------------------------------------
    # 2. Extract assumption operation and value
    # ------------------------------------------

    if isinstance(assumption, str):

        # Example: "percentage_point_change: 5"
        if ":" not in assumption:
            raise ValueError(
                f"Invalid scenario assumption: {assumption}"
            )

        operation, value_text = assumption.split(":", 1)

        operation = operation.strip()
        value = float(value_text.strip())

    elif isinstance(assumption, dict):

        operation = assumption.get("operation")
        value = float(assumption.get("value"))

    else:

        operation = get_field(assumption, "operation")
        value = float(get_field(assumption, "value"))

    # ------------------------------------------
    # 3. Format KPI values
    # ------------------------------------------

    if unit == "percent":

        # Percentage KPIs are stored as proportions.
        baseline_display = f"{baseline * 100:,.2f}%"
        projected_display = f"{projected * 100:,.2f}%"
        change_display = (
            f"{change * 100:+,.2f} percentage points"
        )

    elif unit == "currency":

        # Currency denomination has not been verified.
        baseline_display = f"{baseline:,.2f}"
        projected_display = f"{projected:,.2f}"
        change_display = f"{change:+,.2f}"

    else:

        suffix = f" {unit}" if unit else ""

        baseline_display = f"{baseline:,.2f}{suffix}"
        projected_display = f"{projected:,.2f}{suffix}"
        change_display = f"{change:+,.2f}{suffix}"

    # ------------------------------------------
    # 4. Format scenario assumption
    # ------------------------------------------

    direction = "increases" if value >= 0 else "decreases"

    if operation == "percentage_change":

        assumption_display = (
            f"{metric} {direction} by {abs(value):g}%"
        )

    elif operation == "percentage_point_change":

        assumption_display = (
            f"{metric} {direction} by "
            f"{abs(value):g} percentage points"
        )

    elif operation == "absolute_change":

        suffix = f" {unit}" if unit != "currency" and unit else ""

        assumption_display = (
            f"{metric} {direction} by "
            f"{abs(value):,.2f}{suffix}"
        )

    elif operation == "set_target":

        assumption_display = (
            f"{metric} is set to {value:,.2f}"
        )

    else:

        raise ValueError(
            f"Unsupported scenario operation: {operation}"
        )

    # ------------------------------------------
    # 5. Build business-friendly response
    # ------------------------------------------

    return (
        f"### {metric} — Scenario Projection\n\n"
        f"**Reporting period:** {period}\n\n"
        f"**Actual baseline:** {baseline_display}\n\n"
        f"**Scenario assumption:** {assumption_display}\n\n"
        f"**Projected value:** **{projected_display}**\n\n"
        f"**Change:** {change_display}\n\n"
        "*This is a hypothetical scenario, not a forecast.*"
    )