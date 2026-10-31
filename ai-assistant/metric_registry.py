"""Approved KPI metadata based on app.py VALID_MEASURES and AI_RAG_Documentation."""
from dataclasses import dataclass
import re

@dataclass(frozen=True)
class Metric:
    name: str
    page: str
    unit: str
    kind: str = 'nonnegative'  # nonnegative | rate | signed
    aliases: tuple[str, ...] = ()
    formula: tuple[str, str] | None = None  # numerator, denominator (informational)

_DATA = [
    ('Total Flights','Flights','flights','nonnegative',('flight count',)),
    ('On-Time Performance %','Flights','percent','rate',('otp','on time performance','flight punctuality')),
    ('Average Delay Minutes','Flights','minutes','nonnegative',('average flight delay',)),
    ('Load Factor %','Flights','percent','rate',('load factor',)),
    ('Cancellation Rate %','Flights','percent','rate',('cancellation rate',)),
    ('Total Passengers','Flights','passengers','nonnegative',('flight passengers',)),
    ('Total Passengers Processed','Passenger','passengers','nonnegative',('passenger traffic','passenger throughput','passenger volume', 'passengers processed', 'total number of passengers processed','number of passengers processed','total passenger traffic',)),
    ('Average Security Wait Time','Passenger','minutes','nonnegative',('security wait time',)),
    ('Total Bags Processed','Baggage','bags','nonnegative',('baggage volume',)),
    ('Mishandled Baggage Rate %','Baggage','percent','rate',('mishandled baggage rate',)),
    ('Average First Bag Time','Baggage','minutes','nonnegative',('first bag time',)),
    ('Average Last Bag Time','Baggage','minutes','nonnegative',('last bag time',)),
    ('Total Commercial Revenue','Commercial','currency','signed',('commercial revenue','net commercial revenue')),
    ('Average Transaction Value','Commercial','currency','signed',('transaction value',)),
    ('Revenue per Passenger','Commercial','currency/passenger','signed',('commercial yield',)),
    ('Total Electricity Consumption','Sustainability','kWh','nonnegative',('electricity consumption','electricity',)),
    ('Total Water Consumption','Sustainability','m³','nonnegative',('water consumption',)),
    ('Total Waste Generated','Sustainability','tonnes','nonnegative',('waste generated',)),
    ('Total Estimated CO2e Emissions','Sustainability','tonnes CO2e','nonnegative',('carbon emissions','co2 emissions',)),
    ('Energy per Passenger','Sustainability','kWh/passenger','nonnegative',('electricity per passenger',)),
    ('Total Feedback','Experience','submissions','nonnegative',('passenger feedback','feedback submissions')),
]

METRICS = {row[0]: Metric(*row) for row in _DATA}
# Documented derived KPI relationships; not automatically recalculated unless explicitly modeled.
RELATIONSHIPS = {
    'Revenue per Passenger': ('Total Commercial Revenue', 'Total Passengers Processed'),
    'Energy per Passenger': ('Total Electricity Consumption', 'Total Passengers Processed'),
}

def normalize(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', ' ', value.lower()).strip()

ALIASES = {}
for metric in METRICS.values():
    for label in (metric.name, *metric.aliases):
        key = normalize(label)
        if key in ALIASES and ALIASES[key] != metric.name:
            raise RuntimeError(f'Ambiguous metric alias: {label}')
        ALIASES[key] = metric.name

def resolve_metric(label: str) -> Metric:
    name = ALIASES.get(normalize(label))
    if name is None:
        raise ValueError(f'Unapproved or ambiguous KPI: {label!r}')
    return METRICS[name]
