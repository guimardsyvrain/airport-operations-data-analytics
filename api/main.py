from fastapi import FastAPI
import json
from pathlib import Path

app = FastAPI(
    title="Airport Operations API",
    description="Training REST API for airport operational analytics",
    version="1.0"
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def load_json(filename):
    with open(DATA_DIR / filename, "r", encoding="utf-8") as file:
        return json.load(file)


@app.get("/")
def root():
    return {
        "message": "Airport Operations API",
        "status": "running"
    }


@app.get("/api/v1/flight-events")
def get_flight_events():
    return load_json("flight_operational_events.json")


@app.get("/api/v1/baggage-events")
def get_baggage_events():
    return load_json("baggage_events.json")