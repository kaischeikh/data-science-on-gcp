from flask import Flask
from markupsafe import escape
from ingest_flights_kc import *
app = Flask(__name__)


def _escape(value: str | None) -> str |None:
    if value is None:
        return value
    return escape(value)

@app.route('/', methods=["POST"])
def ingest_flight_data(request):
    try:
        json_data = request.to_json()
        year = _escape(json_data["year"])
        month = _escape(json_data["month"])
        bucket = escape(json_data["bucket"])
    except Exception as e:
        logging.exception("Failed, retry")