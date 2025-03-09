from flask import Flask
from markupsafe import escape
import logging
from ingest_flights_kc import ingest, next_month
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
        
        if year is None or month is None:
            logging.info("Infering year and name")
            year, month = next_month(bucket)
            
        logging.debug(f"Ingesting year={year} month={month}")
        tableref, numrows =  ingest(year, month, bucket)
        sucess = f"Data succesfully {year} {month}: in {tableref} Rows {numrows} have been added"
        return sucess
    
    except Exception as e:
        logging.exception("Failed, retry")