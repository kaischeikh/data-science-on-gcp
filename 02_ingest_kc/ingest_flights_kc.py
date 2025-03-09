from pathlib import Path
import tempfile
import logging
import zipfile
import shutil
from google.cloud import bigquery, storage

import datetime as dt

logging.basicConfig(level=logging.DEBUG)


SOURCE="https://storage.googleapis.com/data-science-on-gcp/edition2/raw"

BASE_URL = f"${SOURCE}/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"

def ingest(year: int, month: int, bucket: str) -> None:
    tmpdir = tempfile.mkdtemp(prefix='Flight_data')
    try:
        local_file = download(year, month, tmpdir)
        gzipped_file = zip_to_file(local_file, tmpdir)
        destination_blob_name = f"flights/raw/{year}{month}.csv"
        upload_csv(gzipped_file, destination_blob_name=destination_blob_name, bucket_name=bucket)
        return bqload(gzipped_file)
    finally:
        logging.info(f"Deleting tmpdir {tmpdir}")
        shutil.rmtree(tmpdir)

def download(year: str, month: str, destdir: Path| str) -> Path:
    destdir = Path(destdir)

    url = f"{BASE_URL}_{year}_{month}.zip"    
    zipfile = destdir / f"{year}_{month:02d}.zip"
    
    logging.debug(zipfile)
    with open(zipfile, "wb") as fp:
        response = urlopen(url)
        logging.debug(response.status)
        fp.write(response.read())
    logging.debug("File loaded properly")        
    return zipfile

def urlopen(url):
    from urllib.request import urlopen as impl
    import ssl

    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL')
    ctx_no_secure.check_hostname = False
    ctx_no_secure.verify_mode = ssl.CERT_NONE
    return impl(url, context=ctx_no_secure)
    
def zip_to_file(filename: Path, destdir: Path | str) -> Path:
    zip_file = zipfile.ZipFile(file=filename, mode="r")
    destdir = Path(destdir)
    destdir.mkdir(exist_ok=True, parents=True)
    zip_file.extractall(destdir)
    files = list(destdir.glob("*.csv"))
    
    if len(files) != 1:
        raise ValueError("Unexpected length of files check folder")
    zip_file.close()
    
    csv_file = files[0]
    gzipped_file = csv_file.with_suffix(".csv.gz")

    with open(csv_file, "rb") as ifp:
        with open(gzipped_file, "rb") as ofp:
            shutil.copyfileobj(ifp, ofp)
    return gzipped_file
    
    
def upload_csv(csvfiles: Path, destination_blob_name: str, bucket_name: str = "ds-on-gcp") -> None:
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(csvfiles)
    
    logging.info(
        f"File {csvfiles} uploaded to {destination_blob_name}."
    )
    
def bqload(gcs_file: Path| str, year: int, month: int):
    client = bigquery.Client()
    _schema = [
        bigquery.SchemaField(col_and_type.split(':')[0], col_and_type.split(':')[1])
        for col_and_type in
        "Year:STRING,Quarter:STRING,Month:STRING,DayofMonth:STRING,DayOfWeek:STRING,FlightDate:DATE,Reporting_Airline:STRING,DOT_ID_Reporting_Airline:STRING,IATA_CODE_Reporting_Airline:STRING,Tail_Number:STRING,Flight_Number_Reporting_Airline:STRING,OriginAirportID:STRING,OriginAirportSeqID:STRING,OriginCityMarketID:STRING,Origin:STRING,OriginCityName:STRING,OriginState:STRING,OriginStateFips:STRING,OriginStateName:STRING,OriginWac:STRING,DestAirportID:STRING,DestAirportSeqID:STRING,DestCityMarketID:STRING,Dest:STRING,DestCityName:STRING,DestState:STRING,DestStateFips:STRING,DestStateName:STRING,DestWac:STRING,CRSDepTime:STRING,DepTime:STRING,DepDelay:STRING,DepDelayMinutes:STRING,DepDel15:STRING,DepartureDelayGroups:STRING,DepTimeBlk:STRING,TaxiOut:STRING,WheelsOff:STRING,WheelsOn:STRING,TaxiIn:STRING,CRSArrTime:STRING,ArrTime:STRING,ArrDelay:STRING,ArrDelayMinutes:STRING,ArrDel15:STRING,ArrivalDelayGroups:STRING,ArrTimeBlk:STRING,Cancelled:STRING,CancellationCode:STRING,Diverted:STRING,CRSElapsedTime:STRING,ActualElapsedTime:STRING,AirTime:STRING,Flights:STRING,Distance:STRING,DistanceGroup:STRING,CarrierDelay:STRING,WeatherDelay:STRING,NASDelay:STRING,SecurityDelay:STRING,LateAircraftDelay:STRING,FirstDepTime:STRING,TotalAddGTime:STRING,LongestAddGTime:STRING,DivAirportLandings:STRING,DivReachedDest:STRING,DivActualElapsedTime:STRING,DivArrDelay:STRING,DivDistance:STRING,Div1Airport:STRING,Div1AirportID:STRING,Div1AirportSeqID:STRING,Div1WheelsOn:STRING,Div1TotalGTime:STRING,Div1LongestGTime:STRING,Div1WheelsOff:STRING,Div1TailNum:STRING,Div2Airport:STRING,Div2AirportID:STRING,Div2AirportSeqID:STRING,Div2WheelsOn:STRING,Div2TotalGTime:STRING,Div2LongestGTime:STRING,Div2WheelsOff:STRING,Div2TailNum:STRING,Div3Airport:STRING,Div3AirportID:STRING,Div3AirportSeqID:STRING,Div3WheelsOn:STRING,Div3TotalGTime:STRING,Div3LongestGTime:STRING,Div3WheelsOff:STRING,Div3TailNum:STRING,Div4Airport:STRING,Div4AirportID:STRING,Div4AirportSeqID:STRING,Div4WheelsOn:STRING,Div4TotalGTime:STRING,Div4LongestGTime:STRING,Div4WheelsOff:STRING,Div4TailNum:STRING,Div5Airport:STRING,Div5AirportID:STRING,Div5AirportSeqID:STRING,Div5WheelsOn:STRING,Div5TotalGTime:STRING,Div5LongestGTime:STRING,Div5WheelsOff:STRING,Div5TailNum:STRING".split(',')
    ]
    table_ref = client.dataset('dsongcp').table('flights_raw${}{}'.format(year, month))
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        ignore_unknown_values = True,
        time_partioning=bigquery.table.TimePartitioning('MONTH', 'FlightDate'),
        schema=_schema
    )
    
    job = client.load_table_from_uri(gcs_file, table_ref, job_config=job_config)
    job.result()
    
    if job.state != 'DONE':
        raise job.exception()
    return table_ref, job.output_rows

def next_month(bucket_name: str) -> tuple[int, int]:
    '''
     Finds which months are on GCS, and returns next year,month to download
   '''
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix="flights/raw/"))

    files = [blob.name for blob in blobs if "csv" in blob.name]
    files.sort()
    last_file = Path(files[-1]).with_suffix("")
    year = int(last_file[:4])
    month = int(last_file[4:])
    
    last_date = dt.datetime(year=year, month=month)
    last_date = last_date + dt.timedelta(days=31)
    
    return str(last_date.year), f"{last_date.month:.02d}" 
    
    
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Parsing module to ingest flight data")
    parser.add_argument("--bucket", default="ds-on-gcp")
    parser.add_argument("--year", required=False, default=None)
    parser.add_argument("--month", required=False, default=None)
    
    try:
        args = parser.parse_args()
        year, month = args.year, args.month
        if year is None or month is None:
            logging.info("Infering year and name")
            year, month = next_month(args.bucket)
            
        logging.debug(f"Ingesting year={year} month={month}")
        tableref, numrows =  ingest(year, month, args.ucket)
        logging.info(f"Data succesfully {year} {month}: in {tableref} Rows {numrows} have been added")
        
    except:
        logging.error("Retry data unavailable")
        
