from pathlib import Path
import logging
import zipfile
import shutil
from gcloud import storage

SOURCE="https://storage.googleapis.com/data-science-on-gcp/edition2/raw"

BASE_URL = f"${SOURCE}/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"

def urlopen(url):
    from urllib.request import urlopen as impl
    import ssl

    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL')
    ctx_no_secure.check_hostname = False
    ctx_no_secure.verify_mode = ssl.CERT_NONE
    return impl(url, context=ctx_no_secure)


def download(year: int, month: int, destdir: Path| str) -> Path:
    if year <0:
        raise ValueError(f"Year must be a positive interge, passed value {year= }")
    if month < 0 & month > 12:
        raise ValueError(f"Month= {month} is not admissible must be a value between [1, 12]")
    destdir = Path(destdir)
    
    url = f"{BASE_URL}_{year}_{month}.zip"
    zipfile = destdir / f"{year}_{month:02d}.zip"
    
    logging.info(zipfile)
    with open(zipfile, "wb") as fp:
        response = urlopen(url)
        fp.write(response.read())
            
    return 
    
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