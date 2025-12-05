import requests
import zipfile
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = "flight-project-bucket-smit"

def get_s3():
    return S3Hook(aws_conn_id="aws_default")


def upload_bytes_to_s3(bytes_data, s3_key):
    """Upload raw bytes directly to S3 using aws_default."""
    s3 = get_s3()
    s3.load_bytes(
        bytes_data,
        key=s3_key,
        bucket_name=BUCKET,
        replace=True
    )
    print(f"Uploaded → s3://{BUCKET}/{s3_key}")


def download_file(url):
    """Download file from URL and return bytes."""
    print(f"Downloading: {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content


def fetch_csv_to_s3(url, s3_key):
    content = download_file(url)
    upload_bytes_to_s3(content, s3_key)


def fetch_zip_extract_csv_to_s3(url, s3_folder, final_filename):
    zip_bytes = download_file(url)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        csv_found = False
        for member in z.namelist():
            if member.lower().endswith(".csv"):
                print(f"Extracting CSV → {member}")
                csv_bytes = z.read(member)

                s3_key = f"{s3_folder}/{final_filename}"
                upload_bytes_to_s3(csv_bytes, s3_key)
                csv_found = True
                break

        if not csv_found:
            raise Exception(f"No CSV found inside ZIP from: {url}")




def fetch_file(url):
    """Download file as bytes."""
    print(f"Downloading: {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content
def upload_zip_direct(url, s3_key):
    """Upload ZIP directly (NO extraction)."""
    content = fetch_file(url)
    upload_bytes_to_s3(content, s3_key)

def fetch_and_upload_all():
    print("\n==== STARTING INGESTION PIPELINE ====\n")

    # Airlines
    fetch_csv_to_s3(
        url="https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat",
        s3_key="metadata/airlines.dat"
    )

    # Airports
    fetch_csv_to_s3(
        url="https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
        s3_key="metadata/airports.dat"
    )

    # Weather
    fetch_csv_to_s3(
        url="https://www.ncei.noaa.gov/data/global-hourly/access/2023/74486094789.csv",
        s3_key="weather/jfk_2023.csv"
    )

    # # BTS ZIP 1
    upload_zip_direct(
        url="https://transtats.bts.gov/PREZIP/On_Time_Marketing_Carrier_On_Time_Performance_Beginning_January_2018_2023_1.zip",
        s3_key="bts/ontime_2023_01.zip"
    )

    # BTS ZIP 2
    upload_zip_direct(
        url="https://transtats.bts.gov/PREZIP/On_Time_Marketing_Carrier_On_Time_Performance_Beginning_January_2018_2023_2.zip",
        s3_key="bts/ontime_2023_02.zip"
    )

    print("\n==== INGESTION PIPELINE COMPLETED SUCCESSFULLY ====\n")


if __name__ == "__main__":
    fetch_and_upload_all()
