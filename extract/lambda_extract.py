import json
import urllib.request
import urllib.error
import boto3
from datetime import datetime, timezone

CITIES = ["Bangalore", "Pune", "Mumbai", "Nagpur", "Delhi"]
BUCKET_NAME = "weather-etl-harshal-1210"
SECRET_NAME = "weather-etl/openweathermap-api-key"

secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


def get_api_key():
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret_dict = json.loads(response["SecretString"])
    return secret_dict["api_key"]


def fetch_weather(city, api_key):
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={city},IN&appid={api_key}&units=metric"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"HTTP error fetching {city}: {e.code} {e.reason}")
        return None
    except Exception as e:
        print(f"Error fetching {city}: {e}")
        return None


def lambda_handler(event, context):
    api_key = get_api_key()

    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y%m%dT%H%M%S")

    results = []
    errors = []

    for city in CITIES:
        data = fetch_weather(city, api_key)
        if data is not None:
            results.append(data)
        else:
            errors.append(city)

    if not results:
        print("No cities fetched successfully. Aborting write.")
        return {"statusCode": 500, "body": "All city fetches failed"}

    payload = {
        "ingested_at": now.isoformat(),
        "cities_requested": CITIES,
        "cities_failed": errors,
        "data": results,
    }

    key = f"raw/{now.strftime('%Y/%m/%d')}/{now.strftime('%H')}-{timestamp_str}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote {len(results)} readings to s3://{BUCKET_NAME}/{key}")
    if errors:
        print(f"Failed cities: {errors}")

    return {
        "statusCode": 200,
        "body": f"Wrote {len(results)} readings, {len(errors)} failed",
    }
