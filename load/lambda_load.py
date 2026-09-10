import json
import boto3
import urllib.parse
from datetime import datetime, timezone

s3_client = boto3.client("s3")


def compute_comfort_category(temp_c, humidity):
    """Simple heuristic comfort category based on temp + humidity."""
    if temp_c >= 35:
        return "hot"
    elif temp_c >= 28 and humidity >= 60:
        return "warm_humid"
    elif temp_c >= 28:
        return "warm"
    elif temp_c <= 15:
        return "cold"
    else:
        return "mild"


def flatten_reading(raw_entry, ingested_at):
    """Flatten one city's OpenWeatherMap payload into a flat record."""
    main = raw_entry.get("main", {})
    wind = raw_entry.get("wind", {})
    weather_list = raw_entry.get("weather", [{}])
    weather_main = weather_list[0].get("main") if weather_list else None
    weather_desc = weather_list[0].get("description") if weather_list else None

    temp_c = main.get("temp")
    humidity = main.get("humidity")

    return {
        "city": raw_entry.get("name"),
        "country": raw_entry.get("sys", {}).get("country"),
        "reading_time_utc": datetime.fromtimestamp(
            raw_entry.get("dt", 0), tz=timezone.utc
        ).isoformat(),
        "temp_celsius": temp_c,
        "feels_like_celsius": main.get("feels_like"),
        "humidity_pct": humidity,
        "pressure_hpa": main.get("pressure"),
        "weather_main": weather_main,
        "weather_description": weather_desc,
        "wind_speed_mps": wind.get("speed"),
        "comfort_category": compute_comfort_category(temp_c, humidity)
        if temp_c is not None and humidity is not None
        else None,
        "ingested_at": ingested_at,
    }


def lambda_handler(event, context):
    records_written = 0

    for s3_record in event.get("Records", []):
        bucket = s3_record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(s3_record["s3"]["object"]["key"])

        print(f"Processing s3://{bucket}/{key}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw_payload = json.loads(response["Body"].read().decode("utf-8"))

        ingested_at = raw_payload.get("ingested_at")
        raw_entries = raw_payload.get("data", [])

        flattened = [flatten_reading(entry, ingested_at) for entry in raw_entries]

        processed_key = key.replace("raw/", "processed/", 1).replace(
            ".json", "-processed.json"
        )

        output_payload = {
            "ingested_at": ingested_at,
            "source_key": key,
            "record_count": len(flattened),
            "records": flattened,
        }

        s3_client.put_object(
            Bucket=bucket,
            Key=processed_key,
            Body=json.dumps(output_payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        print(f"Wrote {len(flattened)} records to s3://{bucket}/{processed_key}")
        records_written += len(flattened)

    return {
        "statusCode": 200,
        "body": f"Processed {records_written} records total",
    }
