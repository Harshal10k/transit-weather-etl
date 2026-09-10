import json
import boto3
import pymysql
import urllib.parse
from datetime import datetime

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

SECRET_NAME = "weather-etl/rds-credentials"

_db_creds = None


def get_db_credentials():
    global _db_creds
    if _db_creds is None:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        _db_creds = json.loads(response["SecretString"])
    return _db_creds


def get_connection():
    creds = get_db_credentials()
    return pymysql.connect(
        host=creds["host"],
        user=creds["username"],
        password=creds["password"],
        database=creds["database"],
        port=3306,
        connect_timeout=10,
        cursorclass=pymysql.cursors.Cursor,
    )


UPSERT_SQL = """
INSERT INTO weather_readings (
    city, country, reading_time_utc, temp_celsius, feels_like_celsius,
    humidity_pct, pressure_hpa, weather_main, weather_description,
    wind_speed_mps, comfort_category, ingested_at
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    temp_celsius = VALUES(temp_celsius),
    feels_like_celsius = VALUES(feels_like_celsius),
    humidity_pct = VALUES(humidity_pct),
    pressure_hpa = VALUES(pressure_hpa),
    weather_main = VALUES(weather_main),
    weather_description = VALUES(weather_description),
    wind_speed_mps = VALUES(wind_speed_mps),
    comfort_category = VALUES(comfort_category),
    ingested_at = VALUES(ingested_at);
"""


def parse_datetime(value):
    if value is None:
        return None
    return datetime.fromisoformat(value)


def lambda_handler(event, context):
    rows_written = 0
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            for s3_record in event.get("Records", []):
                bucket = s3_record["s3"]["bucket"]["name"]
                key = urllib.parse.unquote_plus(s3_record["s3"]["object"]["key"])

                print(f"Processing s3://{bucket}/{key}")

                response = s3_client.get_object(Bucket=bucket, Key=key)
                payload = json.loads(response["Body"].read().decode("utf-8"))

                records = payload.get("records", [])

                for record in records:
                    cursor.execute(
                        UPSERT_SQL,
                        (
                            record.get("city"),
                            record.get("country"),
                            parse_datetime(record.get("reading_time_utc")),
                            record.get("temp_celsius"),
                            record.get("feels_like_celsius"),
                            record.get("humidity_pct"),
                            record.get("pressure_hpa"),
                            record.get("weather_main"),
                            record.get("weather_description"),
                            record.get("wind_speed_mps"),
                            record.get("comfort_category"),
                            parse_datetime(record.get("ingested_at")),
                        ),
                    )
                    rows_written += 1

        connection.commit()
        print(f"Committed {rows_written} rows to weather_readings")

    finally:
        connection.close()

    return {
        "statusCode": 200,
        "body": f"Upserted {rows_written} rows",
    }
