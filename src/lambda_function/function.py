import json
from decimal import Decimal
from logging import INFO, getLogger
from os import environ
from typing import TYPE_CHECKING, Any

from boto3 import client
from boto3.dynamodb.types import Binary, TypeDeserializer

if TYPE_CHECKING:
    from mypy_boto3_firehose.client import FirehoseClient
else:
    FirehoseClient = object

getLogger().setLevel(environ.get("LOGGING_LEVEL") or INFO)

DELIVERY_STREAM_NAME = environ["DELIVERY_STREAM_NAME"]
DYNAMODB_IMAGE_TYPE = environ.get("DYNAMNODB_IMAGE_TYPE", "NewImage")
FIREHOSE = client("firehose")


class DynamoDBEncoder(json.JSONEncoder):
    def default(self, value):
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, Binary):
            return bytes(value)
        return super().default(value)


DESERIALIZER = TypeDeserializer()


def handler(event: dict[str, Any], context):

    getLogger().debug("Processing event {}".format(json.dumps(event)))

    if (records := event.get("Records")) and len(records):
        for batch in create_kinesis_batches(records):
            put_records_batch(batch)

    return event


def create_kinesis_batches(dynamodb_records) -> list[list[dict[str, str]]]:
    if not dynamodb_records or not len(dynamodb_records):
        return []
    kinesis_records = []
    count = 0
    total_length = 0
    for dynamodb_record in dynamodb_records:
        image: dict[str, Any] = None
        dynamodb: dict[str, Any] = dynamodb_record["dynamodb"]
        if image := dynamodb.get(DYNAMODB_IMAGE_TYPE):
            data = (
                json.dumps(
                    {k: DESERIALIZER.deserialize(v) for k, v in image.items()},
                    separators=(",", ":"),
                    cls=DynamoDBEncoder,
                )
                + "\n"
            )
            total_length += len(data)
            if total_length >= 4194304:
                break
            kinesis_records.append({"Data": data})
        count += 1
        if len(kinesis_records) == 500:
            break
    return [kinesis_records] + create_kinesis_batches(dynamodb_records[count:])


def put_records_batch(batch: list[dict[str, str]]) -> None:
    if not batch:
        return
    response = FIREHOSE.put_record_batch(
        DeliveryStreamName=DELIVERY_STREAM_NAME, Records=batch
    )
    getLogger().info(
        "Successfully processed {} records".format(
            len(batch) - response["FailedPutCount"]
        )
    )
    if response["FailedPutCount"]:
        getLogger().warn(
            "Failed to process {} records out of {}".format(
                response["FailedPutCount"], len(batch)
            )
        )
        getLogger().warn(
            "Failed requests {}".format(
                json.dumps(
                    [
                        request
                        for request in response["RequestResponses"]
                        if "ErrorCode" in request
                    ]
                )
            )
        )
