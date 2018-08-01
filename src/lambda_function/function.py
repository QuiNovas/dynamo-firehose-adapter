import boto3
import decimal
import json
import logging.config
import os

from boto3.dynamodb.types import Binary, TypeDeserializer

delivery_stream_name = os.environ['DELIVERY_STREAM_NAME']
dynamodb_image_type = os.environ.get('DYNAMNODB_IMAGE_TYPE', 'NewImage')
firehose = boto3.client('firehose')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DynamoDBEncoder(json.JSONEncoder):
    def default(self, value):
        if isinstance(value, decimal.Decimal):
            return float(value)
        elif isinstance(value, Binary):
            return bytes(value)
        return super(DynamoDBEncoder, self).default(value)


class DynamoDBTypeDeserializer(TypeDeserializer):
    def _deserialize_b(self, value):
        if isinstance(value, unicode):
            value = str(value)
        return super(DynamoDBTypeDeserializer, self)._deserialize_b(value)


type_deserializer = DynamoDBTypeDeserializer()


def handler(event, context):

    logger.debug('Processing event {}'.format(json.dumps(event)))

    if 'Records' in event and len(event['Records']):
        map(put_records_batch, create_kinesis_batches(event['Records']))

    return event


def create_kinesis_batches(dynamodb_records):
    if not dynamodb_records or not len(dynamodb_records):
        return []
    kinesis_records = []
    count = 0
    total_length = 0
    for dynamodb_record in dynamodb_records:
        if dynamodb_image_type in dynamodb_record['dynamodb']:
            image = {
                k: type_deserializer.deserialize(v) for k, v in dynamodb_record['dynamodb'][dynamodb_image_type].items()
            }
            data = json.dumps(image, separators=(',', ':'), cls=DynamoDBEncoder) + '\n'
            total_length += len(data)
            if total_length >= 4194304:
                break
            kinesis_records.append(
                {
                    'Data': data
                }
            )
        count += 1
        if len(kinesis_records) == 500:
            break
    return [kinesis_records]+[create_kinesis_batches(dynamodb_records[count:])]


def put_records_batch(batch):
    if not batch or not len(batch):
        return
    response = firehose.put_record_batch(
        DeliveryStreamName=delivery_stream_name,
        Records=batch
    )
    logger.info('Successfully processed {} records'.format(len(batch) - response['FailedPutCount']))
    if response['FailedPutCount']:
        logger.warn('Failed to process {} records out of {}'.format(response['FailedPutCount'], len(batch)))
        logger.warn(
            'Failed requests {}'.format(
                json.dumps([request for request in response['RequestResponses'] if 'ErrorCode' in request])
            )
        )
