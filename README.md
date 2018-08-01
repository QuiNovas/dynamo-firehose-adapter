# dynamo-firehose-adapter
A simple adapter that takes records from a DynamoDB stream and sends them to a Kinesis Firehose for processing.
Note - this works best with larger "chunks" from the DynamoDB stream, as the adapter uses batch processing of events onto the Firehose.

# Environment Variables:
- **DELIVERY_STREAM_NAME** The name of the Kinesis Firehose to deliver the DynamoDB records to
- **DYNAMNODB_IMAGE_TYPE** Can either be `NewImage` or `OldImage`. This corresponds to the records in the DynamoDB Stream

# Handler Method
function.handler


