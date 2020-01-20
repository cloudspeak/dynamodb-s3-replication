import os
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer
from dynamoEventNames import DynamoEventName, DynamoEventNames

DELIVERY_STREAM_VAR_NAME = "DELIVERY_STREAM_NAME"

def handler(event, context):
    """ This handler consumes DynamoDB INSERT events, converts the records into a more standard
        JSON representation, and puts the result into a Kinesis Firehose stream.
    """

    deliveryStreamName = os.environ[DELIVERY_STREAM_VAR_NAME]
    firehoseClient = boto3.client('firehose')
    firehoseRecords = dynamoEventsToFirehoseRecords(event)
    sendRecordsToFirehose(firehoseClient, deliveryStreamName, firehoseRecords)
    
    return {
        'statusCode': 200,
        'body': 'Success'
    }

def sendRecordsToFirehose(client, deliveryStreamName, records):
    if len(records) > 0:
        response = client.put_record_batch(
            DeliveryStreamName=deliveryStreamName,
            Records=records
        )

        if "FailedPutCount" in response and response["FailedPutCount"] > 0:
            print(f'WARNING: {response["FailedPutCount"]} of {len(records)} records failed when putting to Firehose')
        else:
            print(f'Successfully put {len(records)} records into Firehose delivery stream "{deliveryStreamName}"')

def dynamoEventsToFirehoseRecords(event):
    eventRecords = filter(isRelevantDynamoEvent, event["Records"])
    jsonRecords = map(dynamoEventRecordToJsonRecord, eventRecords)
    firehoseRecords = map(jsonRecordToFirehoseRecord, jsonRecords)
    return list(firehoseRecords)
    
def isRelevantDynamoEvent(event):
    return event["eventSource"] == 'aws:dynamodb' and event["eventName"] in DynamoEventNames

def jsonRecordToFirehoseRecord(record):
    return {
        "Data": json.dumps(record)
    }

def dynamoEventRecordToJsonRecord(record):

    deserializer = CustomTypeDeserializer()

    if record["eventName"] == DynamoEventName.REMOVE:
        return {
            "Operation": record["eventName"],
            "Id": deserializer.deserialize(record["dynamodb"]["Keys"]["Id"])
        }

    else:
        return {
            "Operation": record["eventName"],
            "Data": deserializer.deserialize({
                "M": record["dynamodb"]["NewImage"]
            })
        }

        

class CustomTypeDeserializer(TypeDeserializer):
    """ boto3 has a build in TypeDeserializer, but it converts numbers to decimal types.  This
        class simply overrides this behavior so that it converts numbers to ints or floats
        (which could result in some loss of precision).
    """
    def _deserialize_n(self, value):
        num = float(value)

        if num.is_integer():
            return int(num)
        else:
            return num
