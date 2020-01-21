import sys
sys.path.append("./dynamoTriggerLambda")

import unittest
import json
from unittest.mock import MagicMock, Mock
from dynamoTriggerLambda import dynamoEventsToFirehoseRecords, sendRecordsToFirehose
from dynamoEventNames import DynamoEventName

class DynamoTriggerLambdaTests(unittest.TestCase):

    def test_when_insert_event_occurs_then_convert_to_firehose_record(self):
        inputEvent = {
            "Records": [
                self.createDynamoEvent(1, "Test1")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1")
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_modify_event_occurs_then_convert_to_firehose_record(self):
        inputEvent = {
            "Records": [
                self.createDynamoEvent(1, "Test1", eventName=DynamoEventName.MODIFY)
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1", operation=DynamoEventName.MODIFY)
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_remove_event_occurs_then_convert_to_firehose_record(self):
        inputEvent = {
            "Records": [
                self.createDynamoEvent(1, "Test1", eventName=DynamoEventName.REMOVE)
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1", operation=DynamoEventName.REMOVE)
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_multiple_insert_events_occurs_then_convert_to_firehose_record(self):
        inputEvent = {
            "Records": [
                self.createDynamoEvent(1, "Test1"),
                self.createDynamoEvent(2, "Test2", eventName=DynamoEventName.MODIFY),
                self.createDynamoEvent(3, "Test3", eventName=DynamoEventName.REMOVE)
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1"),
            self.createFirehoseRecord(2, "Test2", operation=DynamoEventName.MODIFY),
            self.createFirehoseRecord(3, "Test3", operation=DynamoEventName.REMOVE)
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_empty_records_then_return_empty_list(self):
        inputEvent = {
            "Records": []
        }

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual([], actualOutput)


    def test_when_non_dynamo_event_then_ignore(self):
        inputEvent = {
            "Records": [
                {
                    "eventSource": "notdynamo"
                },
                self.createDynamoEvent(1, "Test1")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1")
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_non_relevant_event_then_ignore(self):
        inputEvent = {
            "Records": [
                {
                    "eventName": "NOTRELEVANT",
                    "eventSource": "aws:dynamodb"
                },
                self.createDynamoEvent(1, "Test1")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1")
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)

    def test_when_integer_data_then_formatted_correctly(self):
        inputEvent = {
            "Records": [
                self.createDynamoEvent(1, "234", "N")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, 234)
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)

        self.assertEqual(expectedOutput, actualOutput)

    def test_when_float_data_then_formatted_correctly(self):
        inputEvent = {
            "Records": [
                self.createDynamoEvent(1, "234.567", "N")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, 234.567)
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)

        self.assertEqual(expectedOutput, actualOutput)


    def test_when_delete_event_with_string_id_then_formatted_correctly(self):
        inputEvent = {
            "Records": [
                self.createDynamoEvent(1, "Test1", idDataType="S", eventName=DynamoEventName.REMOVE)
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord("1", "Test1", operation=DynamoEventName.REMOVE)
        ]

        actualOutput = dynamoEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_call_send_records_then_client_is_called_correctly(self):

        mockClient = Mock()
        mockClient.put_record_batch.return_value = {
            "FailedRecordCount": 0
        }

        records = [
            self.createFirehoseRecord(1, "Test1"),
            self.createFirehoseRecord(2, "Test2")
        ]

        sendRecordsToFirehose(mockClient, "streamname", records)

        mockClient.put_record_batch.assert_called_once_with(
            DeliveryStreamName="streamname",
            Records=records
        )

    def test_when_call_send_records_and_empty_records_then_client_not_called(self):

        mockClient = Mock()
        sendRecordsToFirehose(mockClient, "streamname", [])

        mockClient.put_record_batch.assert_not_called()


    ## Helper methods

    def createDynamoEvent(self, id, data, dataType="S", idDataType="N", eventName = DynamoEventName.INSERT):
        result = {
            "dynamodb": {
                "Keys": {
                    "Id": {
                        idDataType: f'{id}'
                    }
                }
            },
            "eventName": eventName,
            "eventSource": "aws:dynamodb"
        }

        if eventName != DynamoEventName.REMOVE:
            result["dynamodb"]["NewImage"] = {
                "Value": {
                    dataType: data
                },
                "Id": {
                    idDataType: f'{id}'
                }
            }

        return result
    
    def createFirehoseRecord(self, id, value, operation = DynamoEventName.INSERT):
        
        recordData = {
            "Operation": operation,
        }

        if operation == DynamoEventName.REMOVE:
            recordData["Id"] = id
        else:
            recordData["Data"] = {
                "Value": value,
                "Id": id
            }
        
        return {
            "Data": json.dumps(recordData)
        }

if __name__ == '__main__':
    unittest.main()
