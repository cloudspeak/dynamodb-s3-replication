import unittest
import json
from unittest.mock import MagicMock, Mock
from dynamoTriggerLambda.dynamoTriggerLambda import dynamoInsertEventsToFirehoseRecords, sendRecordsToFirehose

class DynamoTriggerLambdaTests(unittest.TestCase):

    def test_when_event_occurs_then_convert_to_firehose_record(self):
        inputEvent = {
            "Records": [
                self.createDynamoInsertEvent(1, "Test1")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1")
        ]

        actualOutput = dynamoInsertEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)

    def test_when_multiple_events_occurs_then_convert_to_firehose_record(self):
        inputEvent = {
            "Records": [
                self.createDynamoInsertEvent(1, "Test1"),
                self.createDynamoInsertEvent(2, "Test2"),
                self.createDynamoInsertEvent(3, "Test3")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1"),
            self.createFirehoseRecord(2, "Test2"),
            self.createFirehoseRecord(3, "Test3")
        ]

        actualOutput = dynamoInsertEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_empty_records_then_return_empty_list(self):
        inputEvent = {
            "Records": []
        }

        actualOutput = dynamoInsertEventsToFirehoseRecords(inputEvent)
        self.assertEqual([], actualOutput)


    def test_when_non_dynamo_event_then_ignore(self):
        inputEvent = {
            "Records": [
                {
                    "eventSource": "notdynamo"
                },
                self.createDynamoInsertEvent(1, "Test1")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1")
        ]

        actualOutput = dynamoInsertEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)


    def test_when_non_insert_event_then_ignore(self):
        inputEvent = {
            "Records": [
                {
                    "eventName": "NOTINSERT",
                    "eventSource": "aws:dynamodb"
                },
                self.createDynamoInsertEvent(1, "Test1")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, "Test1")
        ]

        actualOutput = dynamoInsertEventsToFirehoseRecords(inputEvent)
        self.assertEqual(expectedOutput, actualOutput)

    def test_when_integer_data_then_formatted_correctly(self):
        inputEvent = {
            "Records": [
                {
                    "eventName": "NOTINSERT",
                    "eventSource": "aws:dynamodb"
                },
                self.createDynamoInsertEvent(1, "234", "N")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, 234)
        ]

        actualOutput = dynamoInsertEventsToFirehoseRecords(inputEvent)

        self.assertEqual(expectedOutput, actualOutput)

    def test_when_float_data_then_formatted_correctly(self):
        inputEvent = {
            "Records": [
                {
                    "eventName": "NOTINSERT",
                    "eventSource": "aws:dynamodb"
                },
                self.createDynamoInsertEvent(1, "234.567", "N")
            ]
        }

        expectedOutput = [
            self.createFirehoseRecord(1, 234.567)
        ]

        actualOutput = dynamoInsertEventsToFirehoseRecords(inputEvent)

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

    def createDynamoInsertEvent(self, id, data, dataType="S"):
        return {
            "dynamodb": {
                "NewImage": {
                    "Data": {
                        dataType: data
                    },
                    "Id": {
                        "S": id
                    }
                }
            },
            "eventName": "INSERT",
            "eventSource": "aws:dynamodb"
        }
    
    def createFirehoseRecord(self, id, data):
        return {
            "Data": json.dumps({
                "Data": data,
                "Id": id
            })
        }


if __name__ == '__main__':
    unittest.main()
