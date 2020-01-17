import unittest
import json
from dynamoTriggerLambda.dynamoTriggerLambda import dynamoInsertEventsToFirehoseRecords

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
