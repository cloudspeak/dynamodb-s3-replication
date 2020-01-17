# Nuage dynamodb-s3-replication

This project describes a Pulumi stack defining a DynamoDB table, a Firehose delivery stream and an S3 bucket.  Changes in the table are pushed into the delivery stream (by way of the Lambda function in `dynamoTriggerLambda/`), which then delivers the data into the bucket.

To run the unit tests for the Lambda function:

    python -m unittest test.dynamoTriggerLambdaTests