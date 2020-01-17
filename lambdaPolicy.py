def getLambdaRoleTrustPolicyDocument():
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

def getAllowDynamoStreamPolicyDocument(streamArn):
    return streamArn.apply(lambda arn: {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetShardIterator",
                    "dynamodb:DescribeStream",
                    "dynamodb:GetRecords"
                ],
                "Resource": arn
            },
            {
                "Effect": "Allow",
                "Action": "dynamodb:ListStreams",
                "Resource": "*"
            }
        ]
    })

def getAllowFirehosePutPolicyDocument(firehoseArn):
    return firehoseArn.apply(lambda arn: {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "firehose:PutRecord",
                    "firehose:PutRecordBatch"
                ],
                "Resource": arn
            }
        ]
    })