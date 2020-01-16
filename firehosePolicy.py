def getFirehoseRoleTrustPolicyDocument(accountId):
    """Returns a trust (AssumeRole) policy allowing the firehose service for a given account"""

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "firehose.amazonaws.com"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": f'{accountId}'
                }
            }
            }
        ]
    }

def getFirehoseRolePolicyDocument():
    """ Returns a role permitting Firehose to read Dynamo tables and write to S3 """

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "glue:GetTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions"
                ],
                "Resource": "*"
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::%FIREHOSE_BUCKET_NAME%",
                    "arn:aws:s3:::%FIREHOSE_BUCKET_NAME%/*"
                ]
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "logs:PutLogEvents"
                ],
                "Resource": [
                    "arn:aws:logs:eu-west-1:558800484112:log-group:/aws/kinesisfirehose/ReplicationDeliveryStream:log-stream:*"
                ]
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": [
                    "kinesis:DescribeStream",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords"
                ],
                "Resource": "arn:aws:kinesis:eu-west-1:558800484112:stream/%FIREHOSE_STREAM_NAME%"
            }
        ]
    }
