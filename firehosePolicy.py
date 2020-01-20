import pulumi

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

def getFirehoseRolePolicyDocument(region, accountId, bucketArn, deliveryStreamName):
    """ Returns a role permitting Firehose to read Dynamo tables and write to S3
    
        region -- The AWS region as a string
        accountID -- The AWS account ID as a string
        bucketArn -- The destination bucket ARN as a Pulumi Output
        deliveryStringName -- The name of the Firehose delivery stream as a Pulumi Output
    """

    return pulumi.Output.all(bucketArn, deliveryStreamName).apply(lambda outputs: {
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
                    outputs[0],
                    f'{outputs[0]}/*',
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
                    f'arn:aws:logs:{region}:{accountId}:log-group:/aws/kinesisfirehose/{outputs[1]}:log-stream:*'
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
                "Resource": f'arn:aws:kinesis:{region}:{accountId}:stream/%FIREHOSE_STREAM_NAME%'
            }
        ]
    })

