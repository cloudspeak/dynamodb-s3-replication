import json
import pulumi
from firehosePolicy import getFirehoseRolePolicyDocument, getFirehoseRoleTrustPolicyDocument
from pulumi_aws import dynamodb, s3, kinesis, iam, get_caller_identity

dynamoTable = dynamodb.Table('ReplicationTable',
    attributes=[{
        'Name': 'Id',
        'Type': 'S'
    }],
    hash_key='Id',
    billing_mode='PAY_PER_REQUEST',
    stream_enabled=True,
    stream_view_type='NEW_IMAGE'
)

bucket = s3.Bucket('ReplicationBucket')

firehoseRole = iam.Role('ReplicationFirehoseRole',
    name='ReplicationFirehoseRole',
    assume_role_policy=getFirehoseRoleTrustPolicyDocument(get_caller_identity().account_id)
)

firehoseRolePolicy = iam.RolePolicy('ReplicationFirehosePolicy',
        role=firehoseRole.name,
        policy=json.dumps(getFirehoseRolePolicyDocument())
)

kinesis.FirehoseDeliveryStream('ReplicationDeliveryStream',
        name='ReplicationDeliveryStream',
        destination='extended_s3',
        extended_s3_configuration={
            'bucketArn': bucket.arn,
            'role_arn': firehoseRole.arn,
            'compressionFormat': 'GZIP'
        }
        )
