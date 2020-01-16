import pulumi
from pulumi_aws import dynamodb

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
