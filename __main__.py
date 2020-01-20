import json
import pulumi
from firehosePolicy import getFirehoseRolePolicyDocument, getFirehoseRoleTrustPolicyDocument
from lambdaPolicy import getLambdaRoleTrustPolicyDocument, getAllowDynamoStreamPolicyDocument, getAllowFirehosePutPolicyDocument
from pulumi_aws import dynamodb, s3, kinesis, iam, lambda_, get_caller_identity, get_region, config

accountId = get_caller_identity().account_id
region = config.region

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
    assume_role_policy=getFirehoseRoleTrustPolicyDocument(accountId)
)

deliveryStream = kinesis.FirehoseDeliveryStream('ReplicationDeliveryStream',
    destination='extended_s3',
    extended_s3_configuration={
        'bucketArn': bucket.arn,
        'role_arn': firehoseRole.arn,
        'compressionFormat': 'GZIP'
    }
)

firehoseRolePolicy = iam.RolePolicy('ReplicationDeliveryStreamPolicy',
        role=firehoseRole.name,
        policy=getFirehoseRolePolicyDocument(region, accountId, bucket.arn, deliveryStream.name).apply(lambda d: json.dumps(d))
)

lambdaRole = iam.Role('ReplicationLambdaRole',
    assume_role_policy=getLambdaRoleTrustPolicyDocument()
)

lambdaRoleBasicExecutionPolicy = iam.RolePolicyAttachment('ReplicationLambdaBasicExecPolicy',
    role=lambdaRole.name,
    policy_arn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
)

lambdaRoleAllowDynamoStreamPolicy = iam.RolePolicy("ReplicationLambdaAllowDynamoPolicy",
    role=lambdaRole.name,
    policy=getAllowDynamoStreamPolicyDocument(dynamoTable.stream_arn).apply(lambda d: json.dumps(d))
)

lambdaRoleAllowFirehosePutPolicy = iam.RolePolicy("ReplicationLambdaAllowFirehosePolicy",
    role=lambdaRole.name,
    policy=getAllowFirehosePutPolicyDocument(deliveryStream.arn).apply(lambda d: json.dumps(d))
)

dynamoTriggerFunction = lambda_.Function('ReplicationLambdaFunction',
    role=lambdaRole.arn,
    runtime='python3.7',
    handler='dynamoTriggerLambda.handler',
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./dynamoTriggerLambda"),
    }),
    environment={
        "Variables": {
            "DELIVERY_STREAM_NAME": deliveryStream.name
        }
    }
)

dynamoTrigger = lambda_.EventSourceMapping("ReplicationDynamoTriggerMapping",
    event_source_arn=dynamoTable.stream_arn,
    function_name=dynamoTriggerFunction.arn,
    starting_position='LATEST'
)

pulumi.export('table_name', dynamoTable.name)
pulumi.export('bucket_name', bucket.id)
pulumi.export('delivery_stream_name', deliveryStream.name)
