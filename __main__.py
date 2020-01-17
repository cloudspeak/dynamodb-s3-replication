import json
import pulumi
from firehosePolicy import getFirehoseRolePolicyDocument, getFirehoseRoleTrustPolicyDocument
from lambdaPolicy import getLambdaRoleTrustPolicyDocument, getAllowDynamoStreamPolicyDocument
from pulumi_aws import dynamodb, s3, kinesis, iam, lambda_, get_caller_identity

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

lambdaRole = iam.Role('ReplicationLambdaRole',
    assume_role_policy=getLambdaRoleTrustPolicyDocument()
)

lambdaRoleBasicExecutionPolicy = iam.RolePolicyAttachment('ReplicationLambdaBasicExecPol',
    role=lambdaRole.name,
    policy_arn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
)

lambdaRoleAllowDynamoStreamPolicy = iam.RolePolicy("ReplicationLambdaAllowDynamoPolicy",
    role=lambdaRole.name,
    policy=getAllowDynamoStreamPolicyDocument(dynamoTable.stream_arn).apply(lambda d: json.dumps(d))
)

dynamoTriggerFunction = lambda_.Function('ReplicationLambdaFunction',
    role=lambdaRole.arn,
    runtime='python3.7',
    handler='dynamoTriggerLambda.handler',
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./dynamoTriggerLambda"),
    }),
)

dynamoTrigger = lambda_.EventSourceMapping("ReplicationDynamoTriggerMapping",
    event_source_arn=dynamoTable.stream_arn,
    function_name=dynamoTriggerFunction.arn,
    starting_position='LATEST'
)
