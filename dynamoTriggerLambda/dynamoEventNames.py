class DynamoEventName:
    """ These are the event names triggered by DynamoDB streams """
    INSERT = "INSERT"
    MODIFY = "MODIFY"
    REMOVE = "REMOVE"

DynamoEventNames = [
    DynamoEventName.INSERT,
    DynamoEventName.MODIFY,
    DynamoEventName.REMOVE
]