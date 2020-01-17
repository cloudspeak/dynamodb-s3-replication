import json

def handler(event, context):
    
    print("Event:")
    print(event)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
