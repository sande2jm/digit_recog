import time
import boto3
import decimal
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('test')
response = table.update_item(
    Key={
    'id': 'i-662314313efsd'
    },
    UpdateExpression='SET #x = :val1, #y = :val2, #z = :val3, #p = :val4',
    ExpressionAttributeNames={
        '#x': 'learning_rate',
        '#y': 'run_time',
        '#z': 'epochs',
        '#p': 'dropout_rate'

    },
    ExpressionAttributeValues={
        ':val1': decimal.Decimal('.01'),
        ':val2': decimal.Decimal('112.5'),
        ':val3': decimal.Decimal('15'),
        ':val4': decimal.Decimal('.2')
    }
)
print(response)