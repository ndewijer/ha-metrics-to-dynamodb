import boto3
from botocore.exceptions import ClientError
import os
import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Place Access Key ID, Secret and Region in environment variables
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_region = os.environ['AWS_REGION']
dynamodb_database = os.environ['DYNAMODB_DATABASE']

if __name__ == '__main__':
    try:
        # Connect AWS DynamoDB

        session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        ddb = session.resource('dynamodb', region_name=aws_region)
        table = ddb.Table(dynamodb_database)
    except ClientError as err:
        logger.error(
            "Couldn't connect to DynamoDB. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

    # query all items between a range from DynamoDB where last_updated_ts is sort key and metadata_id is partition key
    response = table.query(
        KeyConditionExpression='metadata_id = :metadata_id and last_updated_ts between :start_ts and :end_ts',
        ExpressionAttributeValues={
            ':metadata_id': 18,
            ':start_ts': '1689220696.664104',
            ':end_ts': '1689222706.728989'
            }
        )
    print(response)