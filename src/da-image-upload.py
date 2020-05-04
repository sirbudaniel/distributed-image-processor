import json
import boto3

def lambda_handler(event, context):

    imageName = event['queryStringParameters']['imageName']
    s3_client = boto3.client('s3', region_name='eu-west-3')
    try:
        response = s3_client.generate_presigned_post(
            'da-image-processing',
            '{}/raw/{}'.format(imageName, imageName)
        )

    except ClientError as e:
        logging.error(e)
        return None


    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
