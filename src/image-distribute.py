import json
import boto3

def lambda_handler(event, context):

    imageName = event['imageName']
    filterName = event['filterName']

    imageInfoUrl= 'https://sqs.eu-west-2.amazonaws.com/005261323353/image-info'
    sqs = boto3.client('sqs', region_name="eu-west-2")
    sqs.send_message(
        QueueUrl=imageInfoUrl,
        MessageBody=imageName,
        DelaySeconds=0,
        MessageAttributes={
            'Filter': {
                'DataType': 'String',
                'StringValue': filterName
            }
        }
    )

    ecs_client = boto3.client('ecs', region_name='eu-west-2')
    ecs_client.run_task(
        cluster='da-image-processing',  # name of the cluster
        launchType='FARGATE',
        taskDefinition='da-image-processing:1',
        count=1,
        platformVersion='LATEST',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    'subnet-07489018e273f5f59',  # replace with your public subnet or a private with NAT
                ],
                'assignPublicIp': 'ENABLED'
            }
        })

    return {
        'statusCode': 200,
        'body': json.dumps('distributed')
    }
