import json

def lambda_handler(event, context):
    import boto3
    import ast
    import copy

    snsMessage = event['Records'][0]['Sns']['Message']


    rawQueueUrl = 'https://sqs.eu-west-2.amazonaws.com/005261323353/raw-image'
    doneQueueUrl = 'https://sqs.eu-west-2.amazonaws.com/005261323353/done-image'

    sqs = boto3.client('sqs', region_name="eu-west-2")

    while 1:
        response = sqs.receive_message(
            QueueUrl=rawQueueUrl,
            MessageAttributeNames=[
                'Id', 'Filter', 'FilterFactor', 'ToReceiveMessages'
            ],
            WaitTimeSeconds=5,
        )
        if not response.get('Messages'): # TODO make this not stop while usign lambda functions
            continue
        message = response['Messages'][0]
        receiptHandle = message['ReceiptHandle']
        rawImage = message['Body']
        id = message['MessageAttributes']['Id']['StringValue']
        if (id != snsMessage):
            continue
        imageFilter = message['MessageAttributes']['Filter']['StringValue']
        imageFilterFactor = float(message['MessageAttributes']['FilterFactor']['StringValue'])
        toReceiveMessages = float(message['MessageAttributes']['ToReceiveMessages']['StringValue'])

        receivedId = int(id.split(';')[1])
        imageFilterMatrix = ast.literal_eval(imageFilter)
        filterHeight = len(imageFilterMatrix)
        if filterHeight == 0:
            print('emptyFilter')
            exit()

        filterWidth = len(imageFilterMatrix[0])

        imageMatrix = ast.literal_eval(rawImage)

        imageHeight = len(imageMatrix)
        if imageHeight == 0:
            print('emptyImage')
            exit()

        imageWidth = len(imageMatrix[0])

        x = 0
        y = 0

        resultImage = copy.deepcopy(imageMatrix)

        for x in range(imageWidth):
            for y in range(imageHeight):
                red = 0.0
                green = 0.0
                blue = 0.0

                for filterY in range(filterHeight):
                    for filterX in range(filterWidth):
                        imageX = int((x - filterWidth / 2 + filterX + imageWidth) % imageWidth)
                        imageY = int((y - filterHeight / 2 + filterY + imageHeight) % imageHeight)

                        red += imageMatrix[imageY][imageX][0] * imageFilterMatrix[filterY][filterX]
                        green += imageMatrix[imageY][imageX][1] * imageFilterMatrix[filterY][filterX]
                        blue += imageMatrix[imageY][imageX][2] * imageFilterMatrix[filterY][filterX]
                resultImage[y][x][0] = min(max(int(imageFilterFactor * red), 0), 255)
                resultImage[y][x][1] = min(max(int(imageFilterFactor * green), 0), 255)
                resultImage[y][x][2] = min(max(int(imageFilterFactor * blue), 0), 255)

        if receivedId == 0:
            resultImage = resultImage[:-4]
        elif receivedId == toReceiveMessages - 1: #TODO chunk size
            resultImage = resultImage[4:]
        else:
            resultImage = resultImage[4:-4]

        print('{} {}'.format(receivedId, len(resultImage)))
        sqs.send_message(
            QueueUrl=doneQueueUrl,
            MessageBody=str(resultImage),
            DelaySeconds=0,
            MessageAttributes={
                'Id': {
                    'DataType': 'String',
                    'StringValue': id
                }
            }
        )

        sqs.delete_message(
            QueueUrl=rawQueueUrl,
            ReceiptHandle=receiptHandle
        )
        exit()
