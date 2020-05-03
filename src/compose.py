import json

def lambda_handler(event, context):
    import boto3
    import ast
    import array

    doneQueueUrl = 'https://sqs.eu-west-2.amazonaws.com/005261323353/done-image'

    sqs = boto3.client('sqs', region_name="eu-west-2")
    snsEvent = event['Records'][0]['Sns']

    toReceiveImage = snsEvent['Message']
    toReceiveMessages = int(snsEvent['MessageAttributes']['ToReceiveMessages']['Value'])
    chunkSize = int(snsEvent['MessageAttributes']['ChunkSize']['Value'])
    ppmHeader = snsEvent['MessageAttributes']['Header']['Value']

    s3ImagePath = '{}/done/{}'.format(toReceiveImage, toReceiveImage)

    # ppmHeader = b'P6\n' + b'500 333\n' + b'255\n'

    messages = []
    receivedIds = []
    for i in range(toReceiveMessages):
        messages.append([])
        receivedIds.append(False)

    while toReceiveMessages > 0:
        response = sqs.receive_message(
            QueueUrl=doneQueueUrl,
            MessageAttributeNames=[
                'Id'
            ],
            WaitTimeSeconds=10,
        )
        if not response.get('Messages'): # TODO make this not stop while usign lambda functions
            continue

        message = response['Messages'][0]
        receiptHandle = message['ReceiptHandle']
        rawImage = message['Body']
        (receivedImage, receivedId) = message['MessageAttributes']['Id']['StringValue'].split(';')

        if receivedImage != toReceiveImage:
            continue
        # if receivedIds[int(receivedId)]:
        #     continue
        receivedIds[int(receivedId)] = True

        print('received id: {}'.format(receivedId))
        messages[int(receivedId)] = ast.literal_eval(rawImage)

        sqs.delete_message(
            QueueUrl=doneQueueUrl,
            ReceiptHandle=receiptHandle
        )

        toReceiveMessages -= 1


    im = []
    for message in messages:
        for row in message:
            for pixel in row:
                for rgb in pixel:
                    im.append(rgb)

    image = array.array('B', im)


    print('finished receiving')

    with open('/tmp/done.ppm', 'wb') as f:
        f.write(bytearray(ppmHeader.encode()))
        image.tofile(f)

    s3 = boto3.resource('s3').Bucket('da-image-processing')
    s3.upload_file('/tmp/done.ppm', s3ImagePath)
