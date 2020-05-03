import boto3
import cv2
import math
import numpy as np

imageInfoUrl = 'https://sqs.eu-west-2.amazonaws.com/005261323353/image-info'
queueUrl = 'https://sqs.eu-west-2.amazonaws.com/005261323353/raw-image'
snsTopicApply = 'arn:aws:sns:eu-west-2:005261323353:apply-filters-topic'
snsTopicGather = 'arn:aws:sns:eu-west-2:005261323353:final-gather'

sharpenFilter = [[-1, -1, -1, -1, -1],
                 [-1,  2,  2,  2, -1],
                 [-1,  2,  8,  2, -1],
                 [-1,  2,  2,  2, -1],
                 [-1, -1, -1, -1, -1]]
sharpenFilterFactor = 1.0 / 8.0

filters = {'sharpen': sharpenFilter}
filterFactors = {'sharpen': sharpenFilterFactor}

sqs = boto3.client('sqs', region_name="eu-west-2")
response = sqs.receive_message(
    QueueUrl=imageInfoUrl,
    MessageAttributeNames=[
        'Filter'
    ],
    WaitTimeSeconds=20,
)
message = response['Messages'][0]
receiptHandle = message['ReceiptHandle']
imageName = message['Body']
filterName = message['MessageAttributes']['Filter']['StringValue']


s3ImagePath = '{}/raw/{}'.format(imageName, imageName)
boto3.resource('s3').Bucket('da-image-processing').Object(s3ImagePath).download_file('down.ppm')

sns = boto3.client('sns', region_name="eu-west-2")

with open('down.ppm', 'rb') as f:


    header1 = f.readline()
    ppmHeader = header1
    print(header1.decode('utf-8'))
    header2 = f.readline()
    ppmHeader += header2
    print(header2.decode('utf-8'))
    res = [int(i) for i in header2.split() if i.isdigit()]
    (imageWidth, imageHeight) = res
    header3 = f.readline()
    ppmHeader += header3
    print(header3.decode('utf-8'))
    f.seek(0, 0)

    imageWidth

    chunkSize = math.ceil(10000 / imageWidth)
    toReceiveMessages = str(math.ceil(imageHeight/chunkSize))
#####

    imageBytes = f.read()
    decodedBGR = cv2.imdecode(np.frombuffer(imageBytes, np.uint8), -1)
    decoded = cv2.cvtColor(decodedBGR , cv2.COLOR_BGR2RGB)

    numWorkers = math.ceil(imageHeight / chunkSize)

    messages = []
    for i in range(numWorkers):
        messages.append([])

    i = 0
    currentChunk = 0
    currentChunkLimit = chunkSize
    for row in decoded:
        parsedRow = []
        for pixel in row:
            pixelComp = []
            for rgb in pixel:
                pixelComp.append(int(rgb))
            parsedRow.append(pixelComp)
        if i != 0 and i == currentChunkLimit - chunkSize:  # for margins
            messages[currentChunk - 1].append(parsedRow)
            messages[currentChunk].append(messages[currentChunk - 1][-4])
            messages[currentChunk].append(messages[currentChunk - 1][-3])
            messages[currentChunk].append(messages[currentChunk - 1][-2])
            messages[currentChunk].append(messages[currentChunk - 1][-1])
        if i != 0 and i == currentChunkLimit - chunkSize + 1:  # for margins
            messages[currentChunk - 1].append(parsedRow)
        if i != 0 and i == currentChunkLimit - chunkSize + 2:  # for margins
            messages[currentChunk - 1].append(parsedRow)
        if i != 0 and i == currentChunkLimit - chunkSize + 3:  # for margins
            messages[currentChunk - 1].append(parsedRow)
        messages[currentChunk].append(parsedRow)

        i += 1
        if i >= currentChunkLimit:
            currentChunk += 1
            currentChunkLimit += chunkSize

    # print(messages)
    imageFilter = str(filters[filterName])
    imageFilterFactor = str(filterFactors[filterName])

    i = 0

    print(len(messages))

    sns.publish(
        TopicArn=snsTopicGather,
        Message=imageName,
        MessageAttributes={
            'Header': {
                'DataType': 'String',
                'StringValue': ppmHeader.decode()
            },
            'ToReceiveMessages': {
                'DataType': 'String',
                'StringValue': toReceiveMessages
            },
            'ChunkSize': {
                'DataType': 'String',
                'StringValue': str(chunkSize)
            }
        }
    )

    for message in messages:
        sqs.send_message(
            QueueUrl=queueUrl,
            MessageBody=str(message),
            DelaySeconds=0,
            MessageAttributes={
                'Id': {
                    'DataType': 'String',
                    'StringValue': "".join([imageName, ';', str(i)])
                },
                'Filter': {
                    'DataType': 'String',
                    'StringValue': imageFilter
                },
                'FilterFactor': {
                    'DataType': 'String',
                    'StringValue': imageFilterFactor
                },
                'ToReceiveMessages': {
                    'DataType': 'String',
                    'StringValue': toReceiveMessages
                }
            }
        )
        sns.publish(
            TopicArn=snsTopicApply,
            Message="".join([imageName, ';', str(i)])
        )
        i += 1


sqs.delete_message(
    QueueUrl=imageInfoUrl,
    ReceiptHandle=receiptHandle
)


# ppmHeader = b'P6\n' + b'500 333\n' + b'255\n'

# dec = ppmHeader.decode()
# print(type(dec))
# enc = dec.encode()
# print(type(enc))
# import array
# im = []
# for message in messages:
#     for row in message:
#         for pixel in row:
#             for rgb in pixel:
#                 im.append(rgb)
#
# image = array.array('B', im)
#
# with open('done.ppm', 'wb') as f:
#     f.write(bytearray(enc))
#     image.tofile(f)
