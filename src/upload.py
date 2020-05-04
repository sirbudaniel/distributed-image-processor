import requests
import json
import sys
import boto3

imageName = sys.argv[1]
filterName = sys.argv[2]
print('File to be uploaded {} with filter {}'.format(imageName, filterName))

fileName = '../resources/{}'.format(imageName)

uploadLambdaUrl = 'https://0o47to4t6g.execute-api.eu-west-2.amazonaws.com/default/da-image-processing-upload?imageName={}'.format(imageName)
apiKey = API_KEY
header = {'x-api-key': apiKey}

uploadResponse = requests.post(uploadLambdaUrl, headers=header)
response = json.loads(uploadResponse.text)
# print(response)

with open(fileName, 'rb') as f:
    files = {'file': (imageName, f)}
    http_response = requests.post(response['url'], data=response['fields'], files=files)

print(http_response)

lambdaClient = boto3.client('lambda')
lambdaClient.invoke(FunctionName="arn:aws:lambda:eu-west-2:005261323353:function:da-image-distribute",
                    InvocationType='Event',
                    Payload=json.dumps({'imageName': imageName, 'filterName': filterName}))

print("Uploaded successfully!")
