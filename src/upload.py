import requests
import json

fileName = '../resources/file1.ppm'
s3UploadPath = 'file1.ppm'
filterName = 'sharpen'

uploadLambdaUrl = 'https://0o47to4t6g.execute-api.eu-west-2.amazonaws.com/default/da-image-processing-upload?imageName={}'.format(s3UploadPath)
apiKey = API_KEY
header = {'x-api-key': apiKey}

uploadResponse = requests.post(uploadLambdaUrl, headers=header)
response = json.loads(uploadResponse.text)
print(response)

with open(fileName, 'rb') as f:
    files = {'file': (s3UploadPath, f)}
    http_response = requests.post(response['url'], data=response['fields'], files=files)


print(http_response)

print(f'File upload HTTP status code: {http_response.status_code}')

distributeLambdaUrl = 'https://0o47to4t6g.execute-api.eu-west-2.amazonaws.com/default/da-image-distribute?imageName={}&filterName={}'.format(s3UploadPath, filterName)
apiKey = API_KEY
header = {'x-api-key': apiKey}
requests.post(distributeLambdaUrl, headers=header)
