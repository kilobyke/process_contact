import json
import boto3
import botocore
import urllib.parse 
from botocore.exceptions import ClientError
from botocore.client import Config

def lambda_handler(event, context):
    #print('Received event: ' + json.dumps(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    raw_key = event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote(raw_key)
    trimmed_key = key.find('_')
    global new_file
    global new_key
    if trimmed_key != -1:
        new_key = key[:trimmed_key]
        new_file = new_key + '.wav'
        print(new_key)
        print(new_file)
        
    try:
        s3_client = boto3.client('s3')
        copy_source = {
            'Bucket': bucket,
            'Key': key
            }
        response = s3_client.copy(CopySource = copy_source, Bucket = bucket, Key = new_file)
        #remove = s3_client.delete_object(Bucket = bucket, Key = key)
        contact_id = new_key[11:]
        recording_url = 'https://'+bucket+'.s3.amazonaws.com/'+new_file
        print(contact_id)
        print(recording_url)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print('Filename processed')
        
    try:
        s3_resource = boto3.resource('s3')
        object_acl = s3_resource.ObjectAcl(bucket, new_file)
        acl_response = object_acl.put(ACL='public-read')
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print('Object acl updated')
        
    try:
        db_client = boto3.client('dynamodb')
        db_response = db_client.update_item(
            TableName = 'voicemailContactDetails',
            Key={
                'contactId': {
                    'S': contact_id
                }
            },
            AttributeUpdates={
                'audioFromCustomer': {
                    'Value': {
                        'S': recording_url
                    }
                }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print('Data updated')
