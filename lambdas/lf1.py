import json
import boto3
import os
from elasticsearch import Elasticsearch 

def index_doc(index_name='index-photos', document={'temp': 'temp'}):

    client = Elasticsearch(
        hosts=[{'host': os.environ.get('ES_HOST'), 'port': 443}],
        http_auth=[os.environ.get('ES_USERNAME'), os.environ.get('ES_PASSWORD')],
        scheme="https",
        port=443    
    )
    
    response = client.index(
        index= index_name,
        body=document
    )

    print(response)
    return response

def lambda_handler(event, context):
    print(event)

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    image_name = event['Records'][0]['s3']['object']['key']
    
    print(bucket_name, image_name)
    rekognition = boto3.client('rekognition')
    s3 = boto3.client('s3')
    

    labels_reponse = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': image_name,
            }
        }
    )
    
    print(labels_reponse)
    
    labels = [label['Name'] for label in labels_reponse['Labels']]
    
    print("Calling head")
    head_response = s3.head_object(
        Bucket= bucket_name,
        Key =  image_name
        ) 
        
    print(head_response)

    if head_response['Metadata'] and head_response['Metadata']['customlabels'] not in labels:
        labels.append(head_response['Metadata']['customlabels'])
    

    print(labels)
    timestamp = head_response['LastModified'].strftime("%Y-%m-%dT%H:%M:%S")
    data = {
        "objectKey": image_name, 
        "bucket": bucket_name,
        "createdTimestamp": timestamp,
        "labels": labels
}


    response = index_doc(os.environ['ES_INDEX_NAME'], data)

    
    print(response)
    # TODO implement
    print("Lambda invoked")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }