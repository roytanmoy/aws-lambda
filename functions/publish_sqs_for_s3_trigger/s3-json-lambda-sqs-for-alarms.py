
import json, boto3
s3_client = boto3.client('s3')
sqs_client = boto3.resource('sqs')

def lambda_handler(event, context):
    if event:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_name = event['Records'][0]['s3']['object']['key']
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_contents = file_obj['Body'].read().decode('utf-8')
        # print('file_contents:', file_contents)
        records = processData(file_contents)
        print('records:', records)

    queue = sqs_client.create_queue(QueueName='Alarm_Queue')
    queue_object = sqs_client.get_queue_by_name(QueueName='Alarm_Queue')
    # queues = sqs_client.list_queues(QueueNamePrefix='Alarm_Queue')
    # queue_url = queues['QueueUrls'][0]
    # enqueue_response  = sqs_client.send_message(QueueUrl=queue_url, MessageBody=file_contents)
    for record in records:
        queue_response = queue_object.send_message(MessageBody=record)


def processData(data):
    finaldata = []
    data = json.dumps(data)
    print('data', data)
    for item in data:
        if item['temperature'] > 45:
            record = {}
            record['timestamp'] = item['timestamp']
            record['sensorid'] = item['sensorid']
            if item['temperature'] >= 55:
                record['severity'] = 'CRITICAL'
            else:
                record['severity'] = 'MAJOR'
            finaldata.append(record)
    return finaldata