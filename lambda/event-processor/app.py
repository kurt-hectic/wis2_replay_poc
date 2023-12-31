import json
import logging
import os
import string
import random
import boto3

from datetime import datetime, timedelta

log_level = os.getenv("LAMBDA_LOG_LEVEL", "INFO")
step_minutes = int(os.getenv("STEP_MINUTES","5"))
level = logging.getLevelName(log_level)
queue_url = os.getenv("QUEUE_URL")

logger = logging.getLogger(__name__)
logger.setLevel(level)

ddb_name = os.getenv("DDB_NAME",None)
dynamodb_client = boto3.client('dynamodb')


client = boto3.client('iot-data')
sqs_client = boto3.client('sqs')

def process_subscription(topic):
    
    replay_topic_part = topic.split("/")[0]
    
    (_,_,_,_,d_from,d_to) = replay_topic_part.split("-")
    real_topic = "cache/a/wis2/" + "/".join(topic.split('/')[1:])
    base_topic = replay_topic_part
    client_id = "-".join(replay_topic_part.split("-")[0:3])

    d_from = datetime.strptime(d_from,"%Y%m%d%H%M")
    d_to = datetime.strptime(d_to,"%Y%m%d%H%M")

    notification_topic = topic if not topic.endswith("#") else topic.replace("#","replay-info")

    message = f"replaying topic {real_topic} from {d_from} {d_to} to {topic}"

    logger.info(message)

    jobid = ''.join(random.choice(string.ascii_lowercase) for i in range(16)) 

    reponse = dynamodb_client.put_item(
        TableName=ddb_name,
        Item={
            "replaytopic" : { "S" : client_id } ,
            "currentJobId" : { "S" : jobid }
        }
    )

    response = client.publish(
        topic=notification_topic,
        qos=0,
        payload=json.dumps({"message": message})
    )

    date_of_range = [d_from + timedelta(minutes=delta) for delta in range(0, int((d_to - d_from) / timedelta(minutes=1))+1, step_minutes )]

    for i,d in enumerate(date_of_range):

        msg_body = { 
                    "from" : d.isoformat() , 
                    "to" : (d + timedelta(minutes=step_minutes)).isoformat() , 
                    "base_topic" :  base_topic,
                    "real_topic" : real_topic,
                    "job_id" : jobid,
                    "replay_topic" : replay_topic_part
            }
        msg_attr = {
                    "clientId" : {
                    "StringValue" : client_id,
                    "DataType" : "String"
                    }
        }

        logger.info("sending %s and %s to %s",msg_body,msg_attr,queue_url)

        sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody= json.dumps(msg_body),
            MessageAttributes= msg_attr,
            DelaySeconds=i*5
        )


def process_disconnection(client_id):
    logger.info("process disconnect %s",client_id)

    reponse = dynamodb_client.delete_item(
        TableName=ddb_name,
        Key={"replaytopic" : { "S" : client_id}},
    )


def handler(event, context):
    logger.info("Lambda function invoked %s",json.dumps(event))

    for record in event["Records"]:
        payload = json.loads(record["body"])
        logger.debug("payload %s",payload)

        client_id = payload["clientId"]
        
        if payload["eventType"] == "subscribed":
            process_disconnection(client_id)

            for topic in payload["topics"]:
                process_subscription(topic)

        if payload["eventType"] == "unsubscribed" or payload["eventType"] == "disconnected":
            process_disconnection(client_id)


    return