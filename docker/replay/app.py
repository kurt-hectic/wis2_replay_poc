import logging
import json
import os
import boto3
from sqlalchemy import create_engine, text



from datetime import datetime, timedelta, timezone


# Configure logging
logger = logging.getLogger()
log_level = os.getenv("LAMBDA_LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)
logger.setLevel(level)

credential_name = os.environ.get('SECRET_NAME','N/A')
bucket_name = os.environ.get('BUCKET_NAME')
db_override = None if os.environ.get('DB_OVERRIDE','false').lower() in ["false","no", "none"] else os.environ.get('DB_OVERRIDE')
ddb_name = os.getenv("DDB_NAME",None)

cloudwatch = boto3.client('cloudwatch')
client_iot = boto3.client('iot-data')
dynamodb_client = boto3.client('dynamodb')


logger.debug(f"overriding DB for local testing {db_override}")

def get_engine():
    
    secretsmanager = boto3.client('secretsmanager')

    logging.info(f"getting secret {credential_name}")

    secret_result = secretsmanager.get_secret_value(
                    SecretId=credential_name
    )

    secret = json.loads(secret_result["SecretString"])

    if db_override:
        secret["host"] = db_override

    con_string = "mysql+pymysql://{username}:{password}@{host}/{dbname}?charset=utf8mb4".format(**secret)

    logging.info(f"con string {con_string}")

    return create_engine( con_string )


def send_statistics(nr_replayed,cliend_id):
    response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName': 'NumberRecordsReplayed',
                'Dimensions': [
                    {
                        'Name': 'ClientId',
                        'Value': cliend_id
                    },
                ],
                'Unit': 'Count',
                'Value': nr_replayed
            } ,
           


        ],
        Namespace = 'WIS2monitoring'
    )


db_engine = get_engine()

def lambda_handler(event, context):

    logger.info("Lambda function invoked %s",json.dumps(event))

    for record in event["Records"]:
        payload = json.loads(record["body"])
        logger.debug("payload %s",payload)

        jobid = payload["job_id"]
        replay_topic = payload["replay_topic"]
        client_id = "-".join(replay_topic.split("-")[0:3])


        result = dynamodb_client.get_item(
            TableName=ddb_name,
            Key={"replaytopic" : { "S" : client_id}},
            ConsistentRead=True
        )

        try:
            result_jobid = result["Item"]["currentJobId"]["S"]
            if not result_jobid == jobid:
                logger.info("old jobid %s, new jobid is %s, discarding job",jobid,result_jobid) 
                continue
        except KeyError:
            logger.info("no jobid in dynodb, discarding %s",json.dumps(record))
            continue 



        with db_engine.connect() as con:

            base_topic = payload["base_topic"]
            real_topic = payload["real_topic"]

            real_topic = real_topic if not real_topic.endswith("#") else real_topic.replace('#','%')

            data = (  {"d_from" : payload["from"] , "d_to" : payload["to"] , "real_topic" : real_topic  }  )

            statement = text(""" SELECT wis2topic,wis2notification FROM notifications 
                             WHERE date_published_broker>=:d_from AND date_published_broker<:d_to 
                             AND wis2topic like :real_topic""")

            logger.debug("executing %s",statement)

            result = con.execute(statement,data).fetchall()

            i=0
            for row in result:
                new_topic = base_topic + "/" + "/".join(row[0].split("/")[3:]) # remove cache/a/wis2/
                if len(new_topic.split("/"))>8:
                    logger.error("cannot replay %s, too long",new_topic)
                    continue

                payload = row[1].decode("utf-8")

                client_iot.publish(
                    topic = new_topic,
                    payload=payload
                )
                i=i+1

            client_id = base_topic.split("/")[0]

            logger.info("result size %s rows",i)
            send_statistics(i,client_id)






    return True
    
            
