import json
import logging
import os
import boto3
import datetime

logger = logging.getLogger()
log_level = os.getenv("LAMBDA_LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)
logger.setLevel(level)

def handler(event, context):
    logging.info('request: {}'.format(json.dumps(event)))
    
    db_cluster_arn = os.environ.get('CLUSTER_ARN')
    db_credentials_secrets_store_arn = os.environ.get('SECRET_ARN')
    database_name = os.environ.get('DB_NAME','main')
    keep_days = int(os.environ.get("NR_DAYS_KEEP","90"))
    tables = os.environ.get("TABLES","").split(',')

    ago = (datetime.date.today() - datetime.timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S")

    response = None
    try:
        client = boto3.client('rds-data') 

        for table in tables:
            sql = f"delete from {table} where date_inserted_db<'{ago}'"

            logger.debug("execuing: "+sql)

            response = client.execute_statement(
                db_cluster_arn=db_cluster_arn,
                db_credentials_secrets_store_arn = db_credentials_secrets_store_arn,
                sql=sql,
                database_name=database_name
            )

            logger.info("response: {}".format(response))

        
    finally:
        if client:
            client.close()
        return response
    
    