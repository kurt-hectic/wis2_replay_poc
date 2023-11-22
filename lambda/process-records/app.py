
import logging
import base64
import json

from os import getenv


log_level = getenv("LAMBDA_LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)

logger = logging.getLogger(__name__)
logger.setLevel(level)



def lambda_handler(event, context):

    records = []

    for record in event["records"]:
        
        record_id = record["recordId"]
        data = json.loads( base64.b64decode(record["data"]) )

        payload = {
            "data_id" : data["properties"]["data_id"],
            "received_datetime" : data["_meta"]["time_received"],
            "broker" :  data["_meta"]["broker"] ,
            "topic" : data["_meta"]["topic"],
        }
        del data["_meta"]
        payload["notification"] = json.dumps(data)

        ret = {
            "recordId": record_id,
            "result": "Ok",
            "data": base64.b64encode( (json.dumps(payload)+"\n").encode('utf-8') )
        }

        records.append(ret)


    return {'records': records}

