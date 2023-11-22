import csv
import os
import time
import tempfile
import boto3
import botocore
import json
import logging
import sys
import random
import string

from os import getenv
from csv import reader
from datetime import datetime
from botocore.exceptions import ClientError


logger = logging.getLogger("s3tords")
#streamHandler = logging.StreamHandler(stream=sys.stdout)
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#streamHandler.setFormatter(formatter)
#logger.addHandler(streamHandler)



class DBProcessor:

    table = None
    transaction_id = None
    sql = ""
    mapping = {}

    def __init__(self,tablename,mapping):
        self.table=tablename
        self.mapping=mapping
        self.sql = self.create_sql()

        
    def __enter__(self):
        self.batch_size = int(os.environ.get("DB_BATCH_SIZE","1000"))
        self.db_cluster_arn = os.environ.get('CLUSTER_ARN')
        self.db_credentials_secrets_store_arn = os.environ.get('SECRET_ARN')
        self.database_name = os.environ.get('DB_NAME','main')
        self.processed_prefix = os.environ.get("PROCESSED_PREFIX","processed")

        self.rds_client = boto3.client('rds-data')
        self.s3_client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        self.cloudwatch = boto3.client('cloudwatch')

        return self
       

    def __exit__(self, exc_type, exc_value, traceback):

        try:
            self.rds_client.close()
        except:
            pass

        try:
            self.s3_client.close()
        except:
            pass

        try:
            self.s3.close()
        except:
            pass

    # xxxxx
    def format_entry(j):
        raise NotImplemented("implement in inheriting class")
    
    def create_sql(self):
        
        sql = """INSERT INTO {table} ({keys}) values ({values})""".format( 
            keys = ",".join(  [ item["db_col"] for k,item in self.mapping.items() ] ),
            values = ",".join( [ f":{k}" for k,item in self.mapping.items() ] ),
            table = self.table
        )
        
        logger.debug(f"created SQL: {sql}")

        return sql
    
    def format_entry(self,j):

        logger.debug("formating: {}".format(j))

        #        "property_datetime": {
        #     "type": "stringValue",
        #     "db_col": "property_datetime",
        #     "source_key": "property_datetime",
        #     "typeHint": "TIMESTAMP"
        # },


        entry = []
        
        for k,item in self.mapping.items():

            is_empty = (item["source_key"] not in j) or not j[item["source_key"]]

            value = {}
            if is_empty:
                value["isNull"] = True
            else:

                if item.get("typeHint","N/A") == "TIMESTAMP":
                    actual_value = self.format_date( j[item["source_key"]] )
                elif item.get("type","stringValue") == "longValue":
                    actual_value = self.format_int( j[item["source_key"]]  )
                elif item.get("type","stringValue") == "doubleValue":
                    actual_value = self.format_float( j[item["source_key"]] )
                else:
                    actual_value = j[item["source_key"]]

                value[item["type"]] = actual_value

                
         
            elem = { "name" : k , "value" : value }

            if "typeHint" in item and not is_empty:
                elem["typeHint"] = item["typeHint"]


            entry.append(elem)

        logger.debug("returning entry: {}".format(entry))
                
        return entry
    
    def format_date(self,date_string):
            
        #logger.debug("parsing {}".format(date_string))

        # 2023-06-21T15:20:00-04:00
        
        if not date_string:
            return None
        
        date_string = date_string.rstrip("Z")
        if '.' in date_string:
            date_string = date_string.split(".")[0]

        try:
            if ":" in date_string and "-" in date_string:
                date = datetime.fromisoformat(date_string)
            elif "T" in date_string:
                date = datetime.strptime(date_string,"%Y%m%dT%H%M%S")
            else:
                return None

        except ValueError as e:
            logger.warning(f"cannot parse date {date_string}, returning null")
            return None


        return date.strftime("%Y-%m-%d %H:%M:%S")

    def format_int(self,s):
        
        if not s:
            return None
        
        return int(s)

    def format_float(self,s):
        
        if not s:
            return None
        
        return float(s)


    def process(self,records):
        self.wake_aurora()

        batch_item_failures = []
        for record in records: # SQS records

            inner_records = json.loads( record["body"] )

            if not "Records" in inner_records:
                continue

            # all inner records are processed as a single transaction because the SQS treats them as a 
            # single record
            self.begin_transaction()  
            try:
                for inner_record in inner_records["Records"]:

                    source_bucket = inner_record['s3']['bucket']['name']
                    key = inner_record['s3']['object']['key']

                    logger.info("processing {}".format(key))
                
                    try:
                        s3_response_object = self.s3_client.get_object(Bucket=source_bucket,Key=key)
                    except (ClientError) as e:
                        logger.info("key {} does not exist.. likely already processed, skipping")
                        logger.debug("detailed info",exc_info=True)
                        continue

                    file_content = s3_response_object['Body']

                    #file_content = file_content.replace("{}","\n")

                    sql_parameter_sets = []
                    for i,line in enumerate(file_content.readlines()):
                        if not line: # skip empty lines
                            continue
                        line=line.decode().rstrip(",\n")
                        entry = self.format_entry(json.loads(line))
                        sql_parameter_sets.append(entry)

                        if i%self.batch_size == 0 and i>0:
                            self.batch_execute_statement(self.sql, sql_parameter_sets)
                            
                            

                            sql_parameter_sets=[]

                    if len(sql_parameter_sets)>0:
                        self.batch_execute_statement(self.sql, sql_parameter_sets)
                        
                    
                self.commit_transaction()

                # remove files that were processed sucesfully
                for inner_record in inner_records["Records"]:
                    source_bucket = inner_record['s3']['bucket']['name']
                    key = inner_record['s3']['object']['key']

                    try:

                        new_key = key.replace("to-be-processed",self.processed_prefix)
                        logger.info("moving {} to {}".format(key,new_key))
                        self.s3_client.copy_object( CopySource= "{}/{}".format(source_bucket,key), Bucket=source_bucket, Key=new_key)
                        self.s3_client.delete_object(Bucket=source_bucket,Key=key)
                    except (ClientError) as e: 
                        logger.warning("could not move {}. Continuing the processing".format(key))
                        logger.debug("details: ",exc_info=True)
                
            
            except Exception as e:
                logger.error("error processing {}, rolling back".format(record['messageId']),exc_info=True)
                self.rollback_transaction()
                batch_item_failures.append({"itemIdentifier": record['messageId']})

        return batch_item_failures


    def batch_execute_statement(self,sql, sql_parameter_sets):
        response = self.rds_client.batch_execute_statement(
            secretArn=self.db_credentials_secrets_store_arn,
            database=self.database_name,
            resourceArn=self.db_cluster_arn,
            sql=sql,
            parameterSets=sql_parameter_sets,
            transactionId=self.transaction_id
        )

        nr_records_inserted = len(response["updateResults"])

        response = self.cloudwatch.put_metric_data(
            MetricData = [
                {
                    'MetricName': 'RecordsInserteDBdNumber',
                    'Dimensions': [
                        {
                            'Name': 'Table',
                            'Value': self.table
                        },
                    ],
                    'Unit': 'Count',
                    'Value':  nr_records_inserted
                },
            ],
            Namespace = 'WIS2monitoring'
        )

        logger.info(f'number of records updated: {nr_records_inserted}')


        return response

    def begin_transaction(self):

        response = self.rds_client.begin_transaction(
            secretArn=self.db_credentials_secrets_store_arn,
            database=self.database_name,
            resourceArn=self.db_cluster_arn
        )

        self.transaction_id = response["transactionId"]

    def commit_transaction(self):

        response = self.rds_client.commit_transaction(
            secretArn=self.db_credentials_secrets_store_arn,
            resourceArn=self.db_cluster_arn,
            transactionId=self.transaction_id
        )

    def rollback_transaction(self):

        response = self.rds_client.rollback_transaction(
            secretArn=self.db_credentials_secrets_store_arn,
            resourceArn=self.db_cluster_arn,
            transactionId=self.transaction_id
        )

        

    def wake_aurora(self):
        delay = 5
        max_attempts = 10

        attempt = 0
        while attempt < max_attempts:
            logger.debug(f"attempting to wake up aurora: {attempt}")
            attempt += 1
            try:
                wake = self.rds_client.execute_statement (
                    resourceArn = self.db_cluster_arn,
                    secretArn = self.db_credentials_secrets_store_arn,
                    database = self.database_name,
                    sql = 'select 1'
                )
                return
            except ClientError as ce:
                error_code = ce.response.get("Error").get('Code')
                error_msg = ce.response.get("Error").get('Message')
                    # Aurora serverless is waking up
                if error_code == 'BadRequestException' and 'Communications link failure' in error_msg:
                    time.sleep(delay)
                else:
                    raise ce

            raise Exception('Waited for Aurora Serveless but still got errors')
        
class CAPDBProcessor(DBProcessor):
    key_mapping = {
        "cap_identifier": {
            "type": "stringValue",
            "db_col": "cap_identifier",
            "source_key": "cap_identifier"
        },
        "cap_sender": {
            "type": "stringValue",
            "db_col": "cap_sender",
            "source_key": "cap_sender"
        },
        "cap_sent": {
            "type": "stringValue",
            "db_col": "cap_sent",
            "source_key": "cap_sent",
            "typeHint": "TIMESTAMP"
        },
        "cap_sent_orig": {
            "type": "stringValue",
            "db_col": "cap_sent_orig",
            "source_key": "cap_sent",
            "typeHint": "TIMESTAMP"
        },
        "cap_status": {
            "type": "stringValue",
            "db_col": "cap_status",
            "source_key": "cap_status"
        },
        "cap_msgType": {
            "type": "stringValue",
            "db_col": "cap_msgType",
            "source_key": "cap_msgType"
        },
        "cap_scope": {
            "type": "stringValue",
            "db_col": "cap_scope",
            "source_key": "cap_scope"
        },
        "cap_references": {
            "type": "stringValue",
            "db_col": "cap_references",
            "source_key": "cap_references"
        },
        "cap_info_language": {
            "type": "stringValue",
            "db_col": "cap_info_language",
            "source_key": "cap_info_language"
        },
        "cap_info_category": {
            "type": "stringValue",
            "db_col": "cap_info_category",
            "source_key": "cap_info_category"
        },
        "cap_info_event": {
            "type": "stringValue",
            "db_col": "cap_info_event",
            "source_key": "cap_info_event"
        },
        "cap_info_responseType": {
            "type": "stringValue",
            "db_col": "cap_info_responseType",
            "source_key": "cap_info_responseType"
        },
        "cap_info_urgency": {
            "type": "stringValue",
            "db_col": "cap_info_urgency",
            "source_key": "cap_info_urgency"
        },
        "cap_info_severity": {
            "type": "stringValue",
            "db_col": "cap_info_severity",
            "source_key": "cap_info_severity"
        },
        "cap_info_certainty": {
            "type": "stringValue",
            "db_col": "cap_info_certainty",
            "source_key": "cap_info_certainty"
        },
        "cap_info_effective": {
            "type": "stringValue",
            "db_col": "cap_info_effective",
            "source_key": "cap_info_effective"
        },
        "cap_info_onset": {
            "type": "stringValue",
            "db_col": "cap_info_onset",
            "source_key": "cap_info_onset"
        },
        "cap_info_expires": {
            "type": "stringValue",
            "db_col": "cap_info_expires",
            "source_key": "cap_info_expires",
            "typeHint": "TIMESTAMP"
        },
        "cap_info_expires_orig": {
            "type": "stringValue",
            "db_col": "cap_info_expires_orig",
            "source_key": "cap_info_expires"
        }, 
        "cap_info_senderName": {
            "type": "stringValue",
            "db_col": "cap_info_senderName",
            "source_key": "cap_info_senderName"
        },
        "cap_info_headline": {
            "type": "stringValue",
            "db_col": "cap_info_headline",
            "source_key": "cap_info_headline"
        },
        "cap_info_description": {
            "type": "stringValue",
            "db_col": "cap_info_description",
            "source_key": "cap_info_description"
        },
        "cap_info_web": {
            "type": "stringValue",
            "db_col": "cap_info_web",
            "source_key": "cap_info_web"
        },
        "meta_broker": {
            "type": "stringValue",
            "db_col": "meta_broker",
            "source_key": "meta_broker"
        },
        "meta_topic": {
            "type": "stringValue",
            "db_col": "meta_topic",
            "source_key": "meta_topic"
        }
    }

        
    def __init__(self):
        super().__init__(tablename='caps',mapping=self.key_mapping)




class SurfaceObsDBProcessor(DBProcessor):
    key_mapping = {
        "wsi": {
            "type": "stringValue",
            "db_col": "wsi",
            "source_key": "wsi"
        },
        "result_time": {
            "type": "stringValue",
            "db_col": "result_time",
            "source_key": "result_time",
            "typeHint": "TIMESTAMP"
        },
        "phenomenon_time": {
            "type": "stringValue",
            "db_col": "phenomenon_time",
            "source_key": "phenomenon_time"
        },
        "geom_lat": {
            "type": "doubleValue",
            "db_col": "geom_lat",
            "source_key": "geom_lat"
        },
        "geom_lon": {
            "type": "doubleValue",
            "db_col": "geom_lon",
            "source_key": "geom_lon"
        },
        "geom_height": {
            "type": "doubleValue",
            "db_col": "geom_height",
            "source_key": "geom_height"
        },
        "observed_property_pressure_reduced_to_mean_sea_level": {
            "type": "booleanValue",
            "db_col": "observed_property_pressure_reduced_to_mean_sea_level",
            "source_key": "observed_property_pressure_reduced_to_mean_sea_level"
        },
        "observed_property_air_temperature": {
            "type": "booleanValue",
            "db_col": "observed_property_air_temperature",
            "source_key": "observed_property_air_temperature"
        },
        "observed_property_dewpoint_temperature": {
            "type": "booleanValue",
            "db_col": "observed_property_dewpoint_temperature",
            "source_key": "observed_property_dewpoint_temperature"
        },
        "observed_property_relative_humidity": {
            "type": "booleanValue",
            "db_col": "observed_property_relative_humidity",
            "source_key": "observed_property_relative_humidity"
        },
        "observed_property_wind_direction": {
            "type": "booleanValue",
            "db_col": "observed_property_wind_direction",
            "source_key": "observed_property_wind_direction"
        },
        "observed_property_wind_speed": {
            "type": "booleanValue",
            "db_col": "observed_property_wind_speed",
            "source_key": "observed_property_wind_speed"
        },
        "observed_property_total_snow_depth": {
            "type": "booleanValue",
            "db_col": "observed_property_total_snow_depth",
            "source_key": "observed_property_total_snow_depth"
        },
        "all_observed_properties": {
            "type": "blobValue",
            "db_col": "all_observed_properties",
            "source_key": "all_observed_properties"
        },
        "meta_broker": {
            "type": "stringValue",
            "db_col": "meta_broker",
            "source_key": "meta_broker"
        },
        "meta_topic": {
            "type": "stringValue",
            "db_col": "meta_topic",
            "source_key": "meta_topic"
        },
        "meta_lambda_datetime": {
            "type": "stringValue",
            "db_col": "meta_lambda_datetime",
            "source_key": "meta_lambda_datetime",
            "typeHint": "TIMESTAMP"
        },
        "property_data_id": {
            "type": "stringValue",
            "db_col": "property_data_id",
            "source_key": "property_data_id"
        },
        "observed_property_non_coordinate_pressure": {
            "type": "booleanValue",
            "db_col": "observed_property_non_coordinate_pressure",
            "source_key": "observed_property_non_coordinate_pressure"
        },
        "meta_received_datetime": {
            "type": "stringValue",
            "db_col": "meta_received_datetime",
            "source_key": "meta_received_datetime",
            "typeHint": "TIMESTAMP"
        },
    }

    
    def __init__(self):
        super().__init__(tablename='surfaceobservations',mapping=self.key_mapping)




class NotificationsDBProcessor(DBProcessor):
    
    key_mapping = {
        "id": {
            "type": "stringValue",
            "db_col": "wis2_id",
            "source_key": "id"
        },
        "version": {
            "type": "stringValue",
            "db_col": "wis2_version",
            "source_key": "version"
        },
        "property_datetime": {
            "type": "stringValue",
            "db_col": "property_datetime",
            "source_key": "property_datetime",
            "typeHint": "TIMESTAMP"
        },
        "property_datetime_orig": {
            "type": "stringValue",
            "db_col": "property_datetime_orig",
            "source_key": "property_datetime"
        },
        "property_data_id": {
            "type": "stringValue",
            "db_col": "property_data_id",
            "source_key": "property_data_id"
        },
        "content_encoding": {
            "type": "stringValue",
            "db_col": "content_encoding",
            "source_key": "content_encoding"
        },
        "content_size": {
            "type": "longValue",
            "db_col": "content_size",
            "source_key": "content_size"
        },
        "integrity_method": {
            "type": "stringValue",
            "db_col": "integrity_method",
            "source_key": "integrity_method"
        },
        "pubtime": {
            "type": "stringValue",
            "db_col": "pubtime",
            "source_key": "pubtime",
            "typeHint": "TIMESTAMP"
        },
        "pubtime_orig": {
            "type": "stringValue",
            "db_col": "pubtime_orig",
            "source_key": "pubtime"
        },
        "meta_received_datetime": {
            "type": "stringValue",
            "db_col": "meta_received_datetime",
            "source_key": "meta_received_datetime",
            "typeHint": "TIMESTAMP"
        },
        "meta_broker": {
            "type": "stringValue",
            "db_col": "meta_broker",
            "source_key": "meta_broker"
        },
        "meta_topic": {
            "type": "stringValue",
            "db_col": "meta_topic",
            "source_key": "meta_topic"
        },
        "meta_lambda_datetime": {
            "type": "stringValue",
            "db_col": "meta_lambda_datetime",
            "source_key": "meta_lambda_datetime",
            "typeHint": "TIMESTAMP"
        },
        "canonical_href": {
            "type": "stringValue",
            "db_col": "canonical_href",
            "source_key": "canonical_href"
        },
        "canonical_type": {
            "type": "stringValue",
            "db_col": "canonical_type",
            "source_key": "canonical_type"
        },
        "meta_source": {
            "type": "stringValue",
            "db_col": "meta_source",
            "source_key": "meta_source"
        },
        "meta_cache_status": {
            "type": "longValue",
            "db_col": "meta_cache_status",
            "source_key": "meta_cache_status"
        },
        "meta_validates": {
            "type": "booleanValue",
            "db_col": "meta_validates",
            "source_key": "meta_validates"
        },
        "meta_content_embedded": {
            "type": "booleanValue",
            "db_col": "meta_content_embedded",
            "source_key": "meta_content_embedded"
        },    
        "meta_remote_content_size": {
            "type": "longValue",
            "db_col": "meta_remote_content_size",
            "source_key": "meta_remote_content_size"
        },   
    }
        

    def __init__(self):
        super().__init__(tablename='notifications',mapping=self.key_mapping)

class NotficationsreplayProcessor(DBProcessor):
    
    key_mapping = {
        
        "data_id": {
            "type": "stringValue",
            "db_col": "wis2dataid",
            "source_key": "data_id"
        },
        "received_datetime": {
            "type": "stringValue",
            "db_col": "date_published_broker",
            "source_key": "received_datetime",
            "typeHint": "TIMESTAMP"
        },
        "broker": {
            "type": "stringValue",
            "db_col": "wis2broker",
            "source_key": "broker"
        },
        "topic": {
            "type": "stringValue",
            "db_col": "wis2topic",
            "source_key": "topic"
        },
        "notification" : {
            "type" : "blobValue",
            "db_col" : "wis2notification",
            "source_key" : "notification"
        }

    }
        

    def __init__(self):
        super().__init__(tablename='notifications',mapping=self.key_mapping)

