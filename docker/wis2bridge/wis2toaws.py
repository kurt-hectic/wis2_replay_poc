import logging
import time
import sys
import threading
import time
import json
import traceback
import ssl
import os
import signal
import boto3
import random
import string
import queue

import paho.mqtt.client as mqtt_paho

from datetime import datetime


from uuid import uuid4

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)
stack_id = os.getenv("STACK_ID","")
abbreviation = os.getenv("ABBREVIATION")

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[ logging.FileHandler("debug.log"), logging.StreamHandler()] )

def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

topics = [t.strip() for t in os.getenv("TOPICS","cache/a/wis2/#").split(",")]
wis_broker_host = os.getenv("WIS_BROKER_HOST")
wis_broker_port = int(os.getenv("WIS_BROKER_PORT"))
wis_user = os.getenv("WIS_USERNAME")
wis_pw = os.getenv("WIS_PASSWORD")
client_id = os.getenv("CLIENT_ID") + get_random_string(6)
aws_broker = os.getenv("AWS_BROKER")

# update stats every x notifications
threshold = int(os.getenv("REPORTING_THRESHOLD","100"))
# group batch_size records together before sending to Kinesis stream
batch_size = int(os.getenv("BATCH_SIZE","10"))

session = boto3.Session()
cloud_watch = session.client('cloudwatch')

#logging.debug(f"from file {cert_text}")

stream_name = os.getenv("STREAM_NAME")

q = queue.Queue()


def on_connect_wis(client, userdata, flags, rc):
    """
    set the bad connection flag for rc >0, Sets onnected_flag if connected ok
    also subscribes to topics
    """
    logging.info("Connected flags: %s result code: %s ",flags,rc) 
    for topic in topics:   
        logging.info("subscribing to:"+str(topic))
        client_wis2.subscribe(topic,qos=1)

def on_subscribe(client,userdata,mid,granted_qos):
    """removes mid values from subscribe list"""
    logging.info("in on subscribe callback result %s",mid)
    client.subscribe_flag=True

def on_message(client, userdata, msg):
    topic=msg.topic
    logging.debug("message received with topic %s", topic)
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    #logging.debug("message received")
    message_routing(client,topic,m_decode)
    
def on_disconnect(client, userdata, rc):
    logging.info("connection to broker has been lost")
    client.connected_flag=False
    client.disconnect_flag=True
            
def message_routing(client,topic,msg):
    #logging.debug("message_routing")
    logging.debug("routing topic: %s",topic)
    logging.debug("routing message: %s ",msg)
    
    # insert metadata into the message
    if not msg or msg.isspace():
        logging.debug("discarding empty message published on %s", topic)
        return
    try:
        msg = json.loads(msg)
    except json.JSONDecodeError as e:
        logging.error("cannot parse json message %s , %s , %s",msg,e,topic)
        return    

    msg["_meta"] = { "time_received" : datetime.now().isoformat() , "broker" : abbreviation , "topic" : topic }
    
    logging.debug("queuing topic %s with length %s and %s",topic,len(msg),msg)
    q.put( (topic,msg) )

        
def create_wis2_connection():
    transport = "websockets" if wis_broker_port==443 else "tcp"
    logging.info(f"creating wis2 connection to {wis_broker_host}:{wis_broker_port} using {wis_user}/{wis_pw} over {transport} with client id {client_id}")
    
    client = mqtt_paho.Client(client_id=client_id, transport=transport,
         protocol=mqtt_paho.MQTTv311, clean_session=False)
                         
    client.username_pw_set(wis_user, wis_pw)
    if wis_broker_port != 1883:
        logging.info("setting up TLS connection")
        client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED)
    
    client.on_message = on_message
    client.on_connect = on_connect_wis
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    
    client.connect(wis_broker_host,
                   port=wis_broker_port,
                   #clean_start=mqtt_paho.,
                   #properties=properties,
                   keepalive=60)
    
    return client
    




class ConsumerThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(ConsumerThread,self).__init__()
        self.target = target
        self.name = name
        
        self.shutdown_flag = threading.Event()
        self.kinesis = boto3.client('kinesis')

        logging.info("created AWS connection")

        return
    

    def run(self):
        counter = 0
        #threshold = 100 
        before = datetime.now()
        #batch_size = 10

        while not self.shutdown_flag.is_set():
            if not q.empty():

                # batching items together
                records = []
                while not q.empty() and len(records)<batch_size :
                    records.append(  q.get() )

                try:
                    response = self.kinesis.put_records(
                        StreamName=stream_name,
                        Records = [ { "Data" :  json.dumps(msg) , "PartitionKey" : msg["properties"]["data_id"] } for (topic,msg) in records  ]
                    )
                    logging.debug("published %s records to AWS %s",len(records),response)

                    if response["FailedRecordCount"] > 0:
                        for i,(topic,msg) in enumerate(records):
                            if "ErrorCode" in response["Records"][i]:
                                q.put( (topic,msg) ) # put failed notifications back into the queue


                except Exception as e:
                    logging.error(f"could not publish records to AWS",exc_info=True)
                    for (topic,msg) in records:
                        q.put( (topic,msg) ) # put failed notifications back into the queue
                    time.sleep(1)
                    continue

                counter = counter + len(records) - response["FailedRecordCount"]

                # processing of statistics
                if counter > threshold:
                    now = datetime.now()
                    d = int(threshold / (now - before).total_seconds()  )
                    logging.debug("receievd %s messages.. sending stats. Throughput %s per sec",threshold,d)

                    try:
                        response = cloud_watch.put_metric_data(
                                    MetricData = [
                                        {
                                            'MetricName': 'notificationsReceivedFromBroker',
                                            'Dimensions': [
                                                {
                                                    'Name': 'Broker',
                                                    'Value': stack_id+"_"+wis_broker_host
                                                },
                                            ],
                                            'Unit': 'Count',
                                            'Value':  counter
                                        },
                                    ],
                                    Namespace = 'WIS2monitoring'
                                )
                    except Exception as a:
                        logging.error("could not publish stats %s ",e)

                    counter = 0
                    before = now
                
            else: 
                time.sleep(0.01)
        logging.info('Consumer Thread #%s stopped', self.ident)
        return



def service_shutdown(signum, frame):
    signame = signal.Signals(signum).name
    logging.info(f"received signal {signame}")
    
    client_wis2.loop_stop()
    client_wis2.disconnect()
    
    c.shutdown_flag.set()
    c.join()
        
   
if __name__ == '__main__':

    logging.info("starting up bridge service")

    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    
    try:
    
        c = ConsumerThread(name='consumer')
        c.start()
        time.sleep(2)
          
        logging.info("creating connection to WIS2")
        client_wis2 = create_wis2_connection()
        logging.info("connected to WIS2")

        client_wis2.loop_forever() #start loop
        logging.info("exiting main thread")
        
    except Exception as e:
        print("error",e)
