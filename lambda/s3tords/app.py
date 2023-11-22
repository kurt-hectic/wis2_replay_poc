
import logging

from os import getenv

from s3tords import NotificationsDBProcessor, SurfaceObsDBProcessor, CAPDBProcessor, NotficationsreplayProcessor


log_level = getenv("LAMBDA_LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)

logger = logging.getLogger(__name__)
logger.setLevel(level)

logger_module = logging.getLogger("s3tords")
logger_module.setLevel(level)

notification_queue_arn = getenv("NOTIFCIATION_SOURCE_ARN")
surfaceobs_queue_arn = getenv("SURFACEOBS_SOURCE_ARN")
cap_queue_arn = getenv("CAP_SOURCE_ARN")
notificationreplay_queue_arn = getenv("REPLAYNOTIFICATIONS_SOURCE_ARN")


def lambda_handler(event, context):

    logger.info("event handler {}".format(event))

    if not event or not "Records" in event or len(event["Records"])==0:
        return 

    source_queue = event["Records"][0]["eventSourceARN"]

    if source_queue == notification_queue_arn:
        processor_type = NotificationsDBProcessor
    elif source_queue == surfaceobs_queue_arn:
        processor_type = SurfaceObsDBProcessor
    elif source_queue == cap_queue_arn:
        processor_type = CAPDBProcessor
    elif source_queue == notificationreplay_queue_arn:
        processor_type = NotficationsreplayProcessor 
    else:
        raise Exception("unknown source: "+source_queue+" I know the following: {}".format([notification_queue_arn,surfaceobs_queue_arn]))
    
    with processor_type() as processor:

        batch_item_failures = processor.process(event["Records"])
        sqs_batch_response = { "batchItemFailures": batch_item_failures }    
        
        logger.debug("returning {}".format(sqs_batch_response))
        return sqs_batch_response


