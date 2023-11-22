# prototype of a AWS based WIS2 replay service
This AWS CdK based service subscribes to WIS2 Global Brokers and inserts notifications into a Aurora MySQL via a Kinesis stream.
When users (iot things) connect to IoT core and subscribe to a specially crafted topic, indicating which topic should be replayed and which time-interval, the service starts replaying (publishing) notifications. Internally, this works by a SQS queue subscribing to IoT core lifecycle events to react to subscription events and which then triggers a number of parallel Lambda functions which query the database and publish notifications to the topic the user subscribed to.
Disconnecting stops the streaming. 

# usage

The user subscribes to a topic which indicates the time-range and topic(s) that should be replayed. This topic is specific to the user, so that each user can replay according to user needs.
The replay topic convention is "#CLIENT-ID#-#DATE_FROM#-#DATE-TO#/#TOPIC#, for example ```"replay-thing-1-replay-202311200000-202311200226/usa/synoptic_data_pbc/data/core/weather/surface-based-observations/synop"```

When using mosquitto_sub, an example invocation looks like this:

```mosquitto_sub -h awyxyfhut1ugd-ats.iot.eu-central-1.amazonaws.com -p 8883 -i replay-thing-1 --cert MYCERT.crt --key MYPRIVATE.key -t "replay-thing-1-replay-202311200000-202311200226/usa/synoptic_data_pbc/data/core/weather/surface-based-observations/synop" --cafile AmazonRootCA1.pem```

# testing 

## testing

### notifications
```
cdk synth && sam build -t cdk.out\Wis2ReplayPocStack.template.json && sam local invoke replayLambda -t cdk.out\Wis2ReplayPocStack.template.json -e docker\replay\test\test_notification.json --env-vars test_env.json
```
