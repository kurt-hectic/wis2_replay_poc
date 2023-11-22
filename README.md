# prototype of a AWS based WIS2 replay service
This AWS CdK based service subscribes to WIS2 Global Brokers and inserts notifications into a Aurora MySQL via a Kinesis stream.
When users (iot things) connect to IoT core and subscribe to a specially crafted topic, indicating which topic should be replayed and which time-interval, the service starts replaying (publishing) notifications. Internally, this works by a SQS queue subscribing to IoT core lifecycle events to react to subscription events and which then triggers a number of parallel Lambda functions which query the database and publish notifications to the topic the user subscribed to.
Disconnecting stops the streaming. 

# testing 

## testing
### notifications
```
cdk synth && sam build -t cdk.out\Wis2ReplayPocStack.template.json && sam local invoke replayLambda -t cdk.out\Wis2ReplayPocStack.template.json -e docker\replay\test\test_notification.json --env-vars test_env.json
```
