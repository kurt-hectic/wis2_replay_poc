# prototype of a AWS based WIS2 replay service 

# testing 

## testing
### notifications
```
cdk synth && sam build -t cdk.out\Wis2ReplayPocStack.template.json && sam local invoke replayLambda -t cdk.out\Wis2ReplayPocStack.template.json -e docker\replay\test\test_notification.json --env-vars test_env.json
```
