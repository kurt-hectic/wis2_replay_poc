## wis2 to aws iot client

# build
```
docker build --tag wis2awsbridge . 
```

# run
```
docker run --rm -v %cd%/wis2toaws.py:/app/wis2toaws.py wis2awsbridge
```
