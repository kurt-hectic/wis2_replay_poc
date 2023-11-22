FROM public.ecr.aws/lambda/python:3.8

RUN pip3 install boto3 sqlalchemy pymysql

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.lambda_handler" ] 