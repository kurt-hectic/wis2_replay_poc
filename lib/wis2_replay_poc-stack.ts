import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';

import {
  aws_s3 as s3, aws_iam as iam, aws_ec2 as ec2,
  aws_ecs as ecs, aws_lambda as lambda,
  aws_kinesis as kinesis, aws_s3_notifications as s3_notify, 
  aws_sqs as sqs, aws_dynamodb as ddb, aws_events as events, aws_events_targets as targets, RemovalPolicy
} from 'aws-cdk-lib';

import { SqsEventSource, KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { DatabaseClusterEngine, ServerlessCluster, Credentials, AuroraCapacityUnit, DatabaseSecret } from 'aws-cdk-lib/aws-rds'

import * as iot_alpha from '@aws-cdk/aws-iot-alpha';
import * as actions from '@aws-cdk/aws-iot-actions-alpha';

import * as destinations from '@aws-cdk/aws-kinesisfirehose-destinations-alpha';
import * as firehose from "@aws-cdk/aws-kinesisfirehose-alpha";


import * as fs from 'fs'
import { Scope } from 'aws-cdk-lib/aws-ecs';


// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class Wis2ReplayPocStack extends cdk.Stack {

  vpc: ec2.Vpc;
  bucket: s3.Bucket;
  dbCluster: ServerlessCluster;
  dbCredentials: DatabaseSecret;
  myStream: kinesis.Stream;
  metricPolicy: iam.Policy;

  secrets = JSON.parse(fs.readFileSync('env_secret.json', 'utf-8'));

  directory = "to-be-processed/notifications/"

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways : 1,
      subnetConfiguration: [{
        cidrMask: 24,
        name: 'public',
        subnetType: ec2.SubnetType.PUBLIC,
      }, {
        cidrMask: 24,
        name: 'compute',
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      }, {
        cidrMask: 28,
        name: 'rds',
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      }]
    })

    this.bucket = new s3.Bucket(this, "Kinesis2RDSBucket", {
      versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY, autoDeleteObjects: true,
      lifecycleRules: [
        { expiration: cdk.Duration.days(90) }
      ]
    });

    const policyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["cloudwatch:PutMetricData"],
      resources: ['*'],
    });

    this.metricPolicy =  new iam.Policy(this, "publish-metric-policy", { 
      statements: [ policyStatement ]
    })  

    this.myStream = this.setupStream();

    const task = this.setupBridge(id);
    this.myStream.grantWrite(task.taskRole);


    this.setupDBInfrastructure(id);
    this.setupDBImport(id);
    this.setupIoTprocessing();
    

  }

  setupStream(): kinesis.Stream {

    const myStream = new kinesis.Stream(this, "ReplayStream", { 
      retentionPeriod: cdk.Duration.hours(24), 
      streamMode: cdk.aws_kinesis.StreamMode.PROVISIONED,
      shardCount: 2
    })

    const lambdaFunction = new lambda.Function(this, 'Processor', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'app.lambda_handler',
      code: lambda.Code.fromAsset( 'lambda/process-records'),
    });

    const lambdaProcessor = new firehose.LambdaFunctionProcessor(lambdaFunction, {
      bufferInterval: cdk.Duration.minutes(1),
      bufferSize: cdk.Size.mebibytes(1),
      retries: 5,
    });

    const firehoseStream = new firehose.DeliveryStream(this, 'ReplayFirehoseStream', {
      sourceStream : myStream,
      destinations: [new destinations.S3Bucket(this.bucket, {
        processor: lambdaProcessor,
        dataOutputPrefix: this.directory ,
        //bufferingInterval: cdk.Duration.seconds(60),
        //bufferingSize: cdk.Size.mebibytes(1)
      })]
      ,
    });


    return myStream

  }

  setupBridge(id: string): ecs.FargateTaskDefinition {


    const cluster = new ecs.Cluster(this, "BridgeEcsCluster", { vpc: this.vpc });

    const taskDefinition = new ecs.FargateTaskDefinition(this, "BridgeTask", { family: "BridgeTask", memoryLimitMiB: 1024, cpu: 512  });
    
    
   //taskDefinition.taskRole.attachInlinePolicy(this.metricPolicy);


    const policyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["cloudwatch:PutMetricData"],
      resources: ['*'],
    });

    taskDefinition.addToTaskRolePolicy(policyStatement);
    taskDefinition.addToExecutionRolePolicy(policyStatement);

    
    const image = ecs.ContainerImage.fromAsset("./docker/wis2bridge");

    const reporting_threshold = "500" // send bridge statistics every x records
    const batch_size = "50" // group x records together before sending to kinesis stream

    const my_environment = {
      "WIS_USERNAME": this.secrets["WIS_MF_USERNAME"], "WIS_PASSWORD": this.secrets["WIS_MF_PASSWORD"], 
      "TOPICS": "cache/a/wis2/#",
      "CLIENT_ID": "wis2replay_mf_", 
      "WIS_BROKER_HOST": "globalbroker.meteo.fr", "WIS_BROKER_PORT": "8883",
       "LOG_LEVEL": "INFO",
       "STREAM_NAME" : this.myStream.streamName,
       "REPORTING_THRESHOLD" : reporting_threshold, "BATCH_SIZE" : batch_size,
       "STACK_ID" : id,
       "ABBREVIATION" : "MF"
    };

    const my_environment_cma = {
      "WIS_USERNAME": this.secrets["WIS_USERNAME"], "WIS_PASSWORD": this.secrets["WIS_PASSWORD"], 
      "TOPICS": "cache/a/wis2/#",
      "CLIENT_ID": "wis2replay_cma_", 
      "WIS_BROKER_HOST": "gb.wis.cma.cn", "WIS_BROKER_PORT": "1883",
       "LOG_LEVEL": "INFO",
       "STREAM_NAME" : this.myStream.streamName,
       "REPORTING_THRESHOLD" : reporting_threshold, "BATCH_SIZE" : batch_size,
       "STACK_ID" : id,
       "ABBREVIATION" : "CMA"


    };

    
    const my_environment_noaa = {
      "WIS_USERNAME": this.secrets["WIS_USERNAME"], "WIS_PASSWORD": this.secrets["WIS_PASSWORD"], 
      "TOPICS": "cache/a/wis2/#",
      "CLIENT_ID": "wis2replay_noaa_", 
      "WIS_BROKER_HOST": "wis2globalbroker.nws.noaa.gov", "WIS_BROKER_PORT": "1883",
       "LOG_LEVEL": "INFO",
       "STREAM_NAME" : this.myStream.streamName,
       "REPORTING_THRESHOLD" : reporting_threshold, "BATCH_SIZE" : batch_size,
       "STACK_ID" : id,
       "ABBREVIATION" : "NOAA"

    };

    const container = taskDefinition.addContainer("BridgeApp_MF", {
      image: image,
      environment: my_environment,
      logging: new ecs.AwsLogDriver({ streamPrefix: "BrideLog_MF", mode: ecs.AwsLogDriverMode.NON_BLOCKING })
    });

    const containerCMA = taskDefinition.addContainer("BridgeApp_CMA", {
      image: image,
      environment: my_environment_cma,
      logging: new ecs.AwsLogDriver({ streamPrefix: "BrideLog_CMA", mode: ecs.AwsLogDriverMode.NON_BLOCKING })
    });

    const containerNOAA = taskDefinition.addContainer("BridgeApp_NOAA", {
      image: image,
      environment: my_environment_noaa,
      logging: new ecs.AwsLogDriver({ streamPrefix: "BrideLog_NOAA", mode: ecs.AwsLogDriverMode.NON_BLOCKING })
    });

    
    const service = new ecs.FargateService(this, "BridgeService", {
      cluster: cluster,
      taskDefinition: taskDefinition, desiredCount: 1
    });

    return taskDefinition;
  }

  setupIoTprocessing() : void {

    const jobidtable = new ddb.Table(this, 'jobIdTable', {
      partitionKey: { name: 'replaytopic', type: ddb.AttributeType.STRING },
      //sortKey: { name: 'date', type: ddb.AttributeType.NUMBER },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
      //timeToLiveAttribute : "expirationdate"
    });

    const queue = new sqs.Queue(this, "myQueue", {visibilityTimeout:cdk.Duration.minutes(5)} )    
    
    const topicRule = new iot_alpha.TopicRule(this, 'TopicRule', {
      sql: iot_alpha.IotSql.fromStringAsVer20160323(
        "SELECT * FROM '$aws/events/subscriptions/subscribed/#'",
      ),
      actions: [
        new actions.SqsQueueAction(queue, {useBase64:false})
      ],
    });

    const topicRuleDisconnect = new iot_alpha.TopicRule(this,'TopicRuleDisconnect',{
      sql: iot_alpha.IotSql.fromStringAsVer20160323(
        "SELECT * FROM '$aws/events/presence/disconnected/#'",
      ),
      actions: [
        new actions.SqsQueueAction(queue, {useBase64:false})
      ]
    })

    const replayQueue = new sqs.Queue(this, "replayQueue", {visibilityTimeout:cdk.Duration.minutes(30)} )    


    const lfunction = new lambda.Function(this, "myFunction", {
      code: lambda.Code.fromAsset("./lambda/event-processor/"),  
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'app.handler',
      timeout: cdk.Duration.minutes(1),
      environment: {
        "QUEUE_URL": replayQueue.queueUrl,
        "STEP_MINUTES" : "5",
        "LAMBDA_LOG_LEVEL" : "INFO",
        "DDB_NAME" : jobidtable.tableName,
      },
    } ) 

    replayQueue.grantSendMessages(lfunction);
    replayQueue.grantConsumeMessages(lfunction);
    replayQueue.grantPurge(lfunction);

    jobidtable.grantReadWriteData(lfunction);
    
    queue.grantConsumeMessages(lfunction);

    lfunction.addEventSource(new SqsEventSource(queue, {batchSize:1}));

    lfunction.role?.addManagedPolicy( iam.ManagedPolicy.fromAwsManagedPolicyName("AWSIoTDataAccess")  );

    // processing

    const replaylambdaFunction = new lambda.DockerImageFunction(this, "replayLambda", {
      code: lambda.DockerImageCode.fromImageAsset("docker/replay"),
      environment: { 
        "LAMBDA_LOG_LEVEL": "INFO", 
        "SECRET_NAME": this.dbCredentials.secretName,
        "DB_OVERRIDE" : "false",
        "DDB_NAME" : jobidtable.tableName,     
      },
      
      timeout: cdk.Duration.minutes(10),
      vpc: this.vpc,
      memorySize: 128,
    });

    this.dbCredentials.grantRead(replaylambdaFunction)
    this.dbCluster.connections.allowFrom(replaylambdaFunction, ec2.Port.tcp(3306))
 
    replaylambdaFunction.role?.addManagedPolicy( iam.ManagedPolicy.fromAwsManagedPolicyName("AWSIoTDataAccess")  );
    replaylambdaFunction.addEventSource(new SqsEventSource(replayQueue, {batchSize:1}));

    replaylambdaFunction.role?.attachInlinePolicy(this.metricPolicy);

    jobidtable.grantReadData(replaylambdaFunction);
    
  }

  setupDBInfrastructure(id: string): void {

    const instanceIdentifier = 'mysql-01'
    const credsSecretName = `/${id}/rds/creds/${instanceIdentifier}`.toLowerCase()
    this.dbCredentials = new DatabaseSecret(this, 'MysqlRdsCredentials', {
      secretName: credsSecretName,
      username: 'admin'
    });

    const auroraSg = new ec2.SecurityGroup(this, "SecurityGroup", {
      vpc: this.vpc,
      description: "Allow ssh access to aurora cluster",
      allowAllOutbound: true
    })

    auroraSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(3306)
    )


    this.dbCluster = new ServerlessCluster(this, "AuroraCluster", {
      engine: DatabaseClusterEngine.AURORA_MYSQL,
      vpc: this.vpc,
      enableDataApi: true,
      credentials: Credentials.fromSecret(this.dbCredentials),
      defaultDatabaseName: "main",
      securityGroups: [auroraSg],
      scaling: {
        autoPause: cdk.Duration.minutes(10), // default is to pause after 5 minutes of idle time
        minCapacity: AuroraCapacityUnit.ACU_1, // default is 2 Aurora capacity units (ACUs)
        maxCapacity: AuroraCapacityUnit.ACU_32, // default is 16 Aurora capacity units (ACUs)
      }
    })

    // acces the DB via mini EC2 instance
    const bastionSG = new ec2.SecurityGroup(this, 'BastionSg', {
      vpc: this.vpc,
      allowAllOutbound: true,
    });

    bastionSG.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(22),
      'allow SSH access from anywhere',
    );

    const bastionEc2Instance = new ec2.Instance(this, 'BastionEc2Instance', {
      vpc: this.vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      securityGroup: bastionSG,
      instanceType: ec2.InstanceType.of(
        ec2.InstanceClass.BURSTABLE2,
        ec2.InstanceSize.MICRO,
      ),
      machineImage: new ec2.AmazonLinuxImage({
        generation: ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
      }),
      keyName: 'wis2monitoring-key',
    });

    new cdk.CfnOutput(this, "Bastion IP address", {
      value: bastionEc2Instance.instancePublicIp
    });

    this.dbCluster.connections.allowFrom(bastionEc2Instance, ec2.Port.tcp(3306))

    // cleanup DB

    const cleanupLambda = new lambda.Function(this, 'cleanupLambda', {
      code: lambda.Code.fromAsset('lambda/cleanup'),
      handler: 'cleanup.handler',
      runtime: lambda.Runtime.PYTHON_3_9,
      timeout: cdk.Duration.minutes(10),
      environment: {
        "BUCKET": this.bucket.bucketArn,
        "CLUSTER_ARN": this.dbCluster.clusterArn,
        "SECRET_ARN": this.dbCredentials.secretArn,
        "DB_NAME": "main",
        "NR_DAYS_KEEP": "2",
        "LAMBDA_LOG_LEVEL": "DEBUG",
        "TABLES" : "notifications"
      }

    });

    // allow cleanup function access to credentials and database API
    this.dbCluster.grantDataApiAccess(cleanupLambda);


    const event = new events.Rule(this, 'cleanupLambdaRule', {
      description: "cleanup with DB periodically",
      targets: [new targets.LambdaFunction(cleanupLambda)],
      schedule: events.Schedule.rate(cdk.Duration.minutes(10)),
    }
    );

  }

  setupDBImport(id: string): void {

    // setup Queues

    const notificationsSource = this.setupImport(id+"_notifications", this.directory)
    
    const fn = new lambda.Function(this, "S3importFunction", {
      code: lambda.Code.fromAsset("lambda/s3tords"),
      handler: 'app.lambda_handler',
      runtime: lambda.Runtime.PYTHON_3_9,
      environment: {
        "BUCKET": this.bucket.bucketArn,
        "CLUSTER_ARN": this.dbCluster.clusterArn,
        "SECRET_ARN": this.dbCredentials.secretArn,
        "DB_NAME": "main",
        "LAMBDA_LOG_LEVEL": "ERROR",
        "PROCESSED_PREFIX": "processed",
        "DB_BATCH_SIZE": "1000",
        "REPLAYNOTIFICATIONS_SOURCE_ARN": notificationsSource.queue.queueArn,
      },
      timeout: cdk.Duration.minutes(10),
      vpc: this.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      architecture: lambda.Architecture.X86_64
    });

    fn.role?.attachInlinePolicy(this.metricPolicy);

    this.bucket.grantReadWrite(fn);
    this.dbCluster.grantDataApiAccess(fn);
    //this.dbCredentials.grantRead(fn); // included in call above

  fn.addEventSource(notificationsSource);
    
    // surface-obs

  }

  setupImport(name: string, prefix: string): SqsEventSource {

    const deadLetterQueue = new sqs.Queue(this, "DLDqueue_" + name, {
      queueName: "dlq_" + name,
      deliveryDelay: cdk.Duration.millis(0),
      retentionPeriod: cdk.Duration.days(14),
    });

    const queue = new sqs.Queue(this, "S3ImportQueue_" + name, {
      queueName: "S3ImportQueue_" + name,
      visibilityTimeout: cdk.Duration.minutes(6 * 3),
      deadLetterQueue: {
        maxReceiveCount: 2,
        queue: deadLetterQueue
      }
    });

    this.bucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3_notify.SqsDestination(queue), {
      prefix: prefix
    });

    const eventSource = new SqsEventSource(queue, {
      reportBatchItemFailures: true,
      batchSize: 5, // maximum number of files processed by one lambda invocation
      maxBatchingWindow: cdk.Duration.seconds(10),
      maxConcurrency: 3 // maximum number of parallel lambda invocations
    });


    return eventSource

  }
}
