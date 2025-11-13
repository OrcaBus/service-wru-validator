import * as path from 'path';
import { Construct } from 'constructs';
import { Architecture } from 'aws-cdk-lib/aws-lambda';
import { ISecurityGroup, IVpc, SecurityGroup, Vpc, VpcLookupOptions } from 'aws-cdk-lib/aws-ec2';
import { EventBus, IEventBus } from 'aws-cdk-lib/aws-events';
import { aws_lambda, Duration, Stack, StackProps } from 'aws-cdk-lib';
import { PythonFunction, PythonLayerVersion } from '@aws-cdk/aws-lambda-python-alpha';
import { ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
// import {
// OrcaBusApiGateway,
// OrcaBusApiGatewayProps,
// } from '@orcabus/platform-cdk-constructs/api-gateway';

export interface WruValidatorStackProps extends StackProps {
  lambdaSecurityGroupName: string;
  vpcProps: VpcLookupOptions;
  mainBusName: string;
  // apiGatewayCognitoProps: OrcaBusApiGatewayProps;
  stage: string;
}

export class WruValidatorStack extends Stack {
  private props: WruValidatorStackProps;
  private baseLayer: PythonLayerVersion;
  private readonly lambdaEnv: { [key: string]: string };
  private readonly lambdaRuntimePythonVersion: aws_lambda.Runtime = aws_lambda.Runtime.PYTHON_3_12;
  private readonly lambdaRole: Role;
  private readonly validatorLambdaRole: Role;
  private readonly lambdaSG: ISecurityGroup;
  private readonly mainBus: IEventBus;
  private readonly vpc: IVpc;

  constructor(scope: Construct, id: string, props: WruValidatorStackProps) {
    super(scope, id, props);

    this.props = props;

    this.mainBus = EventBus.fromEventBusName(this, 'OrcaBusMain', props.mainBusName);
    this.vpc = Vpc.fromLookup(this, 'MainVpc', props.vpcProps);
    this.lambdaSG = SecurityGroup.fromLookupByName(
      this,
      'LambdaSecurityGroup',
      props.lambdaSecurityGroupName,
      this.vpc
    );

    // Initialize Lambda environment variables
    this.lambdaEnv = {
      POWERTOOLS_SERVICE_NAME: 'wru-validator',
      POWERTOOLS_LOG_LEVEL: 'INFO',
      EVENT_BUS_NAME: props.mainBusName,
      SCHEMA_NAME: 'orcabus.workflowmanager@WorkflowRunUpdate',
      SCHEMA_REGISTRY_NAME: 'orcabus.workflowmanager',
    };

    // Create base layer for Python dependencies
    this.baseLayer = new PythonLayerVersion(this, 'BaseLayer', {
      entry: path.join(__dirname, '../../app/'),
      compatibleRuntimes: [this.lambdaRuntimePythonVersion],
      compatibleArchitectures: [Architecture.ARM_64],
      description: 'Base layer with common dependencies',
    });

    this.lambdaRole = new Role(this, 'LambdaRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      description: 'Lambda execution role for ' + id,
    });
    // FIXME it is best practise to such that we do not use AWS managed policy
    //  we should improve this at some point down the track.
    //  See https://github.com/umccr/orcabus/issues/174
    this.lambdaRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );
    this.lambdaRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole')
    );
    this.lambdaRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMReadOnlyAccess')
    );

    // Add EventBridge Schema Registry permissions
    this.lambdaRole.addToPolicy(
      new PolicyStatement({
        actions: ['schemas:DescribeSchema', 'schemas:GetDiscoveredSchema', 'schemas:ListSchemas'],
        resources: [
          `arn:aws:schemas:${this.region}:${this.account}:registry/discovered-schemas`,
          `arn:aws:schemas:${this.region}:${this.account}:schema/discovered-schemas/*`,
        ],
      })
    );

    // Add EventBridge put events permission
    this.lambdaRole.addToPolicy(
      new PolicyStatement({
        actions: ['events:PutEvents'],
        resources: [this.mainBus.eventBusArn],
      })
    );

    // Create the Lambda functions
    this.createEventBridgeValidator();
  }

  private createPythonFunction(name: string, functionProps: object): PythonFunction {
    return new PythonFunction(this, name, {
      entry: path.join(__dirname, '../../app/'),
      runtime: this.lambdaRuntimePythonVersion,
      layers: [this.baseLayer],
      environment: this.lambdaEnv,
      securityGroups: [this.lambdaSG],
      vpc: this.vpc,
      vpcSubnets: { subnets: this.vpc.privateSubnets },
      role: this.lambdaRole,
      architecture: Architecture.ARM_64,
      memorySize: 1024,
      ...functionProps,
    });
  }

  private createEventBridgeValidator() {
    const procFn: PythonFunction = this.createPythonFunction('WruDraftValidator', {
      index: 'wru_validator/lambda/wru_draft_validator.py',
      handler: 'lambda_handler',
      timeout: Duration.seconds(28),
    });

    // Grant EventBridge permissions
    this.mainBus.grantPutEventsTo(procFn);
  }
}
