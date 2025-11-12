// import { getDefaultApiGatewayConfiguration } from '@orcabus/platform-cdk-constructs/api-gateway';
import { WruValidatorStackProps } from './deployment-stack';
import { StageName } from '@orcabus/platform-cdk-constructs/shared-config/accounts';
import { EVENT_BUS_NAME } from '@orcabus/platform-cdk-constructs/shared-config/event-bridge';
import {
  SHARED_SECURITY_GROUP_NAME,
  VPC_LOOKUP_PROPS,
} from '@orcabus/platform-cdk-constructs/shared-config/networking';

export const getStackProps = (stage: StageName): WruValidatorStackProps => {
  return {
    vpcProps: VPC_LOOKUP_PROPS,
    lambdaSecurityGroupName: SHARED_SECURITY_GROUP_NAME,
    mainBusName: EVENT_BUS_NAME,
    // apiGatewayCognitoProps: {
    //   ...getDefaultApiGatewayConfiguration(stage),
    //   apiName: 'WorkflowManager',
    //   customDomainNamePrefix: 'workflow',
    // },
    stage: stage, // just pass on in case it could be useful
  };
};
