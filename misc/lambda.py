# turn on
import boto3
region = 'ap-southeast-1'
instances = ['i-02989097f3ff95357']
ec2 = boto3.client('ec2', region_name=region)

def lambda_handler(event, context):
    ec2.start_instances(InstanceIds=instances)
    print('started your instances: ' + str(instances))

# turn off
import boto3
region = 'ap-southeast-1'
instances = ['i-028dd6171c14a6582']
ec2 = boto3.client('ec2', region_name=region)

def lambda_handler(event, context):
    ec2.stop_instances(InstanceIds=instances)
    print('stopped your instances: ' + str(instances))