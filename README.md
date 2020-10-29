# WBMQSystemProject

WBMQSystemProject is a message queue system based on a broker written in GoLang served by DynamoDB AWS service. Case study is to manage a warehouse with robots inside, receiving data from sensors located or not, in the same sector of a bot.

## Installation

Necessary libraries to run the system broker are:

```bash
	go get github.com/aws/aws-sdk-go/service/dynamodb
	go get github.com/gorilla/mux
	go get github.com/lithammer/shortuuid
```

Is also needed a fully working AWS account. IAM's user role need DynamoDBFullAccess policy.

## Usage

### Windows ###
#### *Running in local enviroment* ####
```bash
	# AWS ENV VARIABLES
	set AWS_REGION=us-east-2
	set AWS_SDK_LOAD_CONFIG=1
	set AWS_ACCESS_KEY_ID=your_aws_access_key_id
	set AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
	
	# Running in context aware mode
	go run http-api.go pubsub-system.go dynamo-db-repository.go ctx

	# Running without context aware mode
	go run http-api.go pubsub-system.go dynamo-db-repository.go
```
#### *Running in Docker* ####
```bash
	# Running without context aware mode needs to specify this last line in Dockefile
	CMD ["./wbmq"]	
	
	# Running in context aware mode requires to change last line in Dockerfile
	CMD ["./wbmq", "ctx"]

	# From main folder of the project
	docker build -t wbmqsystem .
	docker run -d -p 5000:5000 -e AWS_REGION=us-east-2 -e AWS_SDK_LOAD_CONFIG=1 -e AWS_ACCESS_KEY_ID=your_aws_access_key_id -e AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```

#### *Running in AWS Elastic Beanstalk environment* ####
```bash
	# Running without context aware mode needs to specify this last line in Dockefile
	CMD ["./wbmq"]	
	
	# Running in context aware mode requires to change last line in Dockerfile
	CMD ["./wbmq", "ctx"]

	# From main folder of the project (this step requires awsebcli installed)
	eb init
	eb create
```
A standard configuration used for Elastic Beanstalk Environment consist of:
* Classic Load Balancer
* Docker running on Amazon Linux 64 machine
* Default CNAME, App name and environment name
* No SpotFleet and SSH needed
* IAM's user role ElasticBeanStalkFullAccess and EC2FullAccess policies.

### Linux/MacOS ###
#### *Running in local enviroment* ####
```bash
	# AWS ENV VARIABLES
	export AWS_REGION=us-east-2
	export AWS_SDK_LOAD_CONFIG=1
	export AWS_ACCESS_KEY_ID=your_aws_access_key_id
	export AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```
#### *Running in Docker or Running in AWS Elastic Beanstalk environment* ####
```bash
	# Same step as Windows
```