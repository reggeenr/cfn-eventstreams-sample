# Sample integration IBM Cloud Functions and IBM Cloud Event Streams

## Setup

### Prerequisites

* Create an Event Streams instance in the IBM Cloud and configure service credentials and a topic called `my-topic`

* Copy the file `params-empty.json` to `params.json` and replace its content with the service credentials JSON

* Use the script `./buildAndPush.sh <your-dockerhub-namespace>/cfn-kafka-runtime-node` to build and push an adjusted runtime image to DockerHub

### Cloud Functions Setup

* Choose an IAM enabled namespace in the WebUI <https://cloud.ibm.com/functions>

* Setup the Event streams integration using the Quickstart template <https://cloud.ibm.com/functions/create/template/message-hub-events>. Specify the package name `cfn-eventstreams-sample`

* Login the IBM Cloud CLI and target the new same IAM enabled namespace

* Update the Quickstart template action code by issuing the following command

```
ibmcloud fn action update cfn-eventstreams-sample/process-message process-message.js --kind nodejs:12 
```

* Create a second Action that is used to ingest messages

```
ibmcloud fn action update cfn-eventstreams-sample/producer producer.js --docker <your-dockerhub-namespace>/cfn-kafka-runtime-node --web true --param-file ./params.json
```

* Use the command `ic fn action get cfn-eventstreams-sample/producer --url ` to retrieve the URL of the WebAction

* Create a third Action that is capable of re-consuming processed messages

```
ibmcloud fn action update cfn-eventstreams-sample/retry-consumer retry-consumer.js --docker <your-dockerhub-namespace>/cfn-kafka-runtime-node --web true --param-file ./params.json
```

* Use the command `ic fn action get cfn-eventstreams-sample/retry-consumer --url ` to retrieve the URL of the WebAction

* Open the browser or use cURL to produce new messages: `https://us-south.functions.appdomain.cloud/api/v1/web/<iam-namespace-id>/cfn-eventstreams-sample/producer.json?name=foo&color=bar`

* Use the second URL to re-consume already committed messages `https://us-south.functions.appdomain.cloud/api/v1/web/<iam-namespace-id>/cfn-eventstreams-sample/retry-consumer.json?key=KeyKJ7DG`