const Kafka = require('node-rdkafka');
const openwhisk = require('openwhisk');

function main(params) {
  const fn = 'main ';
  console.log(`${fn}>`);

  return new Promise(async (resolve, reject) => {

    const topicName = params.topic || 'my-topic';
    const partition = params.partition ? parseInt(params.partition, 10) : 0;
    const offset = params.offset ? parseInt(params.offset, 10) : 0;
    const messageKey = params.key;

    console.log(`${fn}- topic '${topicName}', partition: '${partition}', offset: '${offset}', messageKey: '${messageKey}'`);

    const consumerOpts = {
      //'debug': 'all',
      'metadata.broker.list': params.kafka_brokers_sasl,
      'security.protocol': 'sasl_ssl',
      'sasl.mechanisms': 'PLAIN',
      'sasl.username': 'token',
      'sasl.password': params.api_key,
      'broker.version.fallback': '0.10.0',  // still needed with librdkafka 0.11.6 to avoid fallback to 0.9.0
      'log.connection.close': false,

      // consumer specifc
      'group.id': 're-consume-messages',
      'enable.auto.commit': false
    };

    const consumer = new Kafka.KafkaConsumer(consumerOpts);

    // logging debug messages, if debug is enabled
    consumer.on('event.log', function (log) {
      console.log(log);
    });

    // logging all errors
    consumer.on('event.error', function (err) {
      console.error('Error from consumer');
      console.error(err);
      reject(err);
    });

    consumer.on('ready', function (arg) {
      console.log('consumer ready.' + JSON.stringify(arg));

      // see https://blizzard.github.io/node-rdkafka/current/KafkaConsumer.html
      consumer.assign([{ topic: topicName, partition: partition, offset: offset }]);

      // start consuming messages
      consumer.consume();
    });

    consumer.on('data', function (m) {

      console.log(`examinging message '${m.key}' (offset: '${m.offset}')`);

      if (messageKey && messageKey === `${m.key}`) {
        console.log(`Found message ${m.key} by key ${messageKey}: ${JSON.stringify(m)}`);

        // stringify key and and convert the value to a proper JSON
        const retryMessage = m;
        retryMessage.key = `${m.key}`;
        retryMessage.value = JSON.parse(`${m.value}`);

        // utilize OpenWhisk SDK to trigger the message process again
        ow = openwhisk();
        const actionName = 'cfn-eventstreams-sample/process-message';
        const blocking = false;
        const actionParams = { messages: [retryMessage] };
        ow.actions.invoke({ name: actionName, blocking, params: actionParams }).then((result => {
          console.log(`invoked action details: ${JSON.stringify(result)}`);
          console.log(`${fn}< Message '${messageKey}' will be processed again`);
          resolve(result);
          return;
        }))
        .catch((err) => {
          console.error(err);
          reject({ result: `message '${messageKey}' found but failed to invoke '${actionName}'!` });
        });
        
      }
    });

    consumer.on('disconnected', function (arg) {
      console.log('consumer disconnected. ' + JSON.stringify(arg));
    });

    // starting the consumer
    consumer.connect();

    // stopping this example after 10s
    setTimeout(function () {
      consumer.disconnect();
      reject({ result: 'not found!' });
    }, 10000);
  });
}

exports.main = main;