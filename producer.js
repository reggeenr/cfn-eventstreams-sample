const Kafka = require('node-rdkafka');
const crypto = require('crypto');

function connectProducer(producerOpts, topicName) {
  const fn = 'connectProducer ';
  console.log(`${fn}>`);
  return new Promise((resolve, reject) => {
    // Create Kafka producer
    var topicOpts = {
      'request.required.acks': -1,
      'produce.offset.report': true
    };

    producer = new Kafka.Producer(producerOpts, topicOpts);
    producer.setPollInterval(100);

    // Register listener for debug information; only invoked if debug option set in driver_options
    producer.on('event.log', function (log) {
      console.log(log);
    });

    // Register error listener
    producer.on('event.error', function (err) {
      console.error('Error from producer:' + JSON.stringify(err));
      reject(err);
    });

    // Register delivery report listener
    producer.on('delivery-report', function (err, dr) {
      if (err) {
        console.error('Delivery report: Failed sending message ' + dr.value);
        console.error(err);
        // We could retry sending the message
      } else {
        console.log('Message produced, partition: ' + dr.partition + ' offset: ' + dr.offset);
      }
    });

    // Register callback invoked when producer has connected
    producer.on('ready', function () {
      console.log('The producer has connected.');

      // request metadata for all topics
      producer.getMetadata({ timeout: 10000 }, (err, metadata) => {
        if (err) {
          console.error('Error getting metadata: ' + JSON.stringify(err));
          producer.disconnect((err, data) => {
            console.log("producer disconnected")
          });
        } else {
          console.log('Producer obtained metadata: ' + JSON.stringify(metadata));
          var topicsByName = metadata.topics.filter(function (t) {
            return t.name === topicName;
          });
          if (topicsByName.length === 0) {
            console.error('ERROR - Topic ' + topicName + ' does not exist. Exiting');
            producer.disconnect((err, data) => {
              console.log("producer disconnected")
            });
          }
        }
      });

      console.log(`${fn}<`);
      resolve(producer);
    });

    // connect the producer
    producer.connect();
  });
}

function main(params) {
  const fn = 'main ';
  console.log(`${fn}>`);

  return new Promise(async (resolve, reject) => {

    const topicName = params.topic || 'my-topic';
    const partition = null;

    const name = params.name || crypto.randomBytes(10).toString('base64').slice(0, 10);
    const color = params.color || crypto.randomBytes(4).toString('base64').slice(0, 4);

    const message = Buffer.from(`${JSON.stringify({ cats: [ { name, color } ] })}`);
    const messageKey = params.key || `Key${crypto.randomBytes(5).toString('base64').slice(0, 5)}`;
    console.log(`${fn}- messageKey: '${messageKey}', topic '${topicName}', message: '${message}'`);

    const producerOpts = {
      'client.id': 'cfn-eventstreams-sample-producer',
      'dr_msg_cb': true,  // Enable delivery reports with message payload

      //'debug': 'all',
      'metadata.broker.list': params.kafka_brokers_sasl,
      'security.protocol': 'sasl_ssl',
      // 'ssl.ca.location': opts.calocation,
      'sasl.mechanisms': 'PLAIN',
      'sasl.username': 'token',
      'sasl.password': params.api_key,
      'broker.version.fallback': '0.10.0',  // still needed with librdkafka 0.11.6 to avoid fallback to 0.9.0
      'log.connection.close': false
    };

    connectProducer(producerOpts, topicName)
      .then((producer) => {
        try {
          console.log(`${fn}- producing message ...`);
          producer.produce(topicName, partition, message, messageKey);
          console.log(`${fn}- producing message [done]`);
        } catch (err) {
          console.error(`Failed sending message #${messageKey}`);
          throw err;
        } 
        return producer;
      })
      .then((producer) => {
        producer.disconnect((err, data) => {
          if(err) {
            console.error("Failed to disconnect producer!");
            throw err;
          }
          console.log("producer disconnected");
          console.log(`${fn}<`);
          resolve({ status: 'sent!', key: messageKey });
        })
      })
      .catch((err) => {
        console.error(err);
        console.log(`${fn}< - FAILED!`);
        reject(err);
      });
  });
}

exports.main = main;