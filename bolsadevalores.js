#!/usr/bin/env node

var amqp = require('amqplib/callback_api');

//módulo para facilitar a conversão do arquivo csv para json
const csv = require('csv-parser')
const fs = require('fs')
const results = [];
 
fs.createReadStream('acoes_bovespa.csv')
  .pipe(csv({
      options: ['Código', 'Pregão', 'Descrição'],
  }))
  .on('data', (data) => results.push(data))
  .on('end', () => {
    console.log(results);
  });

var args = process.argv.slice(2);

if (args.length == 0) {
    console.log("Usage: receive_logs_topic.js <facility>.<severity>");
    process.exit(1);
}

amqp.connect('amqp://localhost', function(error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function(error1, bolsadevalores) {
        if (error1) {
            throw error1;
        }
        var exchange = 'topic_logs';

        bolsadevalores.assertExchange(exchange, 'topic', {
            durable: false
        });

        bolsadevalores.assertQueue('', {
            exclusive: true
        }, function(error2, broker) {
            if (error2) {
                throw error2;
            }
            console.log(' [*] Waiting for logs. To exit press CTRL+C');

            args.forEach(function(key) {
                bolsadevalores
    .bindQueue(broker.queue, exchange, key);
            });

            bolsadevalores
.consume(broker.queue, function(msg) {
                console.log(" [x] %s:'%s'", msg.fields.routingKey, msg.content.toString());
            }, {
                noAck: true
            });
        });
    });
});