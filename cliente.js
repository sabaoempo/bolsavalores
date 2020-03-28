#!/usr/bin/env node

var amqp = require('amqplib/callback_api');
//import amqp from 'amqp/callback_api'
//var message = document.getElementById('input_message_id').value;

function compra(){
amqp.connect('amqp://localhost', function(error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function(error1, bolsadevalores) {
        if (error1) {
            throw error1;
        }
        var exchange = 'topic_logs';
        var args = process.argv.slice(2);
        var key = (args.length > 0) ? args[0] : 'compra.ativo';
        var msg = args.slice(1).join(' ') || 'TESTE DE SISTEMA';

        bolsadevalores.assertExchange(exchange, 'topic', {
            durable: false
        });
        bolsadevalores.publish(exchange, key, Buffer.from(msg));
        console.log(" [x] Sent %s: '%s'", key, msg);
    });

    setTimeout(function() {
        connection.close();
        process.exit(0);
    }, 500);
});
}

window.compra = compra