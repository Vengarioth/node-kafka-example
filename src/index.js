import '@babel/polyfill';
import bodyParser from 'body-parser';
import express from 'express';
import dotenv from 'dotenv';

import kafka, { KeyedMessage } from 'kafka-node';

if(process.env.NODE_ENV !== 'production') {
    dotenv.config();
}

(async () => {
    const app = express();
    
    const client = new kafka.KafkaClient({ kafkaHost: process.env.KAFKA_HOST });

    const userConsumer = true;
    if(userConsumer) {
        const consumer = new kafka.Consumer(client, [{ topic: 'test', partition: 0 }], { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 });
    
        consumer.on('message', (message) => console.log(message));
        consumer.on('error', (error) => console.error(error));
        consumer.on('offsetOutOfRange', (topic) => {
            console.error(new Error(`Offset on Topic ${topic.topic} in partition ${topic.partition} out of range.`));
        });
    }

    const producer = new kafka.Producer(client, { requireAcks: 1 });

    producer.on('ready', () => {
        console.log('producer ready');

        app.get('/', (req, res) => {
            producer.send([{
                topic: 'test',
                messages: [new KeyedMessage('cf149295-fe58-4552-99a0-7cd93716a195', 'test message')],
            }], (error, result) => {
                if(error) {
                    console.error(error);
                    res.status(500);
                    res.json(error);
                    return;
                }

                res.status(201);
                res.json(result);
            });
        });
    });

    app.use(bodyParser.json({ type: 'application/json' }));
    
    app.listen(process.env.PORT, () => {
        console.log(`listening on port ${process.env.PORT}`);
    });
})();
