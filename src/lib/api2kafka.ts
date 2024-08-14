/**
 * Use http to send data to kafka
 */
import nodeFetch from 'node-fetch';
import dateFormat from 'dateformat';
import debug from 'debug';

const log = debug('umami:api2kafka');

log(`Kafka API: ${process.env.KAFKA_API}`);

type KafkaMessage = { [key: string]: string | number };

function getDateFormat(date: Date, format?: string): string {
  return dateFormat(date, format ? format : 'UTC:yyyy-mm-dd HH:MM:ss');
}

const send2kafka = async (message: KafkaMessage | KafkaMessage[], topic: string) => {
  return nodeFetch(process.env.KAFKA_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      table: topic,
      json: JSON.stringify(message),
    }),
  });
};

const sendMessage = async (message: KafkaMessage, topic: string) => send2kafka(message, topic);

const sendMessages = async (message: KafkaMessage[], topic: string) => send2kafka(message, topic);

export default {
  getDateFormat,
  send2kafka,
  sendMessage,
  sendMessages,
};
