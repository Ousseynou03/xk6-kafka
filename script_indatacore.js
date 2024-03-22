import http from 'k6/http';

import { check, sleep } from "k6";
import {
  Writer,
  Reader,
  Connection,
  SchemaRegistry,
  SCHEMA_TYPE_JSON,
  SASL_PLAINTEXT,
  SASL_SCRAM_SHA512,
} from "k6/x/kafka"; // import kafka extension

export const options = {
  scenarios: {
    sasl_auth: {
      executor: "constant-vus",
      vus: 5,   // Nombre d'utilisateurs virtuels
      duration: "5s",  // Durée du test de charge
    //  gracefulStop: "5s",
    },
  },
};

const brokers = ["192.168.1.xxx:9092"];
const producerTopic = "Display-line-Balance-producer-topic";
const consumerTopic = "Display-line-Balance-consumer-topic";

// Configurations SASL (optionnel)
const saslConfig = {
  username: "xxx",
  password: "xxx",
  algorithm: SASL_SCRAM_SHA512,
};

// Configuration des offset et partition
const offset = 0;
const partition = 0;

// Initialisation du Writer et du Reader pour Display-line-Balance-producer-topic
const producerWriter = new Writer({
  brokers: brokers,
  topic: producerTopic,
  sasl: saslConfig,
});

const producerReader = new Reader({
  brokers: brokers,
  topic: producerTopic,
  partition: partition,
  offset: offset,
  sasl: saslConfig,
});

// Initialisation du Reader pour Display-line-Balance-consumer-topic
const consumerReader = new Reader({
  brokers: brokers,
  topic: consumerTopic,
  partition: partition,
  offset: offset,
  sasl: saslConfig,
});

const connection = new Connection({
  address: brokers[0],
  sasl: saslConfig,
});
const schemaRegistry = new SchemaRegistry();

export const options1 = {
  thresholds: {
    kafka_writer_error_count: ["count == 0"],
    kafka_reader_error_count: ["count == 0"],
  },
};

export default function () {
  // Production de messages vers le topic producteur Display-line-Balance-producer-topic
  for (let index = 0; index < 5; index++) {
    let messages = [
      {
        value: schemaRegistry.serialize({
          data: {
            IBSubscriber: {
              ib_mdn: "123456",
              ib_level: "1",
              ib_levelRetireTime: "2023-06-27T16:46:47",
              ib_sublevel: ""
            },
            IBOperation: {
              origin: "CVM",
              user: "CVM-SYS",
              uuid: "1687798007798431504"
            }
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
    ];

    producerWriter.produce({ messages: messages });
  }

  // Attendre que les messages soient disponible pour le consumer
  sleep(5);

  // Consommation de messages du topic consommateur Display-line-Balance-consumer-topic
  let consumerMessages = consumerReader.consume({ limit: 10 });
  check(consumerMessages, {
    "at least one message returned from consumer topic": (msgs) => msgs.length > 0,
  });

  // Consommation de messages du topic producteur Display-line-Balance-producer-topic
  let producerMessages = producerReader.consume({ limit: 10 });
  check(producerMessages, {
    "at least one message returned from producer topic": (msgs) => msgs.length > 0,
  });

  // Effectuer une requête get sur l'application pour s'assurer qu'il répond d'abord
  http.get("http://192.168.1.39:8080/")


  // Vérification que les messages proviennent du topic producteur (Display-line-Balance-producer-topic)
  for (let msg of producerMessages) {
    check(msg.topic === producerTopic, {
      "message is from producer topic": () => msg.topic === producerTopic,
    });
  }
}

// Fermeture des connections
export function teardown(data) {
  producerWriter.close();
  producerReader.close();
  consumerReader.close();
  connection.close();
}
