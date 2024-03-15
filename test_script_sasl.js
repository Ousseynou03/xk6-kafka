

import { check } from "k6";
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
      vus: 50,   // Réglages ici du nombre de user virtuels
      duration: "10s",  // Durée de test de charge
      gracefulStop: "1s",
    },
  },
};

const brokers = ["192.168.1.212:9092"];
const topic = "Display-line-Balance-producer-topic";

// SASL config is optional
const saslConfig = {
  username: "admin",
  password: "admin-secret",
  algorithm: SASL_SCRAM_SHA512,
};


const offset = 0;
// partition and groupId are mutually exclusive
const partition = 0;

const writer = new Writer({
  brokers: brokers,
  topic: topic,
  sasl: saslConfig,
});
const reader = new Reader({
  brokers: brokers,
  topic: topic,
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
  /*
if (__VU == 0) {
  connection.createTopic({
    topic: topic,
    numPartitions: numPartitions,
    replicationFactor: replicationFactor,
  });
  console.log(
    "Existing topics: ",
    connection.listTopics(saslConfig),
  );
}
*/

export default function () {
  for (let index = 0; index < 100; index++) {
    let messages = [

      {
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-abc-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: {
            name: "Dione send a message",
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
      
    ];

    writer.produce({ messages: messages });
  }

  // Read 10 messages only
  let messages = reader.consume({ limit: 10 });
  check(messages, {
    "10 messages returned": (msgs) => msgs.length == 10,
    "key is correct": (msgs) =>
      schemaRegistry
        .deserialize({ data: msgs[0].key, schemaType: SCHEMA_TYPE_JSON })
        .correlationId.startsWith("test-id-"),
    "value is correct": (msgs) =>
      schemaRegistry.deserialize({
        data: msgs[0].value,
        schemaType: SCHEMA_TYPE_JSON,
      }).name == "Dione send a message",
  });
}

export function teardown(data) {
  writer.close();
  reader.close();
  connection.close();
}