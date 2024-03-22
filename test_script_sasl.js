
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
      vus: 5,   // Réglages ici du nombre de user virtuels
      duration: "2m",  // Durée de test de charge
      gracefulStop: "5s",
    },
  },
};

const brokers = ["192.168.1.212:9092"];
const topic = "Display-line-Balance-producer-topic";
const topicFinal = "Display-line-Balance-consumer-topic";

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
  topic: topic,topicFinal,
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
    for (let index = 0; index < 5; index++) {
      let messages = [
        {
        /*  key: schemaRegistry.serialize({
            data: {
              correlationId: "test-id-abc-" + index,
            },
            schemaType: SCHEMA_TYPE_JSON,
          }),*/

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
                uuid: "1687798007798431503"
              }
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
      "value is correct": (msgs) => {
        const deserialized = schemaRegistry.deserialize({
          data: msgs[0].value,
          schemaType: SCHEMA_TYPE_JSON,
        })
        return deserialized.IBSubscriber.ib_mdn === "123456" && // Vérifiez avec les valeurs réelles envoyées
               deserialized.IBSubscriber.ib_level === "1" &&
               deserialized.IBSubscriber.ib_levelRetireTime === "2023-06-27T16:46:47" &&
               deserialized.IBSubscriber.ib_sublevel === "" &&
               deserialized.IBOperation.origin === "CVM" &&
               deserialized.IBOperation.user === "CVM-SYS" &&
               deserialized.IBOperation.uuid === "1687798007798431503";
      }
    });
  }


  

export function teardown(data) {
  writer.close();
  reader.close();
  connection.close();
}