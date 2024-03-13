import { check } from "k6";
import { Writer, Reader, SchemaRegistry, CODEC_SNAPPY, SCHEMA_TYPE_JSON } from "k6/x/kafka";
import http from "k6/http";

const brokers = ["localhost:9092"];
const topic = "test_indatacore_kafka";

const writer = new Writer({
  brokers: brokers,
  topic: topic,
  autoCreateTopic: true,
  compression: CODEC_SNAPPY,
});

const reader = new Reader({
  brokers: brokers,
  topic: topic,
});

const schemaRegistry = new SchemaRegistry();

export const options = {
  thresholds: {
    kafka_writer_error_count: ["count == 0"],
    kafka_reader_error_count: ["count == 0"],
  },
};

export default function () {
  for (let index = 0; index < 100; index++) {
    const url = "http://localhost:8080/api/v1/messages";
    const data = {
      message: "Dione indatacore send a request in kafka",
      author: "Ousseynou Dione",
      index: index 
    };   
    const params = {
      headers: {
        "Content-Type": "application/json"
      }
    };
    const response = http.post(url, JSON.stringify(data), params); 
    if (response.status == 200) {
      let messages = [
        {
       //   key: schemaRegistry.serialize({
       //     data: {
       //       correlationId: "test-id-abc-" + index,
      //      },
      //      schemaType: SCHEMA_TYPE_JSON,
      //    }),
          value: schemaRegistry.serialize({
            data,
            schemaType: SCHEMA_TYPE_JSON,
          }),
          headers: {
            mykey: "myvalue",
          },
          offset: index,
          partition: 0,
          time: new Date(),
        },
      ];
      
      writer.produce({ messages: messages });
    }
    
  }

  let messages = reader.consume({ limit: 10 });

  check(messages, {
    "10 messages are received": (messages) => messages.length == 10,
  });

  check(messages[0], {
    "Topic equals to test_indatacore_kafka": (msg) => msg["topic"] == topic,
  //  "Key contains key/value and is JSON": (msg) =>
  //    schemaRegistry
   //     .deserialize({ data: msg.key, schemaType: SCHEMA_TYPE_JSON })
   //     .correlationId.startsWith("test-id-"),
  //  "Value contains key/value and is JSON": (msg) =>
    //  typeof schemaRegistry.deserialize({
  //      data: msg.value,
    //    schemaType: SCHEMA_TYPE_JSON,
   //   }) == "object" &&
   //   schemaRegistry.deserialize({
     //   data: msg.value,
    //    schemaType: SCHEMA_TYPE_JSON,
   //   }).name == "xk6-kafka",
    "Header equals {'mykey': 'myvalue'}": (msg) =>
      "mykey" in msg.headers &&
      String.fromCharCode(...msg.headers["mykey"]) == "myvalue",
    "Time is past": (msg) => new Date(msg["time"]) < new Date(),
    "Partition is zero": (msg) => msg["partition"] == 0,
    "Offset is gte zero": (msg) => msg["offset"] >= 0,
    "High watermark is gte zero": (msg) => msg["highWaterMark"] >= 0,
  });
}

export function teardown(data) {
  writer.close();
  reader.close();
}
