/*

Ce script est conçu pour tester la charge d'un cluster Kafka en utilisant xk6. Il envoie 200 messages JSON par itération.

*/

import { check } from "k6";

import {
  Writer,
  Reader,
  Connection,
  SchemaRegistry,
  CODEC_SNAPPY,
  SCHEMA_TYPE_JSON,
} from "k6/x/kafka"; // Importation de l'extension Kafka pour xk6

// Définition des constantes pour les brokers Kafka et le sujet du topic
const brokers = ["localhost:9092"];
const topic = "xk6_kafka_json_topic";

// Initialisation des objets Writer, Reader, Connection et SchemaRegistry
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
const connection = new Connection({
  address: brokers[0],
});
const schemaRegistry = new SchemaRegistry();

// Création du topic si l'utilisateur virtuel est 0 (pour la première itération)
if (__VU == 0) {
  connection.createTopic({
    topic: topic,
    configEntries: [
      {
        configName: "compression.type",
        configValue: CODEC_SNAPPY,
      },
    ],
  });
}

// Définition des seuils de test pour les erreurs d'écriture et de lecture Kafka
export const options = {
  thresholds: {
    kafka_writer_error_count: ["count == 0"], // Nombre d'erreurs d'écriture doit être 0
    kafka_reader_error_count: ["count == 0"], // Nombre d'erreurs de lecture doit être 0
  },
};

// Fonction principale exécutée par chaque utilisateur virtuel
export default function () {
  // Boucle pour envoyer 100 itérations de messages
  for (let index = 0; index < 100; index++) {
    let messages = [
      {
        // Clé de type JSON
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-abc-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        // Valeur de type JSON
        value: schemaRegistry.serialize({
          data: {
            name: "test indatacore xk6-kafka",
            version: "0.9.0",
            author: "Ousseynou Dione",
            description:
              "k6 extension to load test Apache Kafka with support for Avro messages",
            index: index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        headers: {
          mykey: "myvalue",
        },
        offset: index,
        partition: 0,
        time: new Date(), // Timestamp actuel
      },
      {
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-def-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: {
            name: "xk6-kafka",
            version: "0.9.0",
            author: "Mostafa Moradian",
            description:
              "k6 extension to load test Apache Kafka with support for Avro messages",
            index: index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        headers: {
          mykey: "myvalue",
        },
      },
    ];

    // Envoyer les messages à Kafka
    writer.produce({ messages: messages });
  }

  // Lire 10 messages seulement
  let messages = reader.consume({ limit: 10 });

  // Vérifier si 10 messages ont été reçus
  check(messages, {
    "10 messages are received": (messages) => messages.length == 10,
  });

  // Vérification des propriétés de chaque message
  check(messages[0], {
    "Topic equals to xk6_kafka_json_topic": (msg) => msg["topic"] == topic,
    "Key contains key/value and is JSON": (msg) =>
      schemaRegistry
        .deserialize({ data: msg.key, schemaType: SCHEMA_TYPE_JSON })
        .correlationId.startsWith("test-id-"),
    "Value contains key/value and is JSON": (msg) =>
      typeof schemaRegistry.deserialize({
        data: msg.value,
        schemaType: SCHEMA_TYPE_JSON,
      }) == "object" &&
      schemaRegistry.deserialize({
        data: msg.value,
        schemaType: SCHEMA_TYPE_JSON,
      }).name == "xk6-kafka",
    "Header equals {'mykey': 'myvalue'}": (msg) =>
      "mykey" in msg.headers &&
      String.fromCharCode(...msg.headers["mykey"]) == "myvalue",
    "Time is past": (msg) => new Date(msg["time"]) < new Date(),
    "Partition is zero": (msg) => msg["partition"] == 0,
    "Offset is gte zero": (msg) => msg["offset"] >= 0,
    "High watermark is gte zero": (msg) => msg["highWaterMark"] >= 0,
  });
}

// Fonction de nettoyage exécutée après chaque itération des utilisateurs virtuels
export function teardown(data) {
  // Supprimer le topic si l'utilisateur virtuel est 0
  if (__VU == 0) {
    connection.deleteTopic(topic);
  }
  // Fermer les connexions Writer, Reader et Connection
  writer.close();
  reader.close();
  connection.close();
}
