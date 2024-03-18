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
      vus: 5,   // Nombre de utilisateurs virtuels
      duration: "5s",  // Durée du test de charge
    //  gracefulStop: "5s",
    },
  },
};

const brokers = ["192.168.1.212:9092"];
const producerTopic = "Display-line-Balance-producer-topic";
const consumerTopic = "Display-line-Balance-consumer-topic";

// Configurations SASL (optionnel)
const saslConfig = {
  username: "admin",
  password: "admin-secret",
  algorithm: SASL_SCRAM_SHA512,
};

// Configuration des offset et partition
const offset = 0;
const partition = 0;

// Initialisation du Writer et du Reader
const writer = new Writer({
  brokers: brokers,
  topic: producerTopic,
  sasl: saslConfig,
});
const reader = new Reader({
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
  // Production de messages vers le topic producteur
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
              uuid: "1687798007798431503"
            }
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
    ];

    writer.produce({ messages: messages });
  }

  // Attendre un court instant pour que les messages soient disponibles pour la consommation
  sleep(1);

  // Consommation de messages du topic consommateur
  let messagesConsumer = reader.consume({ limit: 10 });
  check(messagesConsumer, {
    "at least one message returned": (msgs) => msgs.length > 0,
  });

  // Vérification que les messages proviennent du topic producteur
  for (let msg of messagesConsumer) {
    check(msg.topic === consumerTopic, {
      "message is from producer topic": () => msg.topic === consumerTopic,
    });
  }

  // Vérification de la structure et du contenu des messages
  for (let msg of messagesConsumer) {
    const deserialized = schemaRegistry.deserialize({
      data: msg.value,
      schemaType: SCHEMA_TYPE_JSON,
    });

    check(deserialized, {
      "message has expected structure": () => deserialized !== null && deserialized !== undefined,
      "value is correct": () => {
        return deserialized.IBSubscriber.ib_mdn === "123456" &&
               deserialized.IBSubscriber.ib_level === "1" &&
               deserialized.IBSubscriber.ib_levelRetireTime === "2023-06-27T16:46:47" &&
               deserialized.IBSubscriber.ib_sublevel === "" &&
               deserialized.IBOperation.origin === "CVM" &&
               deserialized.IBOperation.user === "CVM-SYS" &&
               deserialized.IBOperation.uuid === "1687798007798431503";
      }
    });

    // Ajouter des étiquettes aux métriques pour distinguer les topics
    const topicLabels = { topic: consumerTopic }; // Étiquettes pour le topic consommateur
    const topicProducerLabels = { topic: producerTopic }; // Étiquettes pour le topic producteur

    // Enregistrer les métriques avec les étiquettes appropriées
    // Par exemple, pour le nombre de messages consommés
    const metricName = 'kafka_consumer_messages_count';
    const metricValue = 1; // Nombre de messages consommés
    const metricTags = topicLabels; // Utilisation des étiquettes du topic consommateur
    const metric = {}; // Définition de la métrique
    metric[metricName] = metricValue; // Définition de la valeur de la métrique
    // Émission de la métrique avec les étiquettes spécifiées
    check(metric, metricTags);

    // Vous pouvez faire de même pour d'autres métriques que vous souhaitez enregistrer
    // Assurez-vous de définir correctement les noms et valeurs des métriques pour correspondre à votre cas d'utilisation
  }
}



// Fermeture des connections
export function teardown(data) {
  writer.close();
  reader.close();
  connection.close();
}
