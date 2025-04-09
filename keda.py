from confluent_kafka import KafkaException, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from kubernetes import client, config
import hvac
import tempfile

vault = hvac.Client(url="https://vault.mycompany.local", token="s.your_token_here")
secrets = vault.secrets.kv.v2.read_secret_version(path="kafka/tls")["data"]["data"]

truststore = tempfile.NamedTemporaryFile(delete=False)
keystore = tempfile.NamedTemporaryFile(delete=False)
truststore.write(secrets["truststore_pem"].encode())
keystore.write(secrets["keystore_pem"].encode())
truststore.close()
keystore.close()

kafka_config = {
    "bootstrap.servers": "kafka.mycompany.local:9093",
    "security.protocol": "SSL",
    "ssl.ca.location": truststore.name,
    "ssl.certificate.location": keystore.name,
    "ssl.key.location": keystore.name,
    "group.id": "my-consumer-group",
    "enable.auto.commit": False,
}

topic = "my-topic"

consumer = Consumer(kafka_config)
partitions = consumer.list_topics(topic).topics[topic].partitions
topic_partitions = [TopicPartition(topic, p) for p in partitions]
consumer.assign(topic_partitions)

consumer_offsets = consumer.position(topic_partitions)  
end_offsets = consumer.get_watermark_offsets

total_lag = 0
for tp in topic_partitions:
    committed = consumer.position([tp])[0].offset
    low, high = consumer.get_watermark_offsets(tp)
    lag = max(high - committed, 0)
    total_lag += lag

consumer.close()

print(f"Total Kafka lag: {total_lag}")

if total_lag > 10000:
    replicas = 5
elif total_lag > 1000:
    replicas = 3
else:
    replicas = 1

config.load_kube_config()
apps = client.AppsV1Api()

apps.patch_namespaced_deployment_scale(
    name="my-consumer",
    namespace="default",
    body={"spec": {"replicas": replicas}},
)

print(f"Deployment scaled to {replicas} replicas")
