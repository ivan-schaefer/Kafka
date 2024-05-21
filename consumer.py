from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'kafka-centos-8:29092',
    'group.id': 'test',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['first_topic'])

for i in range(10):
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
