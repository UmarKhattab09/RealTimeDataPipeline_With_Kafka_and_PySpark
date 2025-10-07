from kafka import KafkaProducer
import json, time, random, uuid
if __name__ == "__main__":
    print("Starting Producer")
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: str(v).encode('utf-8'),
        acks='all'
        )
    pages = ["/home", "/cart", "/product/123", "/checkout"]
    # event = {"order_id": "order3", "amount": 300, "product": "product3","page": random.choice(pages)}
    
    while True:
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 100),
            "page": random.choice(pages),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
        }        
        print(event)
        # event = {"order_id": "order3", "amount": 300, "product": "product3","page": random.choice(pages)}
        future = producer.send("click_events",event)
        try:
            metadata = future.get(timeout=10)

            print(f"Messeage send sucessfully to topic {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
        except Exception as e:
            print("Error sending message",e)
        producer.flush()
        time.sleep(2)

# producer.send("orders","order1")
# producer.send("orders",event)
# producer.flush()
# print("Message sent")
