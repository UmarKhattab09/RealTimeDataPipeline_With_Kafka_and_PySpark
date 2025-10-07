from kafka import KafkaConsumer

if __name__ == "__main__":
    print("Starting Consumer")
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092',
        # auto_offset_reset='earliest',
        # enable_auto_commit=True,
        # group_id='my-group',
        value_deserializer=lambda x: x.decode('utf-8')
        )
    consumer.subscribe(['click_events'])
    
    for message in consumer:
        print(f"Received message: {message.value}")

# ------------------------------------------------------------------------------
# from kafka import KafkaConsumer
# if __name__ == "__main__":
#     print("Starting Consumer...")

#     consumer = KafkaConsumer(
#         'orders',
#         bootstrap_servers='localhost:9092',
#         auto_offset_reset='earliest',     # start from earliest if no previous offset
#         enable_auto_commit=False,         # disable auto commit for manual control
#         group_id='my-group',              # required for commit to work
#         value_deserializer=lambda x: x.decode('utf-8')
#     )

#     print("Listening for messages on topic: 'orders'")

#     for message in consumer:
#         print(f"✅ Received message: {message.value}")

#         # Process your message here (save to DB, transform, etc.)

#         # Manually commit offset after processing
#         consumer.commit()
#         print("Offset committed.\n")
    