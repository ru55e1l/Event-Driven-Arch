import pika
import json
import time
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def process_order_fulfillment(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    student_name = data["student_name"]
    
    # wait, simulating fulfilling
    print(f"[{student_name}] Fulfillment: Order {order_id} is being fulfilled...")
    time.sleep(2)  # Simulate some processing time
    
    fulfillment_data = {
        "order_id": order_id,
        "student_name": student_name,
        "status": "fulfilled",
        "fulfilled_by": student_name
    }
    
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key="order-fulfilled",
        body=json.dumps(fulfillment_data)
    )
    
    print(f"[{student_name}] Fulfillment: Order {order_id} fulfilled. Events published.")

def start_fulfillment_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="fulfillment_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="fulfillment_queue", routing_key="payment-applied")

    channel.basic_consume(queue="fulfillment_queue", on_message_callback=process_order_fulfillment, auto_ack=True)

    print("Waiting for payment-applied events...")
    channel.start_consuming()

if __name__ == "__main__":
    start_fulfillment_consumer() 