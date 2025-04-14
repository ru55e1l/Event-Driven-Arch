# consumer_shipping.py
import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def process_shipping(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    student_name = data["student_name"]
    fulfilled_by = data.get("fulfilled_by", "Unknown")
    
    # Simulate shipping the order
    shipping_message = f"Order {order_id} has been shipped to {student_name}. Processed by {fulfilled_by}."
    print(shipping_message)
    
    # Publish order-shipped event
    shipping_data = {
        "order_id": order_id,
        "student_name": student_name,
        "shipped_by": fulfilled_by,
        "status": "shipped"
    }
    
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key="order-shipped",
        body=json.dumps(shipping_data)
    )

def start_shipping_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="shipping_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="shipping_queue", routing_key="order-fulfilled")

    channel.basic_consume(queue="shipping_queue", on_message_callback=process_shipping, auto_ack=True)

    print("Waiting for order-fulfilled messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_shipping_consumer() 