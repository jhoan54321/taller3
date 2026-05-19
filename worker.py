import pika
import time

print("Iniciando worker...", flush=True)

while True:
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='rabbitmq',
                heartbeat=600
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue='reservas')

        print("Worker conectado a RabbitMQ", flush=True)
        break

    except Exception as e:
        print(f"Esperando RabbitMQ... {e}", flush=True)
        time.sleep(5)

def callback(ch, method, properties, body):
    print("Evento recibido:", body.decode(), flush=True)

channel.basic_consume(
    queue='reservas',
    on_message_callback=callback,
    auto_ack=True
)

print("Worker activo esperando eventos...", flush=True)

channel.start_consuming()
