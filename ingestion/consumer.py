import pika
import json
import os

# 🧩 Cấu hình CloudAMQP
amqp_url = "amqps://ogquehkk:VivAP3uU3G-6EoXN_uLmz6zv24_W_OIN@armadillo.rmq.cloudamqp.com/ogquehkk"
params = pika.URLParameters(amqp_url)

connection = pika.BlockingConnection(params)
channel = connection.channel()

queue_name = "job_files"
channel.queue_declare(queue=queue_name, durable=True)

output_dir = "crawled_data"
os.makedirs(output_dir, exist_ok=True)

print(f"🎧 Đang chờ các file JSON từ queue '{queue_name}'...")

def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())
        filename = data["filename"]
        content = data["content"]

        file_path = os.path.join(output_dir, filename)

        # Ghi lại file JSON y hệt bản gốc
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        print(f"💾 Đã nhận & lưu file: {file_path}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"⚠️ Lỗi xử lý message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\n🛑 Dừng consumer.")
    connection.close()
