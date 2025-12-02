import pika
import json
import glob
import os
import time

# 🧩 Cấu hình CloudAMQP
amqp_url = "amqps://ogquehkk:VivAP3uU3G-6EoXN_uLmz6zv24_W_OIN@armadillo.rmq.cloudamqp.com/ogquehkk"
params = pika.URLParameters(amqp_url)

connection = pika.BlockingConnection(params)
channel = connection.channel()

queue_name = "job_files"
channel.queue_declare(queue=queue_name, durable=True)

# 🧾 Đọc tất cả file JSON
files = sorted(glob.glob("./data/job_data_*.json"))

for file_path in files:
    filename = os.path.basename(file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()  # đọc toàn bộ nội dung file

        # Gói dữ liệu kèm tên file
        message = json.dumps({
            "filename": filename,
            "content": content
        })

        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        print(f"📤 Đã gửi file {filename} ({len(content)} ký tự) lên queue '{queue_name}'")
        time.sleep(0.5)

connection.close()
print("✅ Hoàn tất gửi toàn bộ file.")
