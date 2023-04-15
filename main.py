import pika
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
print(key)
supabase: Client = create_client(url, key)
client = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = client.channel()


def mark_attendance(ch, method, properties, body):
    body = body.decode('utf-8')
    person_id, schedule_id = body.split('|')

    try:
        data, count = supabase.table('attendance').insert({"schedule_id": schedule_id, "person": person_id}).execute()
        print(data, count)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[x] Marking attendance for person id {person_id} and schedule id {schedule_id}")
    except Exception:
        print(f"Could not mark attendance!")


if __name__ == "__main__":
    channel.basic_consume(queue='personid|schedule', on_message_callback=mark_attendance)
    channel.start_consuming()
