import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel
channel.queue_declare(queue='myQueue')
path = input("Write DB path (press enter for defult): ")
country = input('Country: ')
year = input('Year: ')

if(path == ''):
    path = 'sqlite:///chinook.db'

message = (path, country, year)
print(message)
channel.basic_publish(exchange='', routing_key='myQueue', body=str(message))
print("Sent")
connection.close()