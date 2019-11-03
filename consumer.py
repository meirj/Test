import csv
import json
import sqlite3
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import pika


engine = create_engine('sqlite:///chinook.db', echo=False)
conn = engine.connect()

# Exercise 1 - create csv file
def q1():
    query = '''
    SELECT a.country, count(b.invoiceid) as numOfInvoices FROM customers a, invoices b  
    where a.customerid = b.customerid
    group by a.country;
    '''
    result = conn.execute(query)
    listResultQuery = result.fetchall()
    with open('Q1.csv', 'w', encoding='utf-8', newline='') as f:
        wr = csv.writer(f)
        wr. writerow(['Country'] + ['NumOfPurchases'])
        for row in listResultQuery:
            wr.writerow(row)


# Exercise 2 - create csv file
def q2():
    query = '''
    SELECT a.country, count(b.invoicelineid) as numOfItems FROM customers a, invoice_items b, invoices c
    where a.customerid = c.customerid
    and c.invoiceid = b.invoiceid
    group by a.country;
    '''

    result = conn.execute(query)
    listResultQuery = result.fetchall()
    with open('Q2.csv', 'w', encoding='utf-8', newline='') as f:
        wr = csv.writer(f)
        wr. writerow(['Country'] + ['NumOfItems'])
        for row in listResultQuery:
            wr.writerow(row)


# Exercise 3 - create json file
def q3():
    query = '''
    SELECT a.country, e.title FROM customers a, invoices b, invoice_items c, tracks d, albums e
    where a.customerid = b.customerid
    and b.invoiceid = c.invoiceid
    and c.trackid = d.trackid
    and d.albumid = e.albumid
    group by a.country, e.title
    order by a.country;
    '''

    result = conn.execute(query)
    listResultQuery = result.fetchall()
    dictCountyAlbum = {}
    for country, album in listResultQuery:
        if country not in dictCountyAlbum:
            dictCountyAlbum[country] = [album]
        else:
            dictCountyAlbum[country].append(album)

    with open('Q3.json', 'w', encoding='utf-8') as f:
        json.dump(dictCountyAlbum, f, ensure_ascii=False, indent=4)


# Exercise 4 - create xml file
def q4(country, year):
    query = """
    SELECT title, country, MAX(cnt), year
    FROM (
    SELECT a.title, b.country, count(c.invoiceid) cnt, strftime('%Y',c.invoiceDate) as year
    FROM albums a, customers b, invoices c, invoice_items d, tracks e
    WHERE b.customerid = c.customerid
    and c.invoiceid = d.invoiceid
    and e.trackid = d.trackid
    and e.albumid = a.albumid
    and e.genreid = 1
    and UPPER(b.country) like UPPER( """ + country + """)
    and CAST(year as decimal) >= """ + year + """
    group by b.country, a.title
    order by country, cnt desc)
    group by country;
    """

    result = conn.execute(query)
    listResultQuery = result.fetchall()

    root = ET.Element("best-selling")

    ET.SubElement(root, "album").text = listResultQuery[0][0]
    ET.SubElement(root, "country").text = listResultQuery[0][1]
    ET.SubElement(root, "sales").text = str(listResultQuery[0][2])
    ET.SubElement(root, "year").text = listResultQuery[0][3]

    tree = ET.ElementTree(root)
    tree.write("Q4.xml")


# Exercise 5 - create tables from csv and xml files
def q5():
    con = sqlite3.connect("chinook.db")
    cur = con.cursor()

# Create table Q1 with data from exercise 1
    with open('Q1.csv') as q1_csv_file:
        csv_Q1_reader = csv.reader(q1_csv_file, delimiter=',')
        header_Q1 = next(csv_Q1_reader)
        to_db_q1 = [(i[0], i[1]) for i in csv_Q1_reader]

        if(engine.dialect.has_table(conn, 'Q1')):
            cur.execute("DROP TABLE Q1")
            cur.execute("CREATE TABLE Q1 "+str(tuple(header_Q1))+";")
        else:
            cur.execute("CREATE TABLE Q1 "+str(tuple(header_Q1))+";")

        cur.executemany("INSERT INTO Q1 "+str(tuple(header_Q1))+" VALUES (?, ?);", to_db_q1)
        con.commit()

# Create table Q2 with data from exercise 2
    with open('Q2.csv') as q2_csv_file:
        csv_Q2_reader = csv.reader(q2_csv_file, delimiter=',')
        header_Q2 = next(csv_Q2_reader)
        to_db_q2 = [(i[0], i[1]) for i in csv_Q2_reader]

        if(engine.dialect.has_table(conn, 'Q2')):
            cur.execute("DROP TABLE Q2")
            cur.execute("CREATE TABLE Q2 "+str(tuple(header_Q2)) +";")
        else:
            cur.execute("CREATE TABLE Q2 "+str(tuple(header_Q2))+";")

        cur.executemany("INSERT INTO Q2 "+str(tuple(header_Q2))+" VALUES (?, ?);", to_db_q2)
        con.commit()

# Create table Q4 with data from exercise 4
    tree = ET.parse('Q4.xml')
    root = tree.getroot()
    header_Q4 = []
    values_Q4 = []

    for child in root:
        header_Q4.append(child.tag)
        values_Q4.append(child.text)

    if engine.dialect.has_table(conn, 'Q4'):
        cur.execute("DROP TABLE Q4")
        cur.execute("CREATE TABLE Q4" +str(tuple(header_Q4)) + ";")
    else:
        cur.execute("CREATE TABLE Q4" +str(tuple(header_Q4)) + ";")

    cur.execute("INSERT INTO Q4 " + str(tuple(header_Q4)) + " VALUES (?, ?, ?, ?);", tuple(values_Q4))
    con.commit()

    con.close()


# Create connection to queue
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='myQueue')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    inputText = str(body)[4:-3].split(',')
    path = inputText[0]
    country = inputText[1]
    year = inputText[2].replace("'", "")
    engine = create_engine(path, echo=False)
    conn = engine.connect()
    q1();
    q2();
    q3();
    q4(country,  year);
    q5();


channel.basic_consume(queue='myQueue', on_message_callback=callback, auto_ack=True)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()



