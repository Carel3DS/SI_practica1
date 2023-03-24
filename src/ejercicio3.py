# EJERCICIO 3
#Para crear en una base de datos los datos del archivo alerts.csv

import sqlite3
import csv

conn = sqlite3.connect('practica1csv.db')

cursor = conn.cursor()
cursor.execute('''CREATE TABLE alertas
                   (timestamp datetime, sid int, msg text, clasification text, priority int, protocol text, origin text, destination text, port int)''')

with open('alerts.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        cursor.execute("INSERT INTO alertas (timestamp, sid, msg, clasification, priority, protocol, origin, destination, port) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)

conn.commit()
conn.close()
