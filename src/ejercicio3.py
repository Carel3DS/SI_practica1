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

#Para agrupar por prioridad de alerta y fecha
conn = sqlite3.connect('practica1csv.db') 
 
cursor = conn.cursor() 
 
cursor.execute(''' 
    SELECT 
        CASE 
            WHEN priority = 1 THEN 'Alertas graves' 
            WHEN priority = 2 THEN 'Alertas medias' 
            WHEN priority = 3 THEN 'Alertas bajas' 
            ELSE 'Otras alertas' 
        END AS prioridad, 
        CASE 
            WHEN timestamp BETWEEN '2022-07-01 00:00:00' AND '2022-07-31 23:59:59' THEN 'Julio' 
            WHEN timestamp BETWEEN '2022-08-01 00:00:00' AND '2022-08-31 23:59:59' THEN 'Agosto' 
            ELSE 'Otras fechas' 
        END AS mes, 
        COUNT(*) AS cantidad 
    FROM alertas 
    GROUP BY prioridad, mes; 
''') 
 
for row in cursor: 
    print(row) 
conn.close() 
