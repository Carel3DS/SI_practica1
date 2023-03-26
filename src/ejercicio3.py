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
con = sqlite3.connect("database.db")

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) = '07' AND PRIORITY = 1")
cols = [column[0] for column in query.description]
dAlertsJul1 = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) = '07' AND PRIORITY = 2")
cols = [column[0] for column in query.description]
dAlertsJul2 = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) = '07' AND PRIORITY = 3")
cols = [column[0] for column in query.description]
dAlertsJul3 = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) = '08' AND PRIORITY = 1")
cols = [column[0] for column in query.description]
dAlertsAgo1 = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) = '08' AND PRIORITY = 2")
cols = [column[0] for column in query.description]
dAlertsAgo2 = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) = '08' AND PRIORITY = 3")
cols = [column[0] for column in query.description]
dAlertsAgo3 = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)


cursor = con.cursor()

#Número de observaciones y valores ausentes (mostrados como "Otras fechas" u "Otras alertas")
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

#Mediana
print("\n")
mediana1 = (dAlertsJul1["sid"].count()+dAlertsAgo1["sid"].count()+1)/2
mediana2 = (dAlertsJul2["sid"].count()+dAlertsAgo2["sid"].count()+1)/2
mediana3 = (dAlertsJul3["sid"].count()+dAlertsAgo3["sid"].count()+1)/2
medianaJulio = (dAlertsJul1["sid"].count()+dAlertsJul2["sid"].count()+dAlertsJul3["sid"].count()+1)/2
medianaAgosto = (dAlertsAgo1["sid"].count()+dAlertsAgo2["sid"].count()+dAlertsAgo3["sid"].count()+1)/2
medianaTot = (dAlertsJul1["sid"].count()+dAlertsJul2["sid"].count()+dAlertsJul3["sid"].count()+dAlertsAgo1["sid"].count()+dAlertsAgo2["sid"].count()+dAlertsAgo3["sid"].count()+1)/2

print("La mediana de las alertas de prioridad 1 es la alerta número", mediana1)
print("La mediana de las alertas de prioridad 2 es la alerta número", mediana2)
print("La mediana de las alertas de prioridad 3 es la alerta número", mediana3)
print("La mediana de las alertas del mes de julio es la alerta de número", medianaJulio)
print("La mediana de las alertas del mes de agosto es la alerta de número", medianaAgosto)
print("La mediana de todas las alertas es la alerta número", medianaTot)
print("\n")

#Media
media1 = (dAlertsJul1["sid"].count()+dAlertsAgo1["sid"].count())/2
media2 = (dAlertsJul2["sid"].count()+dAlertsAgo2["sid"].count())/2
media3 = (dAlertsJul3["sid"].count()+dAlertsAgo3["sid"].count())/2
mediaTot = (dAlertsJul1["sid"].count()+dAlertsJul2["sid"].count()+dAlertsJul3["sid"].count()+dAlertsAgo1["sid"].count()+dAlertsAgo2["sid"].count()+dAlertsAgo3["sid"].count())/2

print("La media mensual de las alertas de prioridad 1 es de", media1, "alertas al mes")
print("La media mensual de las alertas de prioridad 2 es de", media2, "alertas al mes")
print("La media mensual de las alertas de prioridad 3 es de", media3, "alertas al mes")
print("La media mensual de todas las alertas es de", mediaTot, "alertas al mes")
print("\n")

#Varianza
varianza1 = ((dAlertsJul1["sid"].count()-media1)**2+(dAlertsAgo1["sid"].count()-media1)**2)/2
varianza2 = ((dAlertsJul2["sid"].count()-media1)**2+(dAlertsAgo2["sid"].count()-media1)**2)/2
varianza3 = ((dAlertsJul3["sid"].count()-media1)**2+(dAlertsAgo3["sid"].count()-media1)**2)/2
varianzaTot = ((dAlertsJul1["sid"].count()+dAlertsJul2["sid"].count()+dAlertsJul3["sid"].count()-mediaTot)**2+(dAlertsAgo1["sid"].count()+dAlertsAgo2["sid"].count()+dAlertsAgo3["sid"].count()-mediaTot)**2)/2

print("La varianza mensual de las alertas de prioridad 1 es de", varianza1, "alertas al cuadrado al mes")
print("La varianza mensual de las alertas de prioridad 2 es de", varianza2, "alertas al cuadrado al mes")
print("La varianza mensual de las alertas de prioridad 3 es de", varianza3, "alertas al cuadrado al mes")
print("La varianza mensual de todas las alertas es de", varianzaTot, "alertas al mes")
print("\n")

#Valores Máximos y mínimos


con.close()
