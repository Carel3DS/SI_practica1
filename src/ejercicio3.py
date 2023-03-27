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
con = sqlite3.connect("practica1csv.db")

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) = '07' AND PRIORITY = 1")
data_col = []
for column in query.description:
	data_col.append(column[0])
dAlertsJul1 = pd.DataFrame.from_records(data=query.fetchall(), columns=data_col)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp ) = '07' AND PRIORITY = 2")
data_col = []
for column in query.description:
	data_col.append(column[0])
dAlertsJul2 = pd.DataFrame.from_records(data=query.fetchall(), columns=data_col)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp ) = '07' AND PRIORITY = 3")
data_col = []
for column in query.description:
	data_col.append(column[0])
dAlertsJul3 = pd.DataFrame.from_records(data=query.fetchall(), columns=data_col)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp ) = '08' AND PRIORITY = 1")
data_col = []
for column in query.description:
	data_col.append(column[0])
dAlertsAgo1 = pd.DataFrame.from_records(data=query.fetchall(), columns=data_col)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp ) = '08' AND PRIORITY = 2")
data_col = []
for column in query.description:
	data_col.append(column[0])
dAlertsAgo2 = pd.DataFrame.from_records(data=query.fetchall(), columns=data_col)

query = con.execute("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp ) = '08' AND PRIORITY = 3")
data_col = []
for column in query.description:
	data_col.append(column[0])
dAlertsAgo3 = pd.DataFrame.from_records(data=query.fetchall(), columns=data_col)

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
valorMinJulio = 0
valorMaxJulio = 0
valorMinAgosto = 0
valorMaxAgosto = 0

if dAlertsJul1["sid"].count() < (dAlertsJul2["sid"].count()) & (dAlertsJul1["sid"].count() < (dAlertsJul3["sid"].count())):
    valorMinJulio = dAlertsJul1["sid"].count()
elif dAlertsJul2["sid"].count() < dAlertsJul3["sid"].count():
    valorMinJulio = dAlertsJul2["sid"].count()
else:
    valorMinJulio = dAlertsJul3["sid"].count()

if dAlertsJul1["sid"].count() > (dAlertsJul2["sid"].count()) & (dAlertsJul1["sid"].count() > (dAlertsJul3["sid"].count())):
    valorMaxJulio = dAlertsJul1["sid"].count()
elif dAlertsJul2["sid"].count() > dAlertsJul3["sid"].count():
    valorMaxJulio = dAlertsJul2["sid"].count()
else:
    valorMaxJulio = dAlertsJul3["sid"].count()

if dAlertsAgo1["sid"].count() < (dAlertsAgo2["sid"].count()) & (dAlertsAgo1["sid"].count() < (dAlertsAgo3["sid"].count())):
    valorMinAgosto = dAlertsAgo1["sid"].count()
elif dAlertsAgo2["sid"].count() < dAlertsAgo3["sid"].count():
    valorMinAgosto = dAlertsAgo2["sid"].count()
else:
    valorMinAgosto = dAlertsAgo3["sid"].count()

if dAlertsAgo1["sid"].count() > (dAlertsAgo2["sid"].count()) & (dAlertsAgo1["sid"].count() > (dAlertsAgo3["sid"].count())):
    valorMaxAgosto = dAlertsAgo1["sid"].count()
elif dAlertsAgo2["sid"].count() > dAlertsAgo3["sid"].count():
    valorMaxAgosto = dAlertsAgo2["sid"].count()
else:
    valorMaxAgosto = dAlertsAgo3["sid"].count()

valorMin1 = 0
valorMax1 = 0
valorMin2 = 0
valorMax2 = 0
valorMin3 = 0
valorMax3 = 0

mesMin1 = ""
mesMax1 = ""
mesMin2 = ""
mesMax2 = ""
mesMin3 = ""
mesMax3 = ""

if dAlertsJul1["sid"].count() < dAlertsAgo1["sid"].count():
    valorMin1 = dAlertsJul1["sid"].count()
    valorMax1 = dAlertsAgo1["sid"].count()
    mesMin1 = "julio"
    mesMax1 = "agosto"
else:
    valorMin1 = dAlertsAgo1["sid"].count()
    valorMax1 = dAlertsJul1["sid"].count()
    mesMin1 = "agosto"
    mesMax1 = "julio"

if dAlertsJul2["sid"].count() < dAlertsAgo2["sid"].count():
    valorMin2 = dAlertsJul2["sid"].count()
    valorMax2 = dAlertsAgo2["sid"].count()
    mesMin2 = "julio"
    mesMax2 = "agosto"
else:
    valorMin2 = dAlertsAgo2["sid"].count()
    valorMax2 = dAlertsJul2["sid"].count()
    mesMin2 = "agosto"
    mesMax2 = "julio"

if dAlertsJul3["sid"].count() < dAlertsAgo3["sid"].count():
    valorMin3 = dAlertsJul3["sid"].count()
    valorMax3 = dAlertsAgo3["sid"].count()
    mesMin3 = "julio"
    mesMax3 = "agosto"
else:
    valorMin3 = dAlertsAgo3["sid"].count()
    valorMax3 = dAlertsJul3["sid"].count()
    mesMin3 = "agosto"
    mesMax3 = "julio"

valorMinTotal = 0
valorMaxTotal = 0

mesMinTotal = ""
mesMaxTotal = ""

if dAlertsJul1["sid"].count()+dAlertsJul2["sid"].count()+dAlertsJul3["sid"].count() < dAlertsAgo1["sid"].count()+dAlertsAgo2["sid"].count()+dAlertsAgo3["sid"].count():
    valorMinTotal = dAlertsJul1["sid"].count()+dAlertsJul2["sid"].count()+dAlertsJul3["sid"].count()
    valorMaxTotal = dAlertsAgo1["sid"].count() + dAlertsAgo2["sid"].count() + dAlertsAgo3["sid"].count()
    mesMinTotal = "julio"
    mesMaxTotal = "agosto"
else:
    valorMaxTotal = dAlertsJul1["sid"].count() + dAlertsJul2["sid"].count() + dAlertsJul3["sid"].count()
    valorMinTotal = dAlertsAgo1["sid"].count() + dAlertsAgo2["sid"].count() + dAlertsAgo3["sid"].count()
    mesMinTotal = "agosto"
    mesMaxTotal = "julio"

print("El valor mínimo de alertas de prioridad 1 ha sido de", valorMin1, ", en el mes de", mesMin1)
print("El valor máximo de alertas de prioridad 1 ha sido de", valorMax1, ", en el mes de", mesMax1)
print("El valor mínimo de alertas de prioridad 2 ha sido de", valorMin2, ", en el mes de", mesMin2)
print("El valor máximo de alertas de prioridad 2 ha sido de", valorMax2, ", en el mes de", mesMax2)
print("El valor mínimo de alertas de prioridad 3 ha sido de", valorMin3, ", en el mes de", mesMin3)
print("El valor máximo de alertas de prioridad 3 ha sido de", valorMax3, ", en el mes de", mesMax3)

print("El valor mínimo de alertas totales ha sido de", valorMinTotal, ", en el mes de", mesMinTotal)
print("El valor máximo de alertas totales ha sido de", valorMaxTotal, ", en el mes de", mesMaxTotal)


con.close()
