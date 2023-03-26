# EJERCICIO 4
#Para seleccionar las IP de origen más problemáticas
import sqlite3

conn = sqlite3.connect('pruebapractica1csv.db')
cursor = conn.cursor()

query = "SELECT origin, COUNT(*) as count FROM alertas WHERE priority=1 GROUP BY origin ORDER BY count DESC LIMIT 10"
cursor.execute(query)

resultados = cursor.fetchall()

for resultado in resultados:
    print(resultado[0])

conn.close()
