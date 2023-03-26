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


#Número de alertas en el tiempo:
conn = sqlite3.connect('pruebapractica1csv.db')

# Consulta SQL para contar el número de alertas por día
query = '''
        SELECT date(timestamp) as date, count(*) as num_alerts
        FROM alertas
        GROUP BY date(timestamp)
        ORDER BY date(timestamp)
        '''

# Ejecutar la consulta y guardar los resultados en un DataFrame
df = pd.read_sql_query(query, conn)

conn.close()

# Convertir la columna de fecha en un objeto datetime
df['date'] = pd.to_datetime(df['date'])

# Configurar el gráfico
plt.figure(figsize=(12, 6))
plt.plot(df['date'], df['num_alerts'])
plt.title('Número de alertas por día')
plt.xlabel('Fecha')
plt.ylabel('Número de alertas')

plt.show()
