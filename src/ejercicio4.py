# EJERCICIO 4
#Para seleccionar las IP de origen más problemáticas y representarlas:
import sqlite3
import matplotlib.pyplot as plt

con = sqlite3.connect('pruebapractica1csv.db')
cur = con.cursor()
cur.execute("SELECT origin, COUNT(*) FROM alertas WHERE priority = 1 GROUP BY origin ORDER BY COUNT(*) DESC LIMIT 10")

results = cur.fetchall()
con.close()

# Separación de los datos en dos listas para el gráfico de barras
ips = [result[0] for result in results]
counts = [result[1] for result in results]

# Creación del gráfico de barras
plt.bar(ips, counts)
plt.xlabel('IPs de origen')
plt.ylabel('Número de incidencias')
plt.title('Top 10 IPs de origen con mayor número de incidencias (prioridad = 1)')

plt.show()



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
