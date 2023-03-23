# EJERCICIO 2
import sqlite3
import pandas as pd
import json

con = sqlite3.connect('src/data/practica1.db')
cur = con.cursor()
# cur.execute("CREATE TABLE maquinas ("
#             "id text,"
#             "ip text,"
#             "localizacion text,"
#             "responsablenombre text,"
#             "responsabletelefono integer,"
#             "responsablerol text,"
#             "analisisservicios text,"
#             "analisispuertosabiertos integer,"
#             "analisisserviciosinseguros integer,"
#             "analisisvulnerabilidades integer"
#             ")"
#             )

# Open the devices info as JSON and transform into SQL
with open("src/data/devices.json") as f:
    data = json.load(f)
    df = pd.DataFrame(data)

    #Extract analisis and create a extended dataframe
    analisis = []
    for i in range(df.__len__()):
        line = df['analisis'][i]
        line['id'] = df['id'][i]
        if line['puertos_abiertos'] == 'None':
            line['puertos_abiertos'] = ['None']
        analisis.append(pd.DataFrame(line))
    analisis = pd.concat(analisis)
    df.join(analisis)
    print(df)

