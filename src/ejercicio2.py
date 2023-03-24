# EJERCICIO 2
import sqlite3

import pandas as pd
import json


con = sqlite3.connect('data/practica1.db')
cur = con.cursor()
# cur.execute("CREATE TABLE maquinas("
#             "id text,"
#             "ip text,"
#             "servicios text,"
#             "puertos_abiertos text,"
#             "servicios_inseguros integer,"
#             "vulnerabilidades_detectadas integer"
#             ")"
#             )

# INSERT test
# cur.execute("INSERT INTO maquinas VALUES ('web', '172.18.0.0', 'none', 'admin', 656445552, 'Administracion de sistemas', '[\"80/TCP\", \"443/TCP\", \"3306/TCP\", \"40000/UDP\"]', 3, 0, 15), ('paco_pc', '172.17.0.0', 'Barcelona', 'Paco Garcia', 640220120, 'Direccion', 'None', 0, 0, 4), ('luis_pc', '172.19.0.0', 'Madrid', 'Luis Sanchez', 'None', 'Desarrollador', '[\"1194/UDP\", \"8080/TCP\", \"8080/UDP\", \"40000/UDP\"]', 1, 1, 52), ('router1', '172.1.0.0', 'None', 'admin', 656445552, 'Administracion de sistemas', '[\"443/UDP\", \"80/TCP\"]', 1, 0, 3), ('dhcp_server', '172.1.0.1', 'Madrid', 'admin', 'None', 'None', '[\"80/TCP\", \"67/UDP\", \"68/UDP\"]', 2, 2, 12), ('mysql_db', '172.18.0.1', 'None', 'admin', 656445552, 'Administracion de sistemas', '[\"8080/TCP\", \"3306/TCP\", \"3306/UDP\"]', 2, 0, 2), ('ELK', '172.18.0.2', 'None', 'admin', 656445552, 'Administracion de sistemas', '[\"80/TCP\", \"443/TCP\", \"9200/TCP\", \"9300/TCP\", \"5601/TCP\"]', 3, 2, 21)")

# Open the devices info as JSON and transform into SQL
with open("data/devices.json") as f:
    data = json.load(f)
    df = pd.DataFrame(data)

    # Extract analisis and puertos, and create an extended dataframe
    analisis = []
    ports = []

    for i in range(df.__len__()):
        line = df['analisis'][i]
        line['id'] = df['id'][i]
        port = {'id': line['id'], 'puertos': line.pop('puertos_abiertos')}
        if port['puertos'] == "None":
            port['puertos'] = ["None"]

        ports.append(pd.DataFrame(port))
        analisis.append(pd.Series(line))

    analisis = pd.concat(analisis, axis=1).transpose()
    ports = pd.concat(ports, ignore_index=True)

    # Extract responsable and create usuarios dataframe
    usuarios = []
    nombres = []
    for i in range(df.__len__()):
        line = df['responsable'][i]
        line['id'] = df['id'][i]
        nombres.append(line['nombre'])  # Save names to craft maquinas dataframe
        if line['nombre'] not in [s['nombre'] for s in usuarios]:
            usuarios.append(pd.Series(line))  # Save unique usuarios

    nombres = pd.Series(nombres)
    usuarios = pd.concat(usuarios, axis=1).transpose()

    # Drop 'analisis' column, replace 'responsable' with 'nombres' array and merge with 'analisis' dataframe
    maquinas = df.drop(columns='analisis')
    maquinas['responsable'] = nombres

    # Save ports and final df to database
    ports.to_sql('puertos', con, if_exists='replace')       # 'Replace' avoids failure when filling the table
    usuarios.to_sql('usuarios', con, if_exists='replace')
    maquinas.to_sql('maquinas', con, if_exists='replace')
    analisis.to_sql('analisis', con, if_exists='replace')

# Extract transform CSV into alertas table
with open("data/alerts.csv") as f:
    pd.read_csv(f).to_sql('alertas', con, if_exists='replace')

# TEST the database
# cur.execute("SELECT * FROM usuarios JOIN maquinas ON maquinas.responsable=usuarios.nombre ")

# Commit changes and close
con.commit()
con.close()

############
# ANALYSIS #
############

# Use df, analisis, responsable dataframes to analyze data



