# EJERCICIO 2
import sqlite3
con = sqlite3.connect('practica1.db')
cur = con.cursor()
cur.execute("CREATE TABLE maquinas (id text, ip text, localizacion text, responsablenombre text, responsabletelefono integer, responsablerol text, analisisservicios text, analisispuertosabiertos integer, analisisserviciosinseguros integer, analisisvulnerabilidades integer)")
cur.execute("INSERT INTO maquinas VALUES ('web', '172.18.0.0', 'none', 'admin', 656445552, 'Administracion de sistemas', '[\"80/TCP\", \"443/TCP\", \"3306/TCP\", \"40000/UDP\"]', 3, 0, 15), ('paco_pc', '172.17.0.0', 'Barcelona', 'Paco Garcia', 640220120, 'Direccion', 'None', 0, 0, 4), ('luis_pc', '172.19.0.0', 'Madrid', 'Luis Sanchez', 'None', 'Desarrollador', '[\"1194/UDP\", \"8080/TCP\", \"8080/UDP\", \"40000/UDP\"]', 1, 1, 52), ('router1', '172.1.0.0', 'None', 'admin', 656445552, 'Administracion de sistemas', '[\"443/UDP\", \"80/TCP\"]', 1, 0, 3), ('dhcp_server', '172.1.0.1', 'Madrid', 'admin', 'None', 'None', '[\"80/TCP\", \"67/UDP\", \"68/UDP\"]', 2, 2, 12), ('mysql_db', '172.18.0.1', 'None', 'admin', 656445552, 'Administracion de sistemas', '[\"8080/TCP\", \"3306/TCP\", \"3306/UDP\"]', 2, 0, 2), ('ELK', '172.18.0.2', 'None', 'admin', 656445552, 'Administracion de sistemas', '[\"80/TCP\", \"443/TCP\", \"9200/TCP\", \"9300/TCP\", \"5601/TCP\"]', 3, 2, 21)")
con.commit()
con.close()
