# EJERCICIO 2
import sqlite3
con = sqlite3.connect('practica1.db')
cur = con.cursor()
cur.execute("CREATE TABLE maquinas (id text, ip text, localizacion text, responsablenombre text, responsabletelefono integer, responsablerol text, analisisservicios text, analisispuertosabiertos integer, analisisserviciosinseguros integer, analisisvulnerabilidades integer)")
con.commit()
con.close()
