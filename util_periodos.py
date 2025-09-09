# util_periodos.py
import sqlite3

def clonar_traf_mes(db_path, origen_yyMM, destino_yyMM):
    """
    Clona registros TRAF_MES de un periodo a otro,
    cambiando los 4 primeros dígitos de FECHA (yyMM).
    Útil sólo para pruebas de reportes.
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    # leer origen
    cur.execute("SELECT TELEFONO,FECHA,DURACION,REDONDEO,DESTINO,INTERNET,COS_TARIFA,TIPO,COSTO FROM TRAF_MES WHERE substr(FECHA,1,4)=?", (origen_yyMM,))
    rows = cur.fetchall()
    if not rows:
        print("No hay filas en TRAF_MES para el periodo origen.")
        con.close()
        return

    # insertar destino
    for r in rows:
        tel, f, dur, red, des, inte, costar, tpo, cost = r
        nueva_fecha = destino_yyMM + f[4:]  # copia ddHHMMSS
        cur.execute("""
            INSERT INTO TRAF_MES(TELEFONO,FECHA,DURACION,REDONDEO,DESTINO,INTERNET,COS_TARIFA,TIPO,COSTO)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (tel, nueva_fecha, dur, red, des, inte, costar, tpo, cost))
    con.commit()
    con.close()
    print(f"Clonadas {len(rows)} filas de {origen_yyMM} a {destino_yyMM}.")
