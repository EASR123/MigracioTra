    # utils_trafico.py
import sqlite3

def agregar_trafico_para_fecha(db_path="telefonia.db", yymmdd="250702"):
    """
    Inserta tráfico sintético para la fecha dada (prefijo YYMMDD).
    Crea varias llamadas locales, móviles, públicas, rurales, VOIP y especiales.
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Teléfonos existentes en TELTARIF
    tels = [r[0] for r in cur.execute("SELECT TELEFONO FROM TELTARIF").fetchall()]
    if not tels:
        con.close()
        raise RuntimeError("No hay teléfonos en TELTARIF. Carga datos demo primero.")

    rows = []
    for tel in tels:
        # NOR, RED, SRE
        rows += [
            (tel, f"{yymmdd}080000", 120, "2210000"),   # local NOR
            (tel, f"{yymmdd}230000",  90, "2250000"),   # local RED
            (tel, f"{yymmdd}030000", 180, "2260000"),   # local SRE
            # celular
            (tel, f"{yymmdd}100000",  60, "70123456"),
            # público
            (tel, f"{yymmdd}120000", 120, "2105000"),
            # rural
            (tel, f"{yymmdd}010000",  60, "2896000"),
            # VOIP directo y operador
            (tel, f"{yymmdd}200000",  60, "50350000"),
            (tel, f"{yymmdd}201500",  60, "50900000"),
            # 104 y 107
            (tel, f"{yymmdd}121500",  60, "1040000"),
            (tel, f"{yymmdd}120000",  60, "1070000"),
        ]
    cur.executemany("""
        INSERT INTO TRAFICO(TELEFONO,FECHA,DURACION,DESTINO)
        VALUES (?,?,?,?)
    """, rows)
    con.commit()
    con.close()
    print(f"Se insertaron {len(rows)} registros de tráfico para la fecha {yymmdd}.")
