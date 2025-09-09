# cargar_datos_demo.py
import sqlite3
from motor_telefonia import TelefoniaProcessor  # <-- usamos esto para crear el esquema

def cargar_datos_demo(db_path="telefonia.db"):

    boot = TelefoniaProcessor(db_path)
    boot.conn.close() 

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # 1) Limpieza segura 
    for t in (
        "TRAFICO","LOCAL","TELINTER","TRAF_MES","INTERDAT",
        "TARIFAS","TELTARIF","SERVICIO","TRAS_1","TRAF_TRA","RDSI_1","TRAFRDSI"
    ):
        cur.execute(f"DELETE FROM {t};")
    con.commit()

    # 2) TARIFAS (3 planes)
    cur.executemany("""
    INSERT INTO TARIFAS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        (1, 60, 'MINUTO', 200, 16.00, 0.20, 0.13, 0.07, 0.07, 0.60, 1.00, 0.51, 0.34, 0.17, 2.00, 1.55,
         0.30, 0.30, 0.30, 0.60, 0.60, 0.60),
        (38,60, 'MINUTO', 100, 12.00, 0.18, 0.12, 0.06, 0.07, 0.55, 0.90, 0.50, 0.32, 0.16, 2.00, 1.45,
         0.28, 0.28, 0.28, 0.55, 0.55, 0.55),
        (39,60, 'MINUTO', 150, 14.00, 0.19, 0.125,0.065,0.07, 0.58, 0.95, 0.505,0.33, 0.165,2.00, 1.50,
         0.29, 0.29, 0.29, 0.58, 0.58, 0.58),
    ])
    con.commit()

    # 3) SERVICIO (proveedores para tráfico de internet)
    cur.executemany("INSERT INTO SERVICIO(ID_SERV,NOMBRE,TELSER) VALUES (?,?,?)", [
        (96,'INTCOTEL_RURAL_LIB', 1234567),
        (97,'INTCOTEL_RURAL_PLA', 7654321),
        (50,'INTERNET_STD',       2212345),
    ])
    con.commit()

    # 4) TELTARIF (asignación de planes)
    cur.executemany("INSERT INTO TELTARIF(TELEFONO,TARIFA) VALUES(?,?)", [
        (1234567, 1),
        (2233445, 38),
        (7654321, 39),
    ])
    con.commit()

    # 5) TRÁFICO de prueba (YYMMDDHHMMSS)
    cur.executemany("""
    INSERT INTO TRAFICO(TELEFONO,FECHA,DURACION,DESTINO) VALUES (?,?,?,?)
    """, [
        # Tel 1234567 (plan 1)
        (1234567,'250701080000',120,'2212345'),       # local NOR + INTERNET_STD
        (1234567,'250701230500',90,'2256789'),        # local RED
        (1234567,'250701030000',180,'2261111'),       # local SRE
        (1234567,'250701120000',60,'1070000'),        # 107 -> 1 min fijo
        (1234567,'250701121500',60,'1040000'),        # 104 -> CCC
        (1234567,'250701100000',120,'2105000'),       # público NOR
        (1234567,'250701010000',60,'2896000'),        # rural SRE
        (1234567,'250701200000',60,'50350000'),       # VOIP directo (IPN)
        (1234567,'250701201500',60,'50900000'),       # VOIP operador (IPN)

        # Tel 2233445 (plan 38) – INTCOTEL_RUR_LIB
        (2233445,'250701090000',300,'1234567'),       # INTERNET especial (prov 96)
        (2233445,'250701220000',60,'70012345'),       # móvil RED
        (2233445,'250701100000',60,'0012345'),        # internacional NOR (LIN)

        # Tel 7654321 (plan 39) – INTCOTEL_RUR_PLA
        (7654321,'250701093000',240,'7654321'),       # INTERNET especial (prov 97)
        (7654321,'250701110000',60,'0712345'),        # nacional (LNN)
        (7654321,'250701210000',60,'2219999'),        # local NOR
        (7654321,'250701230000',60,'2219999'),        # local RED
        (7654321,'250701030000',60,'2219999'),        # local SRE
        (7654321,'250701120000',60,'900161616'),      # valor agregado
        (7654321,'250701101500',180,'2310000'),       # correo de voz NOR
    ])
    con.commit()
    con.commit()
    con.close()
