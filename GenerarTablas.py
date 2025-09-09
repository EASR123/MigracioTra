# GenerarTablas.py
import sqlite3
import random
from datetime import datetime
from motor_telefonia import TelefoniaProcessor

DB_PATH = "telefonia.db"
RANDOM_SEED = 42

def bootstrap_schema(db_path=DB_PATH):
    """Crea el esquema completo llamando al procesador (incluye CONFIG si tu motor_telefonia.py la define)."""
    p = TelefoniaProcessor(db_path)
    p.conn.close()

def ensure_config_schema(con):
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='CONFIG'")
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute("CREATE TABLE CONFIG (CLAVE TEXT PRIMARY KEY, VALOR_NUM REAL)")
        con.commit()
        return
    cur.execute("PRAGMA table_info(CONFIG)")
    cols = [r[1].upper() for r in cur.fetchall()]  
    if "VALOR_NUM" not in cols:
        cur.execute("ALTER TABLE CONFIG ADD COLUMN VALOR_NUM REAL")
        con.commit()

def limpiar_todas(con):
    """Limpia TODAS las tablas del proyecto para una carga fresca."""
    cur = con.cursor()
    tablas = [
        "TRAFICO","LOCAL","TELINTER","TRAF_MES","INTERDAT",
        "TARIFAS","TELTARIF","SERVICIO","TRAS_1","TRAF_TRA","RDSI_1","TRAFRDSI","CONFIG"
    ]
    for t in tablas:
        cur.execute(f"DELETE FROM {t};")
    con.commit()

def poblar_config(con):
    """Carga valores en CONFIG (variables alterables en el tiempo)."""
    ensure_config_schema(con)  
    cur = con.cursor()
    pares = {
        "REDONDEO": 1,
        "AUX_NOR": 0.20,
        "AUX_RED": 0.13,
        "AUX_SRED": 0.07,
        "AUX_TAR": 16.00,
        "AUX_INTERNET": 0.07,
        "AUX_INTCOTEL": 0.12,
        "AUX_INTCOTEL_RUR_LIB": 0.20,
        "AUX_INTCOTEL_RUR_PLA": 0.15,
        "AUX_RUR_MOV": 1.00,
        "AUX_RURAL": 0.60,
        "AUX_800_MOV_PUB": 0.51,
        "AUX_TEL_PUB_RED": 0.34,
        "AUX_TEL_PUB_SRED": 0.17,
        "AUX_VAG": 2.00,
        "AUX_CPP": 1.55,
        "AUX_COS_IP_NO": 0.30,
        "AUX_COS_IP_RE": 0.30,
        "AUX_COS_IP_SR": 0.30,
        "AUX_COS_IPO_NO": 0.60,
        "AUX_COS_IPO_RE": 0.60,
        "AUX_COS_IPO_SR": 0.60,
    }
    for k, v in pares.items():
        cur.execute("""
            INSERT INTO CONFIG(CLAVE, VALOR_NUM) VALUES(?,?)
            ON CONFLICT(CLAVE) DO UPDATE SET VALOR_NUM=excluded.VALOR_NUM
        """, (k, float(v)))
    con.commit()

def poblar_tarifas(con):
    """Inserta varios planes de tarifas."""
    cur = con.cursor()
    cur.executemany("""
        INSERT INTO TARIFAS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        # TARIFA,MINIMO,UNIDAD,MIN_LIBRES,COS_TARIFA,COS_NORMAL,COS_REDUCI,COS_SRED,
        # COS_INTER,COS_RURAL,COS_1,COS_3,COS_TP_RED,COS_TP_SRE,COS_4,COS_CPP,
        # COS_IP_NO,COS_IP_RE,COS_IP_SR,COS_IPO_NO,COS_IPO_RE,COS_IPO_SR
        (1, 60, 'MINUTO', 200, 16.00, 0.20, 0.13, 0.07, 0.07, 0.60, 1.00, 0.51, 0.34, 0.17, 2.00, 1.55,
         0.30, 0.30, 0.30, 0.60, 0.60, 0.60),
        (38,60, 'MINUTO', 100, 12.00, 0.18, 0.12, 0.06, 0.07, 0.55, 0.90, 0.50, 0.32, 0.16, 2.00, 1.45,
         0.28, 0.28, 0.28, 0.55, 0.55, 0.55),
        (39,60, 'MINUTO', 150, 14.00, 0.19, 0.125,0.065,0.07, 0.58, 0.95, 0.505,0.33, 0.165,2.00, 1.50,
         0.29, 0.29, 0.29, 0.58, 0.58, 0.58),
        (55,60, 'LLAMADA', 0, 10.00, 0.22, 0.14, 0.08, 0.07, 0.62, 1.05, 0.52, 0.35, 0.18, 2.00, 1.60,
         0.31, 0.31, 0.31, 0.62, 0.62, 0.62),
    ])
    con.commit()

def poblar_servicios(con):
    """Servicios de internet/adm/tec"""
    cur = con.cursor()
    cur.executemany("INSERT INTO SERVICIO(ID_SERV,NOMBRE,TELSER) VALUES (?,?,?)", [
        (96, "INTCOTEL_RURAL_LIB", 1234567),
        (97, "INTCOTEL_RURAL_PLA", 7654321),
        (50, "INTERNET_STD",       2212345),
        (51, "INTERNET_EMPRESAS",  2299999),
    ])
    con.commit()

def poblar_teltarif(con):
    """Asigna teléfonos a distintos planes."""
    cur = con.cursor()
    numeros = [
        (1234567, 1),
        (2233445, 38),
        (7654321, 39),
        (2288001, 55),
        (2288002, 1),
        (2288003, 38),
        (2288004, 39),
    ]
    cur.executemany("INSERT INTO TELTARIF(TELEFONO,TARIFA) VALUES (?,?)", numeros)
    con.commit()
    return [n[0] for n in numeros]

def _hora(h, m, s): return f"{h:02d}{m:02d}{s:02d}"

def poblar_trafico(con, telefonos, fechas=("250701","250702","250703")):

    cur = con.cursor()
    rows = []
    random.seed(RANDOM_SEED)

    for tel in telefonos:
        for yymmdd in fechas:
            # Local NOR/RED/SRE
            rows += [
                (tel, f"{yymmdd}{_hora(8,  0, 0)}", random.choice([60,120,180]), "2210000"),  # NOR
                (tel, f"{yymmdd}{_hora(23, 5, 0)}", random.choice([60,90,120]),  "2256789"),  # RED
                (tel, f"{yymmdd}{_hora(3,  0, 0)}", random.choice([60,120,180]), "2261111"),  # SRE
            ]
            # Internet (SERVICIO 50 / 51)
            rows.append((tel, f"{yymmdd}{_hora(9, 0, 0)}",  random.choice([60,120,240,300]), "2212345"))  # INTERNET_STD
            rows.append((tel, f"{yymmdd}{_hora(9,30, 0)}",  random.choice([60,120,240,300]), "2299999"))  # INTERNET_EMPRESAS

            # Especiales / públicos / rural
            rows.append((tel, f"{yymmdd}{_hora(12,0, 0)}", 60, "1070000"))   # 107 -> 1 min fijo
            rows.append((tel, f"{yymmdd}{_hora(12,15,0)}", 60, "1040000"))   # 104 -> CCC
            rows.append((tel, f"{yymmdd}{_hora(10,0, 0)}", 120, "2105000"))  # público NOR
            rows.append((tel, f"{yymmdd}{_hora(1, 0, 0)}",  60, "2896000"))  # rural SRE

            # IP directo (50 32~39), IP operador (50 90~99)
            rows.append((tel, f"{yymmdd}{_hora(20,0,0)}", 60, "50350000"))   # VOIP directo
            rows.append((tel, f"{yymmdd}{_hora(20,15,0)}",60, "50900000"))   # VOIP operador

            # Celular / LDN / LDI
            rows.append((tel, f"{yymmdd}{_hora(10,30,0)}",60, "70123456"))   # móvil
            rows.append((tel, f"{yymmdd}{_hora(11,0, 0)}", 60, "0712345"))   # nacional (0 + no 0)
            rows.append((tel, f"{yymmdd}{_hora(11,30,0)}",60, "0012345"))    # internacional (00...)

            # Valor agregado
            rows.append((tel, f"{yymmdd}{_hora(12,45,0)}",60, "900161616"))

    cur.executemany("""
        INSERT INTO TRAFICO(TELEFONO,FECHA,DURACION,DESTINO) VALUES (?,?,?,?)
    """, rows)
    con.commit()

def poblar_telinter(con, telefonos):
    """Inserta registros en TELINTER (pruebas de reportes)."""
    cur = con.cursor()
    rows = []
    for tel in telefonos:
        rows += [
            (tel, 300, 50, 3.50),   # INTERNET_STD
            (tel, 240, 51, 3.00),   # INTERNET_EMPRESAS
            (tel, 120, 96, 2.40),   # INTCOTEL_RURAL_LIB
        ]
    cur.executemany("""
        INSERT INTO TELINTER(TELEFONO,DURACION,PROV,COSTO) VALUES (?,?,?,?)
    """, rows)
    con.commit()

def poblar_traslados(con, telefonos):
    """TRAS_1/TRAF_TRA y RDSI_1/TRAFRDSI con algunos ejemplares."""
    cur = con.cursor()
    if len(telefonos) < 2:
        return
    t1, t2 = telefonos[0], telefonos[1]

    cur.execute("INSERT INTO TRAS_1(TELEFONO) VALUES (?)", (t1,))
    cur.executemany("""
        INSERT INTO TRAF_TRA(TELEFONO,FECHA,DURACION,DESTINO) VALUES (?,?,?,?)
    """, [
        (t1, "250701100000", 60, "2219999"),
        (t1, "250702230000", 90, "2250000"),
    ])

    cur.execute("INSERT INTO RDSI_1(TELEFONO) VALUES (?)", (t2,))
    cur.executemany("""
        INSERT INTO TRAFRDSI(TELEFONO,FECHA,DURACION,DESTINO) VALUES (?,?,?,?)
    """, [
        (t2, "250701080000", 120, "2211000"),
        (t2, "250703030000", 180, "2262222"),
    ])
    con.commit()

def poblar_interdat(con):
    """Algunos no procesados artificiales."""
    cur = con.cursor()
    cur.executemany("""
        INSERT INTO INTERDAT(TELEFONO,FECHA,DURACION,DESTINO,CONVENIO)
        VALUES (?,?,?,?,?)
    """, [
        (9999999, "250701120000", 60, "9999999", "NOPROCESADO"),
        (9999998, "250701121500", 90, "ABCDEF",  "NOFACT"),
    ])
    con.commit()

def procesar_para_generar_traf_mes_y_local(db_path=DB_PATH):
    """Corre el motor para llenar TRAF_MES y LOCAL a partir de TRAFICO + soporte."""
    p = TelefoniaProcessor(db_path)
    p.process()

def main():
    print("== Generador de datos para TODAS las tablas ==")
    print("Inicio:", datetime.now())

    # 1) Asegura esquema base (crea tablas si faltan)
    bootstrap_schema(DB_PATH)

    con = sqlite3.connect(DB_PATH)

    # 2) Migración/aseguramiento de CONFIG (evita 'no column named VALOR_NUM')
    ensure_config_schema(con)

    # 3) Limpieza total
    limpiar_todas(con)

    # 4) Poblar CONFIG y catálogos
    poblar_config(con)
    poblar_tarifas(con)
    poblar_servicios(con)

    # 5) Teléfonos y planes
    telefonos = poblar_teltarif(con)

    # 6) Tráfico crudo + otros (TELINTER/traslados/INTERDAT)
    poblar_trafico(con, telefonos, fechas=("250701","250702","250703"))
    poblar_telinter(con, telefonos)
    poblar_traslados(con, telefonos)
    poblar_interdat(con)

    con.close()

    # 7) Procesar para generar TRAF_MES y LOCAL
    procesar_para_generar_traf_mes_y_local(DB_PATH)

    print("Fin:", datetime.now())
    print("Listo. Base de datos completa con datos en todas las tablas.")

if __name__ == "__main__":
    main()
