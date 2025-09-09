# admin_config.py
import sqlite3

VARIABlES_EDITABLES = [
    "REDONDEO","AUX_NOR","AUX_RED","AUX_SRED","AUX_TAR","AUX_INTERNET","AUX_INTCOTEL",
    "AUX_INTCOTEL_RUR_LIB","AUX_INTCOTEL_RUR_PLA","AUX_RUR_MOV","AUX_RURAL","AUX_800_MOV_PUB",
    "AUX_TEL_PUB_RED","AUX_TEL_PUB_SRED","AUX_VAG","AUX_CPP",
    "AUX_COS_IP_NO","AUX_COS_IP_RE","AUX_COS_IP_SR","AUX_COS_IPO_NO","AUX_COS_IPO_RE","AUX_COS_IPO_SR"
]

def set_config(db_path="telefonia.db", **kwargs):
    """
    Guarda overrides en CONFIG. Sólo claves válidas de VARIABlES_EDITABLES.
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    for k,v in kwargs.items():
        if k not in VARIABlES_EDITABLES:
            continue
        cur.execute("INSERT INTO CONFIG(CLAVE,VALOR) VALUES(?,?) ON CONFLICT(CLAVE) DO UPDATE SET VALOR=excluded.VALOR", (k, float(v)))
    con.commit()
    con.close()

def get_config(db_path="telefonia.db"):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT CLAVE, VALOR FROM CONFIG ORDER BY CLAVE")
    rows = cur.fetchall()
    con.close()
    return dict(rows)

def menu_config(db_path="telefonia.db"):
    """
    Mini UI por consola para ver/editar variables.
    """
    while True:
        print("\n=== Configuración de variables (AUX_*, REDONDEO) ===")
        cfg = get_config(db_path)
        for k in VARIABlES_EDITABLES:
            val = cfg.get(k, "(defecto)")
            print(f"{k:20s} = {val}")
        print("\n1) Modificar")
        print("2) Salir")
        op = input("Opción: ").strip()
        if op == "1":
            k = input("Variable (nombre exacto): ").strip()
            if k in VARIABlES_EDITABLES:
                v = float(input(f"Nuevo valor para {k}: ").strip())
                set_config(db_path, **{k: v})
                print("Guardado. Al volver a procesar, se aplicará.")
            else:
                print("Variable no editable o nombre inválido.")
        elif op == "2":
            break
        else:
            print("Opción inválida.")
