# graficas_telefonia.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def _query(con, sql, params=None):
    return pd.read_sql_query(sql, con, params=params or [])

def grafico_traf_mes(db_path="telefonia.db", fecha_like=None):

    con = sqlite3.connect(db_path)

    where = ""
    params = []
    if fecha_like:
        where = "WHERE FECHA LIKE ?"
        params = [f"{fecha_like}%"]

    res = _query(con, f"""
        SELECT
          TIPO,
          SUM(REDONDEO) AS segundos,
          SUM(COSTO)    AS costo,
          MIN(FECHA)    AS fecha_min,
          MAX(FECHA)    AS fecha_max
        FROM TRAF_MES
        {where}
        GROUP BY TIPO
        ORDER BY costo DESC
    """, params)

    con.close()

    if res.empty:
        print("No hay datos en TRAF_MES para ese filtro de fecha." if fecha_like else "No hay datos en TRAF_MES.")
        return

    fig, ax = plt.subplots()
    x = list(range(len(res)))
    ax.bar(x, res["costo"])

    ax.set_xticks(x)
    ax.set_xticklabels(res["TIPO"], rotation=45, ha="right")

    fecha_info = f" - FECHA {fecha_like}" if fecha_like else ""
    rango = f"{res['fecha_min'].min()} → {res['fecha_max'].max()}"
    ax.set_title(f"Costo por tipo de tráfico (TRAF_MES){fecha_info}\n{rango}")

    ax.set_xlabel("TIPO")
    ax.set_ylabel("Costo (Bs)")

    for i, v in enumerate(res["costo"]):
        ax.text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.show()

def grafico_conteo_por_tabla(db_path="telefonia.db"):

    con = sqlite3.connect(db_path)
    tablas = [
        "TRAFICO","LOCAL","TELINTER","TRAF_MES","INTERDAT",
        "TARIFAS","TELTARIF","SERVICIO","TRAS_1","TRAF_TRA","RDSI_1","TRAFRDSI","CONFIG"
    ]

    conteos = []
    for t in tablas:
        try:
            df = _query(con, f"SELECT COUNT(*) AS n FROM {t}")
            conteos.append((t, int(df["n"].iloc[0])))
        except Exception:
            conteos.append((t, 0))
    con.close()

    names = [t for t, _ in conteos]
    vals  = [n for _, n in conteos]
    x = list(range(len(names)))

    fig, ax = plt.subplots()
    ax.bar(x, vals)

    ax.set_xticks(x)
    ax.set_xticklabels(names, rotation=60, ha="right")

    ax.set_title("Cantidad de registros por tabla (SELECT/USE)")
    ax.set_xlabel("Tabla")
    ax.set_ylabel("Registros")

    for i, v in enumerate(vals):
        ax.text(i, v, str(v), ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.show()

def exportar_reporte_excel(db_path="telefonia.db", fecha_like=None, out_path="reporte_trafico.xlsx"):

    con = sqlite3.connect(db_path)

    where = ""
    params = []
    if fecha_like:
        where = "WHERE FECHA LIKE ?"
        params = [f"{fecha_like}%"]

    df_detalle = _query(con, f"""
        SELECT *
        FROM TRAF_MES
        {where}
        ORDER BY TELEFONO, FECHA
    """, params)

    df_res_tipo = _query(con, f"""
        SELECT
          TIPO,
          COUNT(*)                 AS llamadas,
          SUM(REDONDEO)/60.0       AS minutos,
          SUM(COSTO)               AS costo
        FROM TRAF_MES
        {where}
        GROUP BY TIPO
        ORDER BY costo DESC
    """, params)

    df_res_tel = _query(con, f"""
        SELECT
          TELEFONO,
          COUNT(*)                 AS llamadas,
          SUM(REDONDEO)/60.0       AS minutos,
          SUM(COSTO)               AS costo
        FROM TRAF_MES
        {where}
        GROUP BY TELEFONO
        ORDER BY costo DESC
    """, params)

    con.close()

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df_detalle.to_excel(xw, index=False, sheet_name="Detalle")
        df_res_tipo.to_excel(xw, index=False, sheet_name="ResumenPorTipo")
        df_res_tel.to_excel(xw, index=False, sheet_name="ResumenPorTelefono")

    print(f"Reporte exportado a: {out_path}")

def grafico_traf_mes_comparar(db_path="telefonia.db", fecha_a="250701", fecha_b="250702"):
    """
    Compara costos por TIPO entre dos prefijos de FECHA (ej.: '250701' vs '250702').
    Side-by-side bars por TIPO (unión de tipos de ambas fechas).
    """
    con = sqlite3.connect(db_path)

    def agg(fecha_like):
        return _query(con, """
            SELECT TIPO, SUM(COSTO) AS costo
            FROM TRAF_MES
            WHERE FECHA LIKE ?
            GROUP BY TIPO
        """, [f"{fecha_like}%"]).set_index("TIPO")["costo"]

    sA = agg(fecha_a)
    sB = agg(fecha_b)
    con.close()

    import numpy as np
    tipos = sorted(set(sA.index).union(set(sB.index)))
    valsA = [float(sA.get(t, 0.0)) for t in tipos]
    valsB = [float(sB.get(t, 0.0)) for t in tipos]

    x = np.arange(len(tipos))
    width = 0.4

    fig, ax = plt.subplots()
    ax.bar(x - width/2, valsA, width, label=f"{fecha_a}")
    ax.bar(x + width/2, valsB, width, label=f"{fecha_b}")

    ax.set_xticks(x)
    ax.set_xticklabels(tipos, rotation=45, ha="right")
    ax.set_title(f"Comparativo de costo por TIPO: {fecha_a} vs {fecha_b}")
    ax.set_xlabel("TIPO")
    ax.set_ylabel("Costo (Bs)")
    ax.legend()

    for i, v in enumerate(valsA):
        ax.text(x[i]-width/2, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    for i, v in enumerate(valsB):
        ax.text(x[i]+width/2, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.show()

# =======================
# NUEVO: REPORTE POR TEL
# =======================

def resumen_costo_por_telefono(db_path="telefonia.db", fecha_like=None):
    """
    Devuelve un DataFrame con costo total por TELEFONO (y #llamadas y minutos),
    opcionalmente filtrado por FECHA (prefijo YYMM o YYMMDD).
    """
    con = sqlite3.connect(db_path)
    where = ""
    params = []
    if fecha_like:
        where = "WHERE FECHA LIKE ?"
        params = [f"{fecha_like}%"]

    df = _query(con, f"""
        SELECT
          TELEFONO,
          COUNT(*)                 AS llamadas,
          ROUND(SUM(REDONDEO)/60.0, 2) AS minutos,
          ROUND(SUM(COSTO), 2)    AS costo
        FROM TRAF_MES
        {where}
        GROUP BY TELEFONO
        ORDER BY costo DESC
    """, params)
    con.close()
    return df

def grafico_costo_por_telefono(db_path="telefonia.db", fecha_like=None, top=15):
    """
    Grafica Top-N teléfonos por costo total (usa TRAF_MES).
    """
    df = resumen_costo_por_telefono(db_path, fecha_like)
    if df.empty:
        print("No hay datos para ese filtro.")
        return
    df_top = df.head(int(top))

    fig, ax = plt.subplots()
    x = list(range(len(df_top)))
    ax.bar(x, df_top["costo"])

    ax.set_xticks(x)
    ax.set_xticklabels(df_top["TELEFONO"].astype(str), rotation=45, ha="right")
    titulo = "Top teléfonos por costo"
    if fecha_like:
        titulo += f" (FECHA {fecha_like})"
    ax.set_title(titulo)
    ax.set_xlabel("TELEFONO")
    ax.set_ylabel("Costo (Bs)")

    for i, v in enumerate(df_top["costo"]):
        ax.text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.show()
