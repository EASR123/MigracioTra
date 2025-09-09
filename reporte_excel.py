# reporte_excel.py
import sqlite3
import pandas as pd

def exportar_traf_mes_excel(db_path="telefonia.db", periodo_yyMM="2507", salida="reporte_trafmes_2507.xlsx"):
    con = sqlite3.connect(db_path)

    detalle = pd.read_sql_query("""
        SELECT TELEFONO, FECHA, DURACION, REDONDEO, DESTINO, INTERNET, COS_TARIFA, TIPO, COSTO
        FROM TRAF_MES
        WHERE substr(FECHA,1,4)=?
        ORDER BY TELEFONO, FECHA
    """, con, params=(periodo_yyMM,))

    resumen_tipo = detalle.groupby("TIPO", as_index=False).agg(
        segundos=("REDONDEO","sum"),
        costo=("COSTO","sum")
    ).sort_values("costo", ascending=False)

    resumen_tel_tipo = detalle.groupby(["TELEFONO","TIPO"], as_index=False).agg(
        segundos=("REDONDEO","sum"),
        costo=("COSTO","sum")
    ).sort_values(["TELEFONO","costo"], ascending=[True,False])

    with pd.ExcelWriter(salida, engine="xlsxwriter") as writer:
        detalle.to_excel(writer, sheet_name="Detalle", index=False)
        resumen_tipo.to_excel(writer, sheet_name="Resumen_TIPO", index=False)
        resumen_tel_tipo.to_excel(writer, sheet_name="Resumen_TELxTIPO", index=False)

    con.close()
    print(f"Excel generado: {salida}")
