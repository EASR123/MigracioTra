# run_demo.py
from cargar_datos_demo import cargar_datos_demo
from motor_telefonia import TelefoniaProcessor
from graficas_telefonia import (
    grafico_traf_mes,
    grafico_conteo_por_tabla,
    exportar_reporte_excel,  # opcional: útil para tu punto 5
)

if __name__ == "__main__":
    DB_PATH = "telefonia.db"

    # 1) Cargar datos de ejemplo (llenado completo)
    cargar_datos_demo(DB_PATH)

    # 2) Procesar tráfico
    p = TelefoniaProcessor(DB_PATH)
    p.process()

    # 3) Graficar (general - todo el rango de fechas cargado)
    grafico_traf_mes(DB_PATH)  # puedes pasar fecha_like="250701" para filtrar por YYMMDD
    grafico_conteo_por_tabla(DB_PATH)

    # 4) (Opcional) Exportar a Excel un reporte filtrado por fecha concreta:
    # exportar_reporte_excel(DB_PATH, fecha_like="250701", out_path="reporte_250701.xlsx")
