# ui_reportes.py
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sqlite3
import pandas as pd

from motor_telefonia import TelefoniaProcessor
from utils_trafico import agregar_trafico_para_fecha
from graficas_telefonia import (
    grafico_traf_mes,
    grafico_traf_mes_comparar,
    grafico_conteo_por_tabla,
    exportar_reporte_excel,
    resumen_costo_por_telefono,       # NUEVO
    grafico_costo_por_telefono,       # NUEVO
)

DB_PATH = "telefonia.db"

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Reportes de Telefonía - Proyecto parte2")
        self.geometry("980x640")

        # almacenaremos el último DataFrame del reporte por teléfono
        self.df_rep_tel = pd.DataFrame()

        self._build_ui()

    def _build_ui(self):
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        # --- Tab Procesamiento ---
        frm_proc = ttk.Frame(nb)
        nb.add(frm_proc, text="Procesamiento")

        ttk.Button(frm_proc, text="Procesar tráfico (run)",
                   command=self.cmd_procesar).grid(row=0, column=0, padx=6, pady=6, sticky="w")

        ttk.Label(frm_proc, text="Agregar tráfico para fecha (YYMMDD):").grid(row=1, column=0, padx=6, pady=6, sticky="w")
        self.e_fecha_add = ttk.Entry(frm_proc, width=12)
        self.e_fecha_add.insert(0, "250702")
        self.e_fecha_add.grid(row=1, column=1, padx=6, pady=6, sticky="w")

        ttk.Button(frm_proc, text="Agregar tráfico sintético",
                   command=self.cmd_agregar_trafico).grid(row=1, column=2, padx=6, pady=6, sticky="w")

        # --- Tab Gráficas ---
        frm_gra = ttk.Frame(nb)
        nb.add(frm_gra, text="Gráficas")

        ttk.Label(frm_gra, text="Fecha (prefijo FECHA YYMM o YYMMDD):").grid(row=0, column=0, padx=6, pady=6, sticky="w")
        self.e_fecha_like = ttk.Entry(frm_gra, width=10)
        self.e_fecha_like.insert(0, "")
        self.e_fecha_like.grid(row=0, column=1, padx=6, pady=6, sticky="w")

        ttk.Button(frm_gra, text="Costo por TIPO",
                   command=self.cmd_grafico_traf_mes).grid(row=0, column=2, padx=6, pady=6, sticky="w")

        ttk.Button(frm_gra, text="Conteo por tabla",
                   command=self.cmd_grafico_conteo).grid(row=0, column=3, padx=6, pady=6, sticky="w")

        ttk.Separator(frm_gra, orient="horizontal").grid(row=1, column=0, columnspan=5, sticky="ew", padx=6, pady=6)

        ttk.Label(frm_gra, text="Comparar A (YYMM o YYMMDD):").grid(row=2, column=0, padx=6, pady=6, sticky="w")
        self.e_fecha_a = ttk.Entry(frm_gra, width=10)
        self.e_fecha_a.insert(0, "250701")
        self.e_fecha_a.grid(row=2, column=1, padx=6, pady=6, sticky="w")

        ttk.Label(frm_gra, text="vs B (YYMM o YYMMDD):").grid(row=2, column=2, padx=6, pady=6, sticky="w")
        self.e_fecha_b = ttk.Entry(frm_gra, width=10)
        self.e_fecha_b.insert(0, "250702")
        self.e_fecha_b.grid(row=2, column=3, padx=6, pady=6, sticky="w")

        ttk.Button(frm_gra, text="Comparar costos por TIPO",
                   command=self.cmd_grafico_comparar).grid(row=2, column=4, padx=6, pady=6, sticky="w")

        # --- Tab Reporte por teléfono (NUEVO) ---
        frm_rep = ttk.Frame(nb)
        nb.add(frm_rep, text="Reporte por teléfono")

        ttk.Label(frm_rep, text="Fecha (prefijo FECHA YYMM o YYMMDD):").grid(row=0, column=0, padx=6, pady=6, sticky="w")
        self.e_rep_fecha = ttk.Entry(frm_rep, width=10)
        self.e_rep_fecha.insert(0, "")
        self.e_rep_fecha.grid(row=0, column=1, padx=6, pady=6, sticky="w")

        ttk.Button(frm_rep, text="Ver resumen",
                   command=self.cmd_rep_refrescar).grid(row=0, column=2, padx=6, pady=6, sticky="w")

        ttk.Label(frm_rep, text="Top N (gráfico):").grid(row=0, column=3, padx=6, pady=6, sticky="e")
        self.sb_top_n = tk.Spinbox(frm_rep, from_=5, to=100, width=5)
        self.sb_top_n.delete(0, tk.END)
        self.sb_top_n.insert(0, "15")
        self.sb_top_n.grid(row=0, column=4, padx=6, pady=6, sticky="w")

        ttk.Button(frm_rep, text="Gráfico Top-N por costo",
                   command=self.cmd_rep_grafico).grid(row=0, column=5, padx=6, pady=6, sticky="w")

        ttk.Button(frm_rep, text="Exportar a Excel",
                   command=self.cmd_rep_exportar_excel).grid(row=0, column=6, padx=6, pady=6, sticky="w")

        # Tabla (Treeview)
        cols = ("TELEFONO","LLAMADAS","MINUTOS","COSTO")
        self.tv_rep = ttk.Treeview(frm_rep, columns=cols, show="headings", height=18)
        for c, w in zip(cols, (140, 100, 100, 120)):
            self.tv_rep.heading(c, text=c)
            self.tv_rep.column(c, width=w, anchor="center")
        self.tv_rep.grid(row=1, column=0, columnspan=7, padx=6, pady=6, sticky="nsew")

        # Scrollbars
        yscroll = ttk.Scrollbar(frm_rep, orient=tk.VERTICAL, command=self.tv_rep.yview)
        self.tv_rep.configure(yscroll=yscroll.set)
        yscroll.grid(row=1, column=7, sticky="ns")

        # expand
        frm_rep.grid_rowconfigure(1, weight=1)
        frm_rep.grid_columnconfigure(6, weight=1)

        # --- Tab Configuración ---
        frm_cfg = ttk.Frame(nb)
        nb.add(frm_cfg, text="Configuración")

        self.vars = {}
        campos = [
            "REDONDEO","AUX_NOR","AUX_RED","AUX_SRED","AUX_TAR","AUX_INTERNET","AUX_INTCOTEL",
            "AUX_INTCOTEL_RUR_LIB","AUX_INTCOTEL_RUR_PLA","AUX_RUR_MOV","AUX_RURAL",
            "AUX_800_MOV_PUB","AUX_TEL_PUB_RED","AUX_TEL_PUB_SRED","AUX_VAG","AUX_CPP",
            "AUX_COS_IP_NO","AUX_COS_IP_RE","AUX_COS_IP_SR",
            "AUX_COS_IPO_NO","AUX_COS_IPO_RE","AUX_COS_IPO_SR",
        ]
        for i, k in enumerate(campos):
            ttk.Label(frm_cfg, text=k).grid(row=i%12, column=(i//12)*2, padx=6, pady=4, sticky="e")
            e = ttk.Entry(frm_cfg, width=10)
            e.grid(row=i%12, column=(i//12)*2+1, padx=6, pady=4, sticky="w")
            self.vars[k] = e

        ttk.Button(frm_cfg, text="Cargar actuales", command=self.cmd_cargar_config).grid(row=12, column=0, padx=6, pady=8, sticky="w")
        ttk.Button(frm_cfg, text="Guardar cambios", command=self.cmd_guardar_config).grid(row=12, column=1, padx=6, pady=8, sticky="w")

        # --- Tab Exportar (general) ---
        frm_xls = ttk.Frame(nb)
        nb.add(frm_xls, text="Exportar")

        ttk.Label(frm_xls, text="Fecha (prefijo FECHA YYMM o YYMMDD):").grid(row=0, column=0, padx=6, pady=6, sticky="w")
        self.e_fecha_xls = ttk.Entry(frm_xls, width=10)
        self.e_fecha_xls.insert(0, "250701")
        self.e_fecha_xls.grid(row=0, column=1, padx=6, pady=6, sticky="w")

        ttk.Button(frm_xls, text="Exportar a Excel", command=self.cmd_exportar_excel).grid(row=0, column=2, padx=6, pady=6, sticky="w")


    def cmd_procesar(self):
        try:
            p = TelefoniaProcessor(DB_PATH)
            p.process()
            messagebox.showinfo("OK", "Procesamiento completado.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_agregar_trafico(self):
        yymmdd = self.e_fecha_add.get().strip()
        if not yymmdd:
            messagebox.showwarning("Atención", "Ingrese YYMMDD.")
            return
        try:
            agregar_trafico_para_fecha(DB_PATH, yymmdd)
            messagebox.showinfo("OK", f"Tráfico agregado para {yymmdd}. Ahora ejecuta 'Procesar tráfico'.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_grafico_traf_mes(self):
        fecha = self.e_fecha_like.get().strip() or None
        try:
            grafico_traf_mes(DB_PATH, fecha_like=fecha)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_grafico_conteo(self):
        try:
            grafico_conteo_por_tabla(DB_PATH)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_grafico_comparar(self):
        a = self.e_fecha_a.get().strip()
        b = self.e_fecha_b.get().strip()
        if not a or not b:
            messagebox.showwarning("Atención", "Complete ambas fechas (A y B).")
            return
        try:
            grafico_traf_mes_comparar(DB_PATH, fecha_a=a, fecha_b=b)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_cargar_config(self):
        try:
            p = TelefoniaProcessor(DB_PATH)
            for k, e in self.vars.items():
                val = getattr(p.cfg, k, "")
                e.delete(0, tk.END)
                e.insert(0, str(val))
            messagebox.showinfo("OK", "Variables cargadas.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_guardar_config(self):
        try:
            p = TelefoniaProcessor(DB_PATH)
            kwargs = {}
            for k, e in self.vars.items():
                txt = e.get().strip()
                if txt == "":
                    continue
                kwargs[k] = float(txt)
            if kwargs:
                p.set_config_overrides(**kwargs)
                messagebox.showinfo("OK", "Variables actualizadas en CONFIG.")
            else:
                messagebox.showinfo("OK", "No hay cambios.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_exportar_excel(self):
        fecha = self.e_fecha_xls.get().strip() or None
        out = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                           filetypes=[("Excel", "*.xlsx")],
                                           initialfile=f"reporte_{fecha or 'todo'}.xlsx")
        if not out:
            return
        try:
            exportar_reporte_excel(DB_PATH, fecha_like=fecha, out_path=out)
            messagebox.showinfo("OK", f"Exportado a:\n{out}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # --- NUEVO (Reporte por teléfono) ---
    def cmd_rep_refrescar(self):
        fecha = self.e_rep_fecha.get().strip() or None
        try:
            df = resumen_costo_por_telefono(DB_PATH, fecha_like=fecha)
            self.df_rep_tel = df.copy()
            # Pinta en Treeview
            for row in self.tv_rep.get_children():
                self.tv_rep.delete(row)
            for _, r in df.iterrows():
                self.tv_rep.insert("", "end", values=(str(r["TELEFONO"]), int(r["llamadas"]), float(r["minutos"]), float(r["costo"])))
            if df.empty:
                messagebox.showinfo("Info", "No hay datos para ese filtro.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_rep_grafico(self):
        fecha = self.e_rep_fecha.get().strip() or None
        try:
            topn = int(self.sb_top_n.get())
        except Exception:
            topn = 15
        try:
            grafico_costo_por_telefono(DB_PATH, fecha_like=fecha, top=topn)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def cmd_rep_exportar_excel(self):
        
        if self.df_rep_tel is None or self.df_rep_tel.empty:
            self.cmd_rep_refrescar()
            if self.df_rep_tel is None or self.df_rep_tel.empty:
                return
        out = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                           filetypes=[("Excel", "*.xlsx")],
                                           initialfile="reporte_por_telefono.xlsx")
        if not out:
            return
        try:
            with pd.ExcelWriter(out, engine="openpyxl") as xw:
                self.df_rep_tel.to_excel(xw, index=False, sheet_name="CostoPorTelefono")
            messagebox.showinfo("OK", f"Exportado a:\n{out}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

if __name__ == "__main__":
    app = App()
    app.mainloop()
