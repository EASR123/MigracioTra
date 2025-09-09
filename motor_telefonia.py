# motor_telefonia.py
import sqlite3, math
from datetime import datetime
from config_telefonia import TarifasConfig, FranjasHorarias, RangosEspeciales, IGNORAR_FACTURACION

class TelefoniaProcessor:
    def __init__(self, db_path: str = "telefonia.db"):
        
        self.cfg = TarifasConfig()
        self.fh  = FranjasHorarias()
        self.rx  = RangosEspeciales()
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._create_schema()
        self._cargar_overrides_config()
        self._post_config_sanity()
        self._reset_telefono_state()
            # Orden EXACTO de columnas (sin 'id') tal como se creó la tabla LOCAL
    COLS_LOCAL = [
        "GESTION","TELEFONO",
        # tiempos
        "TRA_BAS","TRA_NOR","TRA_RED","TRA_SRED","TRA_RUR","TRA_INT","TRA_VNO","TRA_VRE","TRA_104",
        "TRA_NNO","TRA_NRE","TRA_INO","TRA_IRE","TRA_CNO","TRA_CRE","TRA_MOV","TRA_PUB","TRA_TPL",
        "TRA_TP_RED","TRA_TP_SRE","TRA_VAG","TRA_DIP_NO","TRA_DIP_RE","TRA_DIP_SR","TRA_IPO_NO","TRA_IPO_RE","TRA_IPO_SR",
        # estado/tarifa
        "ESTADO","TARIFA",
        # costos
        "COS_BAS","COS_NOR","COS_RED","COS_SRED","COS_RUR","COS_INT","COS_VNO","COS_VRE","COS_104",
        "COS_NNO","COS_NRE","COS_INO","COS_IRE","COS_CNO","COS_CRE","COS_MOV","COS_PUB","COS_TPL",
        "COS_TP_RED","COS_TP_SRE","COS_VAG","COS_DIP_NO","COS_DIP_RE","COS_DIP_SR","COS_IPO_NO","COS_IPO_RE","COS_IPO_SR",
        "COS_TOT"
    ]


    # ---------- SCHEMA ----------
    def _create_schema(self):
        cur = self.conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS TRAFICO (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          TELEFONO INTEGER NOT NULL,
          FECHA TEXT NOT NULL,          -- YYMMDDHHMMSS
          DURACION INTEGER NOT NULL,    -- en segundos
          DESTINO TEXT NOT NULL
        );
        
        -- Config (valores que cambian en el tiempo)
        CREATE TABLE IF NOT EXISTS CONFIG (
          CLAVE TEXT PRIMARY KEY,
          VALOR REAL
        );

        CREATE TABLE IF NOT EXISTS LOCAL (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          GESTION TEXT NOT NULL,
          TELEFONO INTEGER NOT NULL,
          -- TRAFICO (tiempos en segundos)
          TRA_BAS INTEGER DEFAULT 0, TRA_NOR INTEGER DEFAULT 0, TRA_RED INTEGER DEFAULT 0, TRA_SRED INTEGER DEFAULT 0,
          TRA_RUR INTEGER DEFAULT 0, TRA_INT INTEGER DEFAULT 0, TRA_VNO INTEGER DEFAULT 0, TRA_VRE INTEGER DEFAULT 0,
          TRA_104 INTEGER DEFAULT 0, TRA_NNO INTEGER DEFAULT 0, TRA_NRE INTEGER DEFAULT 0, TRA_INO INTEGER DEFAULT 0,
          TRA_IRE INTEGER DEFAULT 0, TRA_CNO INTEGER DEFAULT 0, TRA_CRE INTEGER DEFAULT 0, TRA_MOV INTEGER DEFAULT 0,
          TRA_PUB INTEGER DEFAULT 0, TRA_TPL INTEGER DEFAULT 0, TRA_TP_RED INTEGER DEFAULT 0, TRA_TP_SRE INTEGER DEFAULT 0,
          TRA_VAG INTEGER DEFAULT 0, TRA_DIP_NO INTEGER DEFAULT 0, TRA_DIP_RE INTEGER DEFAULT 0, TRA_DIP_SR INTEGER DEFAULT 0,
          TRA_IPO_NO INTEGER DEFAULT 0, TRA_IPO_RE INTEGER DEFAULT 0, TRA_IPO_SR INTEGER DEFAULT 0,
          -- ESTADO/TARIFA y COSTOS acumulados
          ESTADO TEXT, TARIFA INTEGER,
          COS_BAS REAL DEFAULT 0, COS_NOR REAL DEFAULT 0, COS_RED REAL DEFAULT 0, COS_SRED REAL DEFAULT 0,
          COS_RUR REAL DEFAULT 0, COS_INT REAL DEFAULT 0, COS_VNO REAL DEFAULT 0, COS_VRE REAL DEFAULT 0,
          COS_104 REAL DEFAULT 0, COS_NNO REAL DEFAULT 0, COS_NRE REAL DEFAULT 0, COS_INO REAL DEFAULT 0,
          COS_IRE REAL DEFAULT 0, COS_CNO REAL DEFAULT 0, COS_CRE REAL DEFAULT 0, COS_MOV REAL DEFAULT 0,
          COS_PUB REAL DEFAULT 0, COS_TPL REAL DEFAULT 0, COS_TP_RED REAL DEFAULT 0, COS_TP_SRE REAL DEFAULT 0,
          COS_VAG REAL DEFAULT 0, COS_DIP_NO REAL DEFAULT 0, COS_DIP_RE REAL DEFAULT 0, COS_DIP_SR REAL DEFAULT 0,
          COS_IPO_NO REAL DEFAULT 0, COS_IPO_RE REAL DEFAULT 0, COS_IPO_SR REAL DEFAULT 0,
          COS_TOT REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS TELINTER (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          TELEFONO INTEGER NOT NULL,
          DURACION INTEGER NOT NULL,
          PROV INTEGER NOT NULL,
          COSTO REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS SERVICIO (
          ID_SERV INTEGER PRIMARY KEY,  -- mapea SERVICIO.ID_SERV del VFP
          NOMBRE TEXT,
          TELSER INTEGER                -- índice lógico del VFP
        );

        CREATE TABLE IF NOT EXISTS TRAF_MES (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          TELEFONO INTEGER NOT NULL,
          FECHA TEXT NOT NULL,          -- YYMMDDHHMMSS
          DURACION INTEGER NOT NULL,    -- segundos originales
          REDONDEO INTEGER NOT NULL,    -- segundos redondeados/partidos
          DESTINO TEXT NOT NULL,
          INTERNET INTEGER DEFAULT 0,   -- ID_SERV o 0
          COS_TARIFA REAL DEFAULT 0,    -- tarifa aplicada
          TIPO TEXT NOT NULL,           -- BAS,NOR,RED,SRE,INT,RUR,TPL,TPR,TPS,CEN,CER,CVN,CVR,LIN/LIR,LNN/LNR,CCC,VAG,IPN/IPR/IPS,IPN/IPO etc.
          COSTO REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS INTERDAT (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          TELEFONO INTEGER NOT NULL,
          FECHA TEXT NOT NULL,
          DURACION INTEGER NOT NULL,
          DESTINO TEXT NOT NULL,
          CONVENIO TEXT
        );

        CREATE TABLE IF NOT EXISTS TARIFAS (
          TARIFA INTEGER PRIMARY KEY,
          MINIMO INTEGER NOT NULL,      -- 60=por minuto, 1=por llamada
          UNIDAD TEXT NOT NULL,         -- 'MINUTO' | 'LLAMADA'
          MIN_LIBRES INTEGER NOT NULL,  -- minutos libres
          COS_TARIFA REAL NOT NULL,
          COS_NORMAL REAL NOT NULL,
          COS_REDUCI REAL NOT NULL,
          COS_SRED REAL NOT NULL,
          COS_INTER REAL NOT NULL,
          COS_RURAL REAL NOT NULL,
          COS_1 REAL NOT NULL,          -- rural móvil
          COS_3 REAL NOT NULL,          -- tel. público normal
          COS_TP_RED REAL NOT NULL,
          COS_TP_SRE REAL NOT NULL,
          COS_4 REAL NOT NULL,          -- valor agregado
          COS_CPP REAL NOT NULL,        -- fijo->móvil por segundo (CPP/60 por segundo)
          COS_IP_NO REAL NOT NULL, COS_IP_RE REAL NOT NULL, COS_IP_SR REAL NOT NULL,
          COS_IPO_NO REAL NOT NULL, COS_IPO_RE REAL NOT NULL, COS_IPO_SR REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS TELTARIF (
          TELEFONO INTEGER PRIMARY KEY,
          TARIFA INTEGER NOT NULL REFERENCES TARIFAS(TARIFA)
        );

        -- Tablas auxiliares de traslado/RDSI (pueden estar vacías, pero existen para “SELE 9,10,11,12”)
        CREATE TABLE IF NOT EXISTS TRAS_1 ( TELEFONO INTEGER PRIMARY KEY );
        CREATE TABLE IF NOT EXISTS TRAF_TRA ( id INTEGER PRIMARY KEY, TELEFONO INTEGER, FECHA TEXT, DURACION INTEGER, DESTINO TEXT );
        CREATE TABLE IF NOT EXISTS RDSI_1 ( TELEFONO INTEGER PRIMARY KEY );
        CREATE TABLE IF NOT EXISTS TRAFRDSI ( id INTEGER PRIMARY KEY, TELEFONO INTEGER, FECHA TEXT, DURACION INTEGER, DESTINO TEXT );

        -- Índices equivalentes a los “INDEX” del VFP (SQLite usa índices explícitos):
        CREATE INDEX IF NOT EXISTS idx_trafico_tel_fecha ON TRAFICO(TELEFONO, FECHA);
        CREATE INDEX IF NOT EXISTS idx_trafmes_tel_fecha ON TRAF_MES(TELEFONO, FECHA);
        CREATE INDEX IF NOT EXISTS idx_telinter_tel ON TELINTER(TELEFONO);
        CREATE INDEX IF NOT EXISTS idx_local_tel ON LOCAL(TELEFONO);
        """)
        self.conn.commit()


        cur.executescript("""
        CREATE VIEW IF NOT EXISTS v_TRAFiCO_ordenado AS
        SELECT * FROM TRAFICO ORDER BY TELEFONO, FECHA;

        CREATE VIEW IF NOT EXISTS v_TARIFAS AS SELECT * FROM TARIFAS;
        CREATE VIEW IF NOT EXISTS v_TELTARIF AS SELECT * FROM TELTARIF;
        CREATE VIEW IF NOT EXISTS v_TELINTER AS SELECT * FROM TELINTER;
        CREATE VIEW IF NOT EXISTS v_SERVICIO AS SELECT * FROM SERVICIO;
        CREATE VIEW IF NOT EXISTS v_LOCAL AS SELECT * FROM LOCAL;
        CREATE VIEW IF NOT EXISTS v_INTERDAT AS SELECT * FROM INTERDAT;
        CREATE VIEW IF NOT EXISTS v_TRAS_1 AS SELECT * FROM TRAS_1;
        CREATE VIEW IF NOT EXISTS v_TRAf_TRA AS SELECT * FROM TRAF_TRA;
        CREATE VIEW IF NOT EXISTS v_RDSI_1 AS SELECT * FROM RDSI_1;
        CREATE VIEW IF NOT EXISTS v_TRAFRDSI AS SELECT * FROM TRAFRDSI;
        """)
        self.conn.commit()
    @staticmethod
    def _yymmddhhmmss_to_parts(f: str):
        yy, mm, dd = int(f[0:2]), int(f[2:4]), int(f[4:6])
        hh, mi, ss = int(f[6:8]), int(f[8:10]), int(f[10:12])
        return 2000 + yy, mm, dd, hh*100 + mi, hh, mi, ss  # año, mes, día, HORA(HHMM), hh, mi, ss

    def _en_rango(self, numero: int, rangos: tuple) -> bool:
        for a,b in rangos:
            if a <= numero <= b: return True
        return False

    def _franja(self, hora_hhmm: int) -> str:
        if self.fh.HORA_NOR_INI <= hora_hhmm <= self.fh.HORA_NOR_FIN:
            return "NOR"
        if (self.fh.HORA_RED_1_INI <= hora_hhmm <= self.fh.HORA_RED_1_FIN) or \
           (self.fh.HORA_RED_2_INI <= hora_hhmm <= self.fh.HORA_RED_2_FIN):
            return "RED"
        return "SRE"  
    def _post_config_sanity(self):
        """Asegura que los valores críticos existan y sean válidos."""

        try:
            r = int(round(float(self.cfg.REDONDEO)))
        except Exception:
            r = 1
        if r <= 0:
            r = 1
        self.cfg.REDONDEO = r

    def _to_num(self, v):
        try:
            return float(v)
        except Exception:
            return None

    # ---------- ESTADO POR TELÉFONO ----------
    def _reset_telefono_state(self):
        # tiempos
        self.T = {k:0 for k in (
            "BAS","NOR","RED","SRE","RUR","INT","VNO","VRE","104","NNO","NRE","INO","IRE",
            "CNO","CRE","MOV","PUB","TPL","TP_RED","TP_SRE","VAG","DIP_NO","DIP_RE","DIP_SR",
            "IPO_NO","IPO_RE","IPO_SR"
        )}
        # costos
        self.C = {k:0.0 for k in (
            "BAS","NOR","RED","SRE","RUR","INT","VNO","VRE","104","NNO","NRE","INO","IRE",
            "CNO","CRE","MOV","PUB","TPL","TP_RED","TP_SRE","VAG","DIP_NO","DIP_RE","DIP_SR",
            "IPO_NO","IPO_RE","IPO_SR"
        )}
        self.COSTO_TEL = 0.0
        self.TEL_AUX = 0
        self.TAR_AUX = 0
        self.V_MINIMO = 60
        self.V_UNIDAD = "MINUTO"
        self.MIN_AUX = 0
        # precios variables (se actualizan por tarifa / o default)
        self.V_NORMAL = self.cfg.AUX_NOR
        self.V_REDUCIDO = self.cfg.AUX_RED
        self.V_SRED = self.cfg.AUX_SRED
        self.V_INTERNET = self.cfg.AUX_INTERNET
        self.V_RURAL = self.cfg.AUX_RURAL
        self.V_RUR_MOV = self.cfg.AUX_RUR_MOV
        self.V_800_MOV_PUB = self.cfg.AUX_800_MOV_PUB
        self.V_TP_RED = self.cfg.AUX_TEL_PUB_RED
        self.V_TP_SRE = self.cfg.AUX_TEL_PUB_SRED
        self.V_VAG = self.cfg.AUX_VAG
        self.V_CPP = self.cfg.AUX_CPP
        self.V_IP_NO, self.V_IP_RE, self.V_IP_SR = self.cfg.AUX_COS_IP_NO, self.cfg.AUX_COS_IP_RE, self.cfg.AUX_COS_IP_SR
        self.V_IPO_NO, self.V_IPO_RE, self.V_IPO_SR = self.cfg.AUX_COS_IPO_NO, self.cfg.AUX_COS_IPO_RE, self.cfg.AUX_COS_IPO_SR
        self.LETRA_ESTADO = "S"  # 'S' sin tarifa específica, 'T' con tarifa

    # ---------- TARIFAS ----------
    def _cargar_tarifa_de_telefono(self, telefono: int):
        cur = self.conn.cursor()
        cur.execute("SELECT TARIFA FROM TELTARIF WHERE TELEFONO=?", (telefono,))
        row = cur.fetchone()
        if not row:

            self.TAR_AUX = 0
            self.MIN_AUX = 200 * 60
            self.COSTO_TEL += self.cfg.AUX_TAR
            self.V_NORMAL = self.cfg.AUX_NOR
            self.V_REDUCIDO = self.cfg.AUX_RED
            self.V_SRED = self.cfg.AUX_SRED
            self.V_INTERNET = self.cfg.AUX_INTERNET
            self.V_RURAL = self.cfg.AUX_RURAL
            self.V_RUR_MOV = self.cfg.AUX_RUR_MOV
            self.V_800_MOV_PUB = self.cfg.AUX_800_MOV_PUB
            self.V_TP_RED = self.cfg.AUX_TEL_PUB_RED
            self.V_TP_SRE = self.cfg.AUX_TEL_PUB_SRED
            self.V_VAG = self.cfg.AUX_VAG
            self.V_CPP = self.cfg.AUX_CPP
            self.V_IP_NO, self.V_IP_RE, self.V_IP_SR = self.cfg.AUX_COS_IP_NO, self.cfg.AUX_COS_IP_RE, self.cfg.AUX_COS_IP_SR
            self.V_IPO_NO, self.V_IPO_RE, self.V_IPO_SR = self.cfg.AUX_COS_IPO_NO, self.cfg.AUX_COS_IPO_RE, self.cfg.AUX_COS_IPO_SR
            self.LETRA_ESTADO = "S"
            self.V_MINIMO = 60
            self.V_UNIDAD = "MINUTO"
            return

        tarifa = row[0]
        cur.execute("SELECT * FROM TARIFAS WHERE TARIFA=?", (tarifa,))
        t = cur.fetchone()
        if not t:
            self._reset_telefono_state()
            return

        (
            _TARIFA, MINIMO, UNIDAD, MIN_LIBRES, COS_TARIFA, COS_NORMAL, COS_REDUCI, COS_SRED,
            COS_INTER, COS_RURAL, COS_1, COS_3, COS_TP_RED, COS_TP_SRE, COS_4, COS_CPP,
            COS_IP_NO, COS_IP_RE, COS_IP_SR, COS_IPO_NO, COS_IPO_RE, COS_IPO_SR
        ) = t

        self.TAR_AUX = tarifa
        self.V_MINIMO = MINIMO
        self.V_UNIDAD = UNIDAD
        self.MIN_AUX = MIN_LIBRES * MINIMO
        self.COSTO_TEL += COS_TARIFA
        self.V_NORMAL, self.V_REDUCIDO, self.V_SRED = COS_NORMAL, COS_REDUCI, COS_SRED
        self.V_INTERNET, self.V_RURAL = COS_INTER, COS_RURAL
        self.V_RUR_MOV, self.V_800_MOV_PUB = COS_1, COS_3
        self.V_TP_RED, self.V_TP_SRE = COS_TP_RED, COS_TP_SRE
        self.V_VAG, self.V_CPP = COS_4, COS_CPP
        self.V_IP_NO, self.V_IP_RE, self.V_IP_SR = COS_IP_NO, COS_IP_RE, COS_IP_SR
        self.V_IPO_NO, self.V_IPO_RE, self.V_IPO_SR = COS_IPO_NO, COS_IPO_RE, COS_IPO_SR
        self.LETRA_ESTADO = "T"

    # ---------- PERSISTENCIA ----------
    def _guardar_local(self, telefono: int):

        row = {
            "GESTION": self.cfg.PERIODO,
            "TELEFONO": telefono,

            "TRA_BAS": self.T["BAS"],   "TRA_NOR": self.T["NOR"],   "TRA_RED": self.T["RED"],
            "TRA_SRED": self.T["SRE"],  "TRA_RUR": self.T["RUR"],   "TRA_INT": self.T["INT"],
            "TRA_VNO": self.T["VNO"],   "TRA_VRE": self.T["VRE"],   "TRA_104": self.T["104"],
            "TRA_NNO": self.T["NNO"],   "TRA_NRE": self.T["NRE"],   "TRA_INO": self.T["INO"],
            "TRA_IRE": self.T["IRE"],   "TRA_CNO": self.T["CNO"],   "TRA_CRE": self.T["CRE"],
            "TRA_MOV": self.T["MOV"],   "TRA_PUB": self.T["PUB"],   "TRA_TPL": self.T["TPL"],
            "TRA_TP_RED": self.T["TP_RED"], "TRA_TP_SRE": self.T["TP_SRE"], "TRA_VAG": self.T["VAG"],
            "TRA_DIP_NO": self.T["DIP_NO"], "TRA_DIP_RE": self.T["DIP_RE"], "TRA_DIP_SR": self.T["DIP_SR"],
            "TRA_IPO_NO": self.T["IPO_NO"], "TRA_IPO_RE": self.T["IPO_RE"], "TRA_IPO_SR": self.T["IPO_SR"],

            "ESTADO": self.LETRA_ESTADO,
            "TARIFA": self.TAR_AUX,

            "COS_BAS": self.C["BAS"],   "COS_NOR": self.C["NOR"],   "COS_RED": self.C["RED"],
            "COS_SRED": self.C["SRE"],  "COS_RUR": self.C["RUR"],   "COS_INT": self.C["INT"],
            "COS_VNO": self.C["VNO"],   "COS_VRE": self.C["VRE"],   "COS_104": self.C["104"],
            "COS_NNO": self.C["NNO"],   "COS_NRE": self.C["NRE"],   "COS_INO": self.C["INO"],
            "COS_IRE": self.C["IRE"],   "COS_CNO": self.C["CNO"],   "COS_CRE": self.C["CRE"],
            "COS_MOV": self.C["MOV"],   "COS_PUB": self.C["PUB"],   "COS_TPL": self.C["TPL"],
            "COS_TP_RED": self.C["TP_RED"], "COS_TP_SRE": self.C["TP_SRE"], "COS_VAG": self.C["VAG"],
            "COS_DIP_NO": self.C["DIP_NO"], "COS_DIP_RE": self.C["DIP_RE"], "COS_DIP_SR": self.C["DIP_SR"],
            "COS_IPO_NO": self.C["IPO_NO"], "COS_IPO_RE": self.C["IPO_RE"], "COS_IPO_SR": self.C["IPO_SR"],

            "COS_TOT": self.COSTO_TEL
        }


        cols = self.COLS_LOCAL
        placeholders = ",".join("?" for _ in cols)
        sql = f"INSERT INTO LOCAL ({','.join(cols)}) VALUES ({placeholders})"
        values = [row[c] for c in cols]

        self.conn.execute(sql, values)
        self.conn.commit()


    def _push_traf_mes(self, tel, fecha, duracion, redondeo, destino, internet, cos_tarifa, tipo, costo):
        self.conn.execute("""
            INSERT INTO TRAF_MES(TELEFONO,FECHA,DURACION,REDONDEO,DESTINO,INTERNET,COS_TARIFA,TIPO,COSTO)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (tel, fecha, duracion, redondeo, destino, internet, cos_tarifa, tipo, costo))
        self.conn.commit()


    def _acum(self, clave_tiempo: str, segundos: int, clave_costo: str, costo: float):
        self.T[clave_tiempo] += segundos
        self.C[clave_costo]  += round(costo, 4)
        self.COSTO_TEL       += round(costo, 4)
        
    def _cargar_overrides_config(self):
        """Lee CONFIG y pisa self.cfg sólo con números válidos."""
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT CLAVE, VALOR_NUM FROM CONFIG")
        except sqlite3.OperationalError:

            return

        for k, v in cur.fetchall():
            val = self._to_num(v)
            if val is None:
                continue
            if not hasattr(self.cfg, k):
                continue
            if k == "REDONDEO":

                try:
                    vv = int(round(float(val)))
                except Exception:
                    vv = 1
                setattr(self.cfg, k, max(1, vv))
            else:
                setattr(self.cfg, k, float(val))




    def process(self):
        print("INICIO:", datetime.now().date(), datetime.now().time(), "PERIODO:", self.cfg.PERIODO)
        cur = self.conn.cursor()
        cur.execute("SELECT TELEFONO, FECHA, DURACION, DESTINO FROM v_TRAFiCO_ordenado")
        rows = cur.fetchall()

        tel_actual = None
        for idx, (TELEFONO, FECHA, DURACION, DESTINO) in enumerate(rows, start=1):

            if tel_actual is None or TELEFONO != tel_actual:
                if tel_actual is not None:
                    self._guardar_local(tel_actual)

                self._reset_telefono_state()
                self._cargar_tarifa_de_telefono(TELEFONO)
                tel_actual = TELEFONO


            try:
                var_des_7 = int(str(DESTINO).strip()[:7] or 0)
            except:
                var_des_7 = 0
            if TELEFONO in IGNORAR_FACTURACION:

                self.conn.execute("INSERT INTO INTERDAT(TELEFONO,FECHA,DURACION,DESTINO,CONVENIO) VALUES (?,?,?,?,?)",
                                  (TELEFONO, FECHA, DURACION, DESTINO, "NOFACT"))
                continue


            V_DURA = math.ceil(DURACION / self.cfg.REDONDEO) * self.cfg.REDONDEO
            yy, mm, dd, HORA, hh, mi, ss = self._yymmddhhmmss_to_parts(FECHA)
            fr = self._franja(HORA)

            destino = str(DESTINO).strip()
            pref2 = destino[:2]
            pref3 = destino[:3]
            pref8 = destino[:8]
            pref9 = destino[:9]

            prov = 0

            c2 = self.conn.cursor()
            c2.execute("SELECT ID_SERV FROM SERVICIO WHERE TELSER=?", (var_des_7,))
            srow = c2.fetchone()
            if srow:
                prov = int(srow[0])

            # 107 => 1 minuto fijo
            if pref3 == '107':
                V_DURA = 60

            # Valor agregado 900161616/900162010
            if pref9 in self.rx.VALOR_AGREGADO_9:
                costo = self.cfg.AUX_VAG if FECHA[0:6] > '100610' else 0.0
                self._acum("VAG", 60, "VAG", costo)
                self._push_traf_mes(TELEFONO, FECHA, DURACION, 60, DESTINO, 0, self.cfg.AUX_VAG, "VAG", costo)
                continue

            # 104 (cargo por llamada)
            if pref3 == '104':
                costo = 0.0  
                self._acum("104", 60, "104", costo)
                self._push_traf_mes(TELEFONO, FECHA, DURACION, 60, DESTINO, 0, 0.0, "CCC", costo)
                continue

            # 106/2310000 -> Correo de voz
            if pref3 == '106' or destino == '2310000':
                if fr == "NOR":
                    costo = (V_DURA/60)*self.V_NORMAL
                    self._acum("VNO", V_DURA, "VNO", costo)
                    self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, self.V_NORMAL, "CVN", costo)
                else:
                    costo = (V_DURA/60)*self.V_REDUCIDO
                    self._acum("VRE", V_DURA, "VRE", costo)
                    self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, self.V_REDUCIDO, "CVR", costo)
                continue

            # ----------------- LLAMADA LOCAL -----------------
            es_local = (pref2 in self.rx.LOCALES_2) or (pref3 in self.rx.ESPECIALES_3) or (pref9 in self.rx.VALOR_AGREGADO_9)
            if es_local and V_DURA > 0:
                # Internet por SERVICIO
                if prov:
                    # reglas de INTCOTEL_RUR_LIB/PLA para ciertos planes (38/39) y servicios 96/97
                    # Para simplificar, aplicamos tarifa de internet general si no coincide caso especial
                    costo_min = self.V_INTERNET
                    if prov in (96,97) and self.TAR_AUX in (38,39):
                        costo_min = self.cfg.AUX_INTCOTEL_RUR_LIB if prov == 96 else self.cfg.AUX_INTCOTEL_RUR_PLA
                    costo = (V_DURA/60) * costo_min
                    self._acum("INT", V_DURA, "INT", costo)
                    # además registro en TELINTER
                    self.conn.execute("INSERT INTO TELINTER(TELEFONO,DURACION,PROV,COSTO) VALUES (?,?,?,?)",
                                      (TELEFONO, V_DURA, prov, round(costo,4)))
                    self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, prov, costo_min, "INT", costo)
                    continue

                # VOIP por rango IP “50xxxxxx” y subrango 50320000–50399999 con franja
                if destino.startswith(self.rx.IP_8_PREF):
                    try:
                        rip = int(destino[:8])
                    except:
                        rip = 0
                    if self.rx.IP_RANGO_MIN <= rip <= self.rx.IP_RANGO_MAX:
                        # DIP_* (directo IP)
                        if fr == "NOR":
                            costo_min = self.V_IP_NO; clave = ("DIP_NO", "IPN")
                        elif fr == "RED":
                            costo_min = self.V_IP_RE; clave = ("DIP_RE", "IPR")
                        else:
                            costo_min = self.V_IP_SR; clave = ("DIP_SR", "IPS")
                        costo = (V_DURA/60)*costo_min
                        self._acum(clave[0], V_DURA, clave[0], costo)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, costo_min, clave[1], costo)
                        continue
                    else:
                        # IP Operador (IPO_*)
                        if fr == "NOR":
                            costo_min = self.V_IPO_NO; clave = ("IPO_NO","IPN")
                        elif fr == "RED":
                            costo_min = self.V_IPO_RE; clave = ("IPO_RE","IPR")
                        else:
                            costo_min = self.V_IPO_SR; clave = ("IPO_SR","IPS")
                        costo = (V_DURA/60)*costo_min
                        self._acum(clave[0], V_DURA, clave[0], costo)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, costo_min, clave[1], costo)
                        continue

                # Rural
                try:
                    n7 = int(destino[:7])
                except:
                    n7 = 0
                if self._en_rango(n7, self.rx.RURALES):
                    costo_min = self.V_RURAL
                    costo = (V_DURA/60)*costo_min
                    self._acum("RUR", V_DURA, "RUR", costo)
                    self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, costo_min, "RUR", costo)
                    continue

                # Teléfono público (TPL/TPR/TPS según franja)
                if self._en_rango(n7, self.rx.PUBLICOS):
                    if fr == "NOR":
                        costo_min = self.V_800_MOV_PUB; tipo="TPL"; clave=("TPL","TPL")
                    elif fr == "RED":
                        costo_min = self.V_TP_RED;     tipo="TPR"; clave=("TP_RED","TP_RED")
                    else:
                        costo_min = self.V_TP_SRE;     tipo="TPS"; clave=("TP_SRE","TP_SRE")
                    costo = (V_DURA/60)*costo_min
                    self._acum(clave[0], V_DURA, clave[1], costo)
                    self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, costo_min, tipo, costo)
                    continue

                # Básico/minutos libres y luego franja NOR/RED/SRE
                # Acumula consumo básico (por minuto o por llamada)
                cons = 1 if self.V_UNIDAD.upper()=="LLAMADA" else V_DURA
                prev_bas = self.T["BAS"]
                self.T["BAS"] += cons
                if self.T["BAS"] <= self.MIN_AUX:
                    # dentro de básicos => costo 0
                    self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, 0.0, "BAS", 0.0)
                else:
                    # parte si cruzó el mínimo
                    exced = self.T["BAS"] - self.MIN_AUX
                    usado_en_bas = cons - exced
                    if usado_en_bas > 0:
                        # tramo gratuito
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, usado_en_bas if self.V_UNIDAD.upper()=="MINUTO" else 0, DESTINO, 0, 0.0, "BAS", 0.0)
                        # ajusta el restante
                        V_DURA = exced if self.V_UNIDAD.upper()=="MINUTO" else V_DURA

                    # tramo tarifado por franja
                    if fr == "NOR":
                        costo_min = self.V_NORMAL; tipo="NOR"; clave=("NOR","NOR")
                    elif fr == "RED":
                        costo_min = self.V_REDUCIDO; tipo="RED"; clave=("RED","RED")
                    else:
                        costo_min = self.V_SRED; tipo="SRE"; clave=("SRE","SRE")
                    # si UNIDAD es 'LLAMADA', cobra 1 unidad al costo_min
                    costo = ((V_DURA/self.V_MINIMO) if self.V_UNIDAD.upper()=="MINUTO" else 1.0) * costo_min
                    self._acum(clave[0], V_DURA if self.V_UNIDAD.upper()=="MINUTO" else 0, clave[1], costo)
                    self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, costo_min, tipo, costo)
                continue

            # ----------------- OTROS OPERADORES: 0xx (LDI/LDN) y 6/7 móviles -----------------
            if destino and destino[0] in ('0','6','7') and V_DURA>0:
                # 0x0... => internacional (LIN/LIR), 0x... => nacional (LNN/LNR) – aquí simplificado
                if destino[0]=='0' and len(destino)>1 and destino[1]=='0':
                    # Internacional
                    if fr == "NOR":
                        self._acum("INO", 60, "INO", 0.0)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, 60, DESTINO, 0, 0.0, "LIN", 0.0)
                    else:
                        self._acum("IRE", 60, "IRE", 0.0)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, 60, DESTINO, 0, 0.0, "LIR", 0.0)
                elif destino[0]=='0':
                    # Nacional
                    if fr == "NOR":
                        self._acum("NNO", 60, "NNO", 0.0)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, 60, DESTINO, 0, 0.0, "LNN", 0.0)
                    else:
                        self._acum("NRE", 60, "NRE", 0.0)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, 60, DESTINO, 0, 0.0, "LNR", 0.0)
                else:
                    # Celular (CPP por segundo)
                    costo = (self.V_CPP/60.0) * V_DURA
                    if fr == "NOR":
                        self._acum("CNO", V_DURA, "CNO", costo)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, self.V_CPP, "CEN", costo)
                    else:
                        self._acum("CRE", V_DURA, "CRE", costo)
                        self._push_traf_mes(TELEFONO, FECHA, DURACION, V_DURA, DESTINO, 0, self.V_CPP, "CER", costo)
                continue

            # Si llegó aquí y no encaja, lo mandamos a INTERDAT (no procesado)
            self.conn.execute(
                "INSERT INTO INTERDAT(TELEFONO,FECHA,DURACION,DESTINO,CONVENIO) VALUES (?,?,?,?,?)",
                (TELEFONO, FECHA, DURACION, DESTINO, "NOPROCESADO")
            )

        # guarda el último teléfono
        if tel_actual is not None:
            self._guardar_local(tel_actual)

        print("FIN:", datetime.now().date(), datetime.now().time())
