# config_telefonia.py
from dataclasses import dataclass

@dataclass
class TarifasConfig:
    # Redondeo y periodo
    PERIODO: str = "01072025"   # DDMMAAAA
    REDONDEO: int = 1           # (segundos)

    # Costos/auxiliares por defecto (si el teléfono no tiene tarifa en TELTARIF/TARIFAS)
    AUX_NOR: float = 0.20
    AUX_RED: float = 0.13
    AUX_SRED: float = 0.07
    AUX_TAR: float = 16.00
    AUX_INTERNET: float = 0.07
    AUX_INTCOTEL: float = 0.12
    AUX_INTCOTEL_RUR_LIB: float = 0.20
    AUX_INTCOTEL_RUR_PLA: float = 0.15
    AUX_RUR_MOV: float = 1.00
    AUX_RURAL: float = 0.60
    AUX_800_MOV_PUB: float = 0.51
    AUX_TEL_PUB_RED: float = 0.34
    AUX_TEL_PUB_SRED: float = 0.17
    AUX_VAG: float = 2.00
    AUX_CPP: float = 1.55
    AUX_COS_IP_NO: float = 0.30
    AUX_COS_IP_RE: float = 0.30
    AUX_COS_IP_SR: float = 0.30
    AUX_COS_IPO_NO: float = 0.60
    AUX_COS_IPO_RE: float = 0.60
    AUX_COS_IPO_SR: float = 0.60

@dataclass
class FranjasHorarias:
    # HHMM en enteros
    # “Normal” 08:00–21:59, “Reducido” 22:00–01:59, “Super Reducido” 02:00–07:59 (según parche 11/2010)
    HORA_NOR_INI: int = 800
    HORA_NOR_FIN: int = 2159
    HORA_RED_1_INI: int = 2200
    HORA_RED_1_FIN: int = 2359
    HORA_RED_2_INI: int = 0
    HORA_RED_2_FIN: int = 159
    HORA_SRED_INI: int = 200
    HORA_SRED_FIN: int = 759

@dataclass
class RangosEspeciales:
    # Prefijos locales
    LOCALES_2: tuple = tuple(str(x) for x in range(22, 30)) 
    # Operadores/servicios especiales locales
    ESPECIALES_3: tuple = (
        '104','107','211','212','213','214','215','216','217','218','219',
        '250','251','252','255','256','258','259','290','291','292','293','294','295','298'
    )
    # Valor agregado
    VALOR_AGREGADO_9: tuple = ('900161616','900162010',)
    # Rango IP 
    IP_8_PREF: str = '50'  # destinos que empiezan en '50'
    IP_RANGO_MIN: int = 50320000
    IP_RANGO_MAX: int = 50399999

    # Rurales 
    RURALES: tuple = (
        (2895000,2897499),(2133000,2139999),(8249000,8249999),(8257000,8257999),
        (8399000,8399999),(8629000,8629999),(8710000,8714999),(8720000,8721999),
        (8731000,8731999),(8740000,8740999),(8750000,8750999),(8780000,8799999),
        (8702000,8702999),(2755000,2757499),(2875000,2877499)
    )

    # Telef. públicos 
    PUBLICOS: tuple = (
        (2100000,2109999),(2130000,2132999),(2160000,2164999),(2190000,2199999),
        (2320000,2329999),(2340000,2349999),(2510000,2514999),(2520000,2527999),
        (2540000,2549999),(2560000,2560999),(2573000,2574999),(2580000,2599999),
        (2950000,2969999),(2877500,2879999),(2980000,2989999),
        (8200000,8200499),(8212000,8213499),(8214000,8214199),
        (8216000,8217499),(8218000,8218499),(8219000,8219199),
        (8234000,8239999),(8247000,8248999),(8350300,8350499),
        (8397000,8398999),(8627000,8628999),(2757500,2759999),(2897500,2899999)
    )

IGNORAR_FACTURACION = {2813211,2899998,2899999}  # no facturar
