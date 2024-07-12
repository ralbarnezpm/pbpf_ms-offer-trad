from ..utils import to_number_format
from datetime import datetime
from pytz import timezone

phases = {
    "INICIAL": {
        1: [2, 3], #"En edición":["Descartada", "Aprobada"]
        2: [], # "Descartada":["Descartada"]
        3: [4], # "Aprobada":["Aprobada/colaborativa"]
    },
    "COLABORATIVA": {
        4: [5, 6], # "En edición":["En revisión", "Descartada"]
        5: [7, 8, 9], # "En revisión":["Condicional", "Rechazada", "Aprobada"]
        6: [], #"Descartada":[]
        7: [5], # "Condicional":["Aprobada"]
        8: [], # "Rechazada": [8]
        9: [10], #"Aprobada": ["En negociación/negociacion"]
    },
    "NEGOCIACIÓN": {
        10: [11, 12, 13], # "En negociación": ["Sin acuerdo", "Condicional", "Confirmada"]
        11: [], # "Sin acuerdo": []
        12: [11, 13], # "Condicional":["Aprobada"]
        13: [14], # "Confirmada":["Confirmada/ejecucion"]
    },
    "EJECUCIÓN": {
        14: [15, 16, 17], # "Confirmada": ["Condicional", "Cancelada Cliente", "Cancelada PF", "Ejecutada"]
        15: [16, 17, 14], #"Condicional": ["Cancelada Cliente", "Cancelada PF", "Confirmada"]
        16: [], # "Cancelada Cliente": []
        17: [], # "Cancelada PF": []
        18: [], # "Ejecutada":[]
    },
}

MONTH_TO_NUMBER = {
    'Enero' : 1,         
    'Febrero' : 2,         
    'Marzo' : 3,           
    'Abril' : 4,              
    'Mayo' : 5, 
    'Junio' : 6,
    'Julio' : 7, 
    'Agosto' : 8, 
    'Septiembre' : 9, 
    'Octubre' : 10, 
    'Noviembre' : 11, 
    'Diciembre' : 12
}

def month_year_promo_str(month_promotion_str, year_promotion_str):
    """ Returns a string with the format YYYY-MM"""
    month_promotion = MONTH_TO_NUMBER[month_promotion_str]

    if month_promotion<10:
        month_str = f"0{month_promotion}"
    else:
        month_str = month_promotion
    year_month = f"{year_promotion_str}-{month_str}"
    return year_month

def pull_timestamp():
    return datetime.now(timezone('Chile/Continental')).strftime("%Y-%m-%d %H:%M:%S")

def datetime_now():
    return datetime.now(timezone('Chile/Continental'))