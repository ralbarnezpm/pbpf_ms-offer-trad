iva=1.19
inf_value=float('inf')

PRODUCTION_URL="https://pfpromobooster-production.pricemaker.io/"

NOTIFICATION_SUMMARY={
    "Movimiento": "Se movió",
    "Comentario": "Comentarios en",
    "Cambios": "Cambios en",
}

MONTHS = {
    'enero': 1,
    'febrero': 2,
    'marzo': 3,
    'abril': 4,
    'mayo': 5,
    'junio': 6,
    'julio': 7,
    'agosto': 8,
    'septiembre': 9,
    'octubre': 10,
    'noviembre': 11,
    'diciembre': 12
}

PHASES = {
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
