from pymongo import MongoClient
from datetime import datetime
from config import MONGO_URI, DB_NAME

# Parâmetros da consulta
carta = "Rocket"
inicio = datetime(2025, 4, 1)
fim = datetime(2025, 4, 30)

# Conectar ao banco
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
battles = db["battles"]

# Pipeline de agregação
pipeline = [
    {
        "$match": {
            "utcTime": {"$gte": inicio, "$lte": fim},
            "$or": [
                {"team.cards.name": carta},
                {"opponent.cards.name": carta}
            ]
        }
    },
    {
        "$group": {
            "_id": "$is_winner",
            "total": {"$sum": 1}
        }
    }
]

# Execução
result = list(battles.aggregate(pipeline))

# Processar resultados
vitórias = sum(doc["total"] for doc in result if doc["_id"] == True)
derrotas = sum(doc["total"] for doc in result if doc["_id"] == False)
total = vitórias + derrotas

print(f"Carta: {carta}")
print(f"Período: {inicio.date()} até {fim.date()}")

if total == 0:
    print("Nenhuma batalha encontrada com essa carta no intervalo fornecido.")
else:
    print(f"Total de batalhas: {total}")
    print(f"Vitórias: {vitórias} ({(vitórias/total)*100:.2f}%)")
    print(f"Derrotas: {derrotas} ({(derrotas/total)*100:.2f}%)")
