import requests
import pymongo
from datetime import datetime
from config import API_TOKEN, PLAYER_TAGS, MONGO_URI, DB_NAME

# Conexão com MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
battles_collection = db["battles"]
players_collection = db["players"]

# Headers da API
headers = {
    "Authorization": API_TOKEN
}

# Loop pelas TAGs dos jogadores
for tag in PLAYER_TAGS:
    encoded_tag = tag.replace("#", "%23")
    url = f"https://api.clashroyale.com/v1/players/{encoded_tag}/battlelog"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        battlelog = response.json()

        for battle in battlelog:
            # Salva os dados do jogador principal (você)
            player = battle.get("team", [{}])[0]
            player_info = {
                "name": player.get("name"),
                "tag": tag,
                "trophies": player.get("startingTrophies"),
                "expLevel": player.get("expLevel")
            }
            players_collection.update_one({"tag": tag}, {"$set": player_info}, upsert=True)

            # Converter battleTime string → datetime
            battle_time_str = battle.get("battleTime")
            battle_time_dt = datetime.strptime(battle_time_str, "%Y%m%dT%H%M%S.%fZ")

            # Dados principais da batalha
            battle_data = {
                "utcTime": battle_time_dt,
                "type": battle.get("type"),
                "team": battle.get("team"),
                "opponent": battle.get("opponent"),
                "is_winner": player.get("crowns", 0) > battle.get("opponent", [{}])[0].get("crowns", 0)
            }

            # Evita duplicatas pelo timestamp + nome do jogador
            key = {
                "utcTime": battle_data["utcTime"],
                "team.name": player_info["name"]
            }

            battles_collection.update_one(key, {"$set": battle_data}, upsert=True)

        print(f"Batalhas de {tag} importadas com sucesso.")
    else:
        print(f"Erro ao acessar API para o jogador {tag}: {response.status_code} - {response.text}")
