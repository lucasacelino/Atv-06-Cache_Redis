from flask import Flask, jsonify
import os
import json
import psycopg2
import redis

app = Flask(__name__)

redis = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)), db=0)
CACHE_KEY = "users:all"

@app.get("/")
def index():
    try:
        conn = getConnectionDB()
        return "Conectado ao banco com sucesso!"
    except Exception as e:
        return f"Erro ao conectar: {e}"

from datetime import datetime, date

def json_serializer(obj):
    """Serializa objetos que não são serializáveis por padrão"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable") 

def getConnectionDB():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        # cursor_factory=RealDictCursor
    )
    
    return conn

@app.get("/sobreviventes")
def getTodasMusicasB():
    
    # conection = getConnectionDB()
    
    # try:
    #     cacheaTodasMusicas = redis.get(CACHE_KEY)
    #     if(cacheaTodasMusicas):
    #         return jsonify(json.loads(cacheaTodasMusicas))
        
    #     conection = getConnectionDB()
    #     cursor = conection.cursor()
    #     cursor.execute("SELECT * FROM spotifySongs")
    #     musicas = cursor.fetchall()
        
    #     redis.setex(CACHE_KEY, 120, json.dumps(musicas))
        
    #     conection.close()
        
    #     return {"músicas": musicas}
    # try:
    #     # Verifica cache primeiro
    #     cached_musicas = redis.get(CACHE_KEY)
    #     if cached_musicas:
    #         return jsonify(json.loads(cached_musicas))
        
    #     # Se não tem cache, busca no banco
    #     connection = getConnectionDB()
    #     cursor = connection.cursor()
        
    #     cursor.execute("SELECT * FROM spotifySongs")
    #     columns = [desc[0] for desc in cursor.description]  # Pega os nomes das colunas
    #     musicas = [dict(zip(columns, row)) for row in cursor.fetchall()]  # Converte para dict
        
    #     # Armazena no cache
    #     redis.setex(CACHE_KEY, 120, json.dumps(musicas))
        
    #     return jsonify({"musicas": musicas})
    
    # except Exception as e:
    #     return jsonify({"erro": str(e)}), 500
    
    connection = None
    try:
        # Verifica cache primeiro
        cached_sobreviventes = redis.get(CACHE_KEY)
        if cached_sobreviventes:
            return jsonify(json.loads(cached_sobreviventes))
        
        # Se não tem cache, busca no banco
        connection = getConnectionDB()
        cursor = connection.cursor()
        
        cursor.execute("SELECT * FROM sobreviventes")
        columns = [desc[0] for desc in cursor.description]
        musicas = [dict(zip(columns, row)) for row in cursor.fetchall()]# Pega os nomes das colunas
        
        # Converte para dict e serializa campos datetime
        
        # Armazena no cache com o serializador personalizado
        redis.setex(CACHE_KEY, 120, json.dumps(musicas))
        
        return jsonify({"musicas": musicas})
    
    except Exception as e:
        return jsonify({"erro": str(e)}), 500
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
