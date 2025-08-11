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


def getConnectionDB():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    
    return conn

@app.get("/sobreviventes")
def getTodasMusicasB():
    connection = None
    try:
        cached_sobreviventes = redis.get(CACHE_KEY)
        if cached_sobreviventes:
            return jsonify(json.loads(cached_sobreviventes))
        
        connection = getConnectionDB()
        cursor = connection.cursor()
        
        cursor.execute("SELECT * FROM sobreviventes")
        columns = [desc[0] for desc in cursor.description]
        sobreviventes = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        redis.setex(CACHE_KEY, 120, json.dumps(sobreviventes))
        
        return jsonify(sobreviventes)
    
    except Exception as e:
        return jsonify({"erro": str(e)}), 500
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
