from fastapi import FastAPI, HTTPException, Depends, WebSocket
from sqlalchemy.orm import Session
import redis, pika, json, time

from database import SessionLocal, engine, Base
import models
from sqlalchemy.exc import OperationalError
from fastapi.middleware.cors import CORSMiddleware

# ---------------- ESPERAR POSTGRES ----------------

while True:
    try:
        Base.metadata.create_all(bind=engine)
        print("Conectado a PostgreSQL", flush=True)
        break
    except OperationalError:
        print("Esperando PostgreSQL...", flush=True)
        time.sleep(5)

# ---------------- ESPERAR RABBITMQ ----------------

while True:
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='rabbitmq',
                heartbeat=600
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue='reservas')

        print("Conectado a RabbitMQ", flush=True)
        break

    except Exception as e:
        print(f"Esperando RabbitMQ... {e}", flush=True)
        time.sleep(5)

# ---------------- APP ----------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

clientes_ws = []

# ---------------- DB ----------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------- WEBSOCKET ----------------

@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    clientes_ws.append(ws)

    try:
        while True:
            await ws.receive_text()
    except:
        clientes_ws.remove(ws)

async def notify(msg):
    for c in clientes_ws:
        try:
            await c.send_text(msg)
        except:
            pass

# ---------------- SALAS ----------------

@app.post("/salas")
def crear_sala(sala: dict, db: Session = Depends(get_db)):
    nueva = models.Sala(**sala)

    db.add(nueva)
    db.commit()
    db.refresh(nueva)

    return nueva

@app.get("/salas")
def obtener_salas(db: Session = Depends(get_db)):
    return db.query(models.Sala).all()

@app.delete("/salas/{sala_id}")
def eliminar_sala(sala_id: int, db: Session = Depends(get_db)):

    sala = db.query(models.Sala).filter(models.Sala.id == sala_id).first()

    if not sala:
        raise HTTPException(404, "Sala no encontrada")

    reservas = db.query(models.Reserva).filter(
        models.Reserva.sala_id == sala_id
    ).all()

    for r in reservas:
        clave = f"sala:{r.sala_id}:{r.hora}"

        redis_client.delete(clave)
        db.delete(r)

    db.delete(sala)
    db.commit()

    return {"mensaje": "Habitación eliminada"}

# ---------------- RESERVAS ----------------

@app.post("/reservas")
async def crear_reserva(reserva: dict, db: Session = Depends(get_db)):

    sala = db.query(models.Sala).filter(
        models.Sala.id == reserva["sala_id"]
    ).first()

    if not sala:
        raise HTTPException(404, "Habitación no existe")

    clave = f"sala:{reserva['sala_id']}:{reserva['hora']}"

    if not redis_client.setnx(clave, reserva["usuario"]):
        raise HTTPException(400, "Habitación ya reservada")

    nueva = models.Reserva(**reserva)

    db.add(nueva)
    db.commit()
    db.refresh(nueva)

    channel.basic_publish(
        exchange='',
        routing_key='reservas',
        body=json.dumps(reserva)
    )

    print("Evento enviado a RabbitMQ", flush=True)

    await notify("Nueva reserva creada")

    return nueva

@app.get("/reservas")
def obtener_reservas(db: Session = Depends(get_db)):
    return db.query(models.Reserva).all()

@app.delete("/reservas/{reserva_id}")
async def cancelar_reserva(reserva_id: int, db: Session = Depends(get_db)):

    reserva = db.query(models.Reserva).filter(
        models.Reserva.id == reserva_id
    ).first()

    if not reserva:
        raise HTTPException(404, "Reserva no encontrada")

    clave = f"sala:{reserva.sala_id}:{reserva.hora}"

    redis_client.delete(clave)

    db.delete(reserva)
    db.commit()

    channel.basic_publish(
        exchange='',
        routing_key='reservas',
        body=json.dumps({
            "accion": "cancelada",
            "id": reserva_id
        })
    )

    print("Evento de cancelación enviado", flush=True)

    await notify("Reserva cancelada")

    return {"mensaje": "Reserva cancelada"}
