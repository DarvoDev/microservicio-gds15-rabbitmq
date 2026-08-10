# service.py  ─── Servicio GDS-15 con RabbitMQ + historial SQLite
#
# Variables de entorno (configurables):
#   RABBIT_HOST      default: localhost
#   COLA_SOLICITUD   default: gds15.solicitud
#   DB_PATH          default: gds15.db

import os, json, sqlite3, uuid
from datetime import datetime, timezone
import pika

# ── Configuración desacoplada ────────────────────────────────────────────────
RABBIT_HOST    = os.getenv("RABBIT_HOST",    "localhost")
COLA_SOLICITUD = os.getenv("COLA_SOLICITUD", "gds15.solicitud")
DB_PATH        = os.getenv("DB_PATH",        "gds15.db")

# ── Lógica de negocio (sin cambios respecto a tu original) ───────────────────
RESPUESTAS_PUNTUADAS = 0b110_1011_1011_1110
MASCARA = (1 << 15) - 1

def calcular_puntaje(bits: int) -> int:
    no_coinciden = (bits ^ RESPUESTAS_PUNTUADAS) & MASCARA
    return 15 - no_coinciden.bit_count()

def interpretar(puntaje: int):
    if puntaje <= 4:
        return "Normal", "0 - 4 puntos. Sin indicios de depresión."
    return "Presencia de síntomas depresivos", "5 o más puntos."

# ── Base de datos ─────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS resultados (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario         TEXT    NOT NULL,
            doctor_id       TEXT,
            fecha_prueba    TEXT    NOT NULL,
            respuestas_bits INTEGER NOT NULL,
            puntaje         INTEGER NOT NULL,
            nivel           TEXT    NOT NULL,
            descripcion     TEXT    NOT NULL,
            fecha_registro  TEXT    NOT NULL
        )
    """)
    con.commit()
    con.close()

def guardar(usuario, doctor_id, fecha_prueba, respuestas_bits, puntaje, nivel, desc):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO resultados
          (usuario, doctor_id, fecha_prueba, respuestas_bits,
           puntaje, nivel, descripcion, fecha_registro)
        VALUES (?,?,?,?,?,?,?,?)
    """, (usuario, doctor_id, fecha_prueba, respuestas_bits,
          puntaje, nivel, desc, datetime.now(timezone.utc).isoformat()))
    con.commit()
    con.close()

def consultar_historico(usuario, fecha_inicio, fecha_fin) -> list:
    con = sqlite3.connect(DB_PATH)
    cur = con.execute("""
    SELECT id, usuario, doctor_id, fecha_prueba, puntaje, nivel, descripcion
          FROM resultados
         WHERE usuario = ?
           AND fecha_prueba BETWEEN ? AND ?
         ORDER BY fecha_prueba
    """, (usuario, fecha_inicio, fecha_fin))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    con.close()
    return rows

# ── Handlers de mensajes ─────────────────────────────────────────────────────
def on_mensaje(ch, method, props, body):
    try:
        msg  = json.loads(body)
        tipo = msg.get("tipo", "aplicar_test")

        if tipo == "aplicar_test":
            bits    = int(msg["respuestas_bits"])
            puntaje = calcular_puntaje(bits)
            nivel, desc = interpretar(puntaje)

            guardar(
                msg["usuario"], msg.get("doctor_id"), msg["fecha_prueba"],
                bits, puntaje, nivel, desc
            )
            respuesta = {
                "status":         "ok",
                "usuario":        msg["usuario"],
                "fecha_prueba":   msg["fecha_prueba"],
                "puntaje":        puntaje,
                "maximo":         15,
                "nivel":          nivel,
                "descripcion":    desc,
                "correlation_id": props.correlation_id,
            }

        elif tipo == "consultar_historico":
            historico = consultar_historico(
                msg["usuario"],
                msg.get("fecha_inicio", "2000-01-01"),
                msg.get("fecha_fin",    "2999-12-31"),
            )
            respuesta = {
                "status":         "ok",
                "resultados":     historico,
                "correlation_id": props.correlation_id,
            }

        else:
            respuesta = {"status": "error", "error": f"tipo desconocido: {tipo}"}

    except Exception as e:
        respuesta = {"status": "error", "error": str(e)}

    # Publicar respuesta en la cola reply_to
    if props.reply_to:
        ch.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id
            ),
            body=json.dumps(respuesta, ensure_ascii=False),
        )

    ch.basic_ack(delivery_tag=method.delivery_tag)

# ── Arranque ──────────────────────────────────────────────────────────────────
def main():
    init_db()
    conn    = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel = conn.channel()
    channel.queue_declare(queue=COLA_SOLICITUD, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=COLA_SOLICITUD, on_message_callback=on_mensaje)
    print(f"[✓] GDS-15 service escuchando en '{RABBIT_HOST}' → cola '{COLA_SOLICITUD}'")
    channel.start_consuming()

if __name__ == "__main__":
    main()