# service.py  ─── Servicio GDS-15: solo lógica de negocio + consumo de RabbitMQ
#
# IMPORTANTE: este archivo YA NO declara exchanges ni bindings. Esa parte es
# responsabilidad de intermediario.py, que debe ejecutarse primero (una vez)
# para dejar creada la topología de mensajería. Aquí solo nos conectamos a la
# cola que el intermediario ya dejó lista.
#
# Variables de entorno (configurables):
#   RABBIT_HOST        default: localhost
#   COLA_SOLICITUD     default: gds15.solicitud            (cola creada por intermediario.py)
#   EXCHANGE_EVENTOS   default: geriatricos.eventos        (exchange creado por intermediario.py)
#   DB_PATH            default: gds15.db

import os, json, sqlite3
from datetime import datetime, timezone
import pika

# ── Configuración desacoplada ────────────────────────────────────────────────
RABBIT_HOST        = os.getenv("RABBIT_HOST",        "localhost")

COLA_SOLICITUD      = os.getenv("COLA_SOLICITUD",      "gds15.solicitud")

# Publish/subscribe: exchange topic donde se anuncian eventos de negocio.
# Cualquier otro servicio (auditoría, notificaciones, dashboard) puede
# suscribirse sin que este servicio sepa que existe. Lo declara intermediario.py.
EXCHANGE_EVENTOS    = os.getenv("EXCHANGE_EVENTOS",    "geriatricos.eventos")
EVENTO_GUARDADO_RK  = "gds15.resultado.guardado"

DB_PATH             = os.getenv("DB_PATH",            "gds15.db")

# ── Esquema del mensaje de entrada (documentado) ──────────────────────────────
# {
#   "tipo":            "aplicar_test" | "consultar_historico",
#   "usuario":         str   -> identificador del paciente (idPaciente)
#   "doctor_id":       str   -> identificador del geriatra  (idGeriatra)
#   "fecha_prueba":    str   -> ISO 8601, fecha en que se aplicó el test
#   "respuestas_bits": int   -> 15 respuestas codificadas en bits (solo aplicar_test)
#   "total_preguntas": int   -> 15 (solo aplicar_test)
#   "fecha_inicio":    str   -> filtro opcional (solo consultar_historico)
#   "fecha_fin":       str   -> filtro opcional (solo consultar_historico)
#   "correlation_id":  str   -> generado por la interfaz para el RPC
#   "reply_to":        str   -> cola exclusiva donde la interfaz espera la respuesta
# }
CAMPOS_OBLIGATORIOS = {
    "aplicar_test":        ("usuario", "doctor_id", "fecha_prueba", "respuestas_bits"),
    "consultar_historico": ("usuario",),
}

def validar_mensaje(msg: dict):
    tipo = msg.get("tipo", "aplicar_test")
    faltantes = [c for c in CAMPOS_OBLIGATORIOS.get(tipo, ()) if c not in msg]
    if faltantes:
        raise ValueError(f"Campos obligatorios faltantes para '{tipo}': {faltantes}")
    return tipo

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
    cur = con.execute("""
        INSERT INTO resultados
          (usuario, doctor_id, fecha_prueba, respuestas_bits,
           puntaje, nivel, descripcion, fecha_registro)
        VALUES (?,?,?,?,?,?,?,?)
    """, (usuario, doctor_id, fecha_prueba, respuestas_bits,
          puntaje, nivel, desc, datetime.now(timezone.utc).isoformat()))
    con.commit()
    id_insertado = cur.lastrowid
    con.close()
    return id_insertado

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

# ── Publish/Subscribe: anunciar eventos de negocio ────────────────────────────
def publicar_evento(ch, id_test, usuario, puntaje, nivel, fecha_prueba):
    """Publica en el exchange topic de eventos. Cualquier servicio suscrito
    a EXCHANGE_EVENTOS con la routing key 'gds15.*' o 'gds15.resultado.*'
    se entera de esto sin que el servicio GDS-15 sepa quién es."""
    evento = {
        "evento":       "resultado_guardado",
        "test":         "gds15",
        "id_test":      id_test,
        "usuario":      usuario,
        "puntaje":      puntaje,
        "nivel":        nivel,
        "fecha_prueba": fecha_prueba,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }
    ch.basic_publish(
        exchange=EXCHANGE_EVENTOS,
        routing_key=EVENTO_GUARDADO_RK,
        body=json.dumps(evento, ensure_ascii=False),
    )

# ── Handlers de mensajes ─────────────────────────────────────────────────────
def on_mensaje(ch, method, props, body):
    try:
        msg  = json.loads(body)
        tipo = validar_mensaje(msg)

        if tipo == "aplicar_test":
            bits    = int(msg["respuestas_bits"])
            puntaje = calcular_puntaje(bits)
            nivel, desc = interpretar(puntaje)

            id_test = guardar(
                msg["usuario"], msg.get("doctor_id"), msg["fecha_prueba"],
                bits, puntaje, nivel, desc
            )

            # Publish/Subscribe: avisar al resto del sistema que hay un
            # resultado nuevo (independiente de la respuesta RPC de abajo).
            publicar_evento(ch, id_test, msg["usuario"], puntaje, nivel, msg["fecha_prueba"])

            respuesta = {
                "status":         "ok",
                "id_test":        id_test,
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
        respuesta = {"status": "error", "error": str(e),
                      "correlation_id": getattr(props, "correlation_id", None)}

    # Publicar respuesta RPC en la cola reply_to (esto sigue siendo 1 a 1)
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

    # No se declara nada aquí: se asume que intermediario.py ya creó
    # el exchange, la cola y el binding. Si no se ha ejecutado, esta llamada
    # fallará (queue not found) — eso es intencional, para forzar el orden
    # correcto: 1) intermediario.py  2) service.py  3) interfaz.py
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=COLA_SOLICITUD, on_message_callback=on_mensaje)

    print(f"[✓] GDS-15 service escuchando en '{RABBIT_HOST}', cola '{COLA_SOLICITUD}'")
    print(f"    (publicará eventos en exchange '{EXCHANGE_EVENTOS}' con routing_key '{EVENTO_GUARDADO_RK}')")
    channel.start_consuming()

if __name__ == "__main__":
    main()