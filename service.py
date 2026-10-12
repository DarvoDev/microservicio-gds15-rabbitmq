# service.py  ─── Servicio GDS-15: solo lógica de negocio + consumo de RabbitMQ
#
# IMPORTANTE: este archivo YA NO declara exchanges ni bindings. Esa parte es
# responsabilidad de intermediario.py, que debe ejecutarse primero (una vez)
# para dejar creada la topología de mensajería. Aquí solo nos conectamos a la
# cola que el intermediario ya dejó lista.
#
# Persistencia: MySQL vía SQLAlchemy (antes SQLite).
#
# Identificación de pacientes: el paciente ya NO se identifica por nombre
# (dos personas pueden llamarse igual). Se usa:
#   - uid           -> identificador del expediente en la aplicación general
#                      del hospital (viene en cada mensaje).
#   - id (interno)  -> PK propia de este servicio en la tabla 'pacientes'.
# Al recibir un uid se mapea a su id interno; los resultados se guardan
# contra ese id interno (resultados.paciente_id).
#
# Variables de entorno (configurables):
#   RABBIT_HOST        default: localhost
#   COLA_SOLICITUD     default: gds15.solicitud            (cola creada por intermediario.py)
#   EXCHANGE_EVENTOS   default: geriatricos.eventos        (exchange creado por intermediario.py)
#   DB_HOST            default: localhost
#   DB_PORT            default: 3306
#   DB_USER            default: root
#   DB_PASSWORD        default: "" (vacío)
#   DB_NAME            default: gds15
#   DB_URL             opcional: si se define, sobreescribe DB_HOST/PORT/USER/PASSWORD/NAME
#                      (ej: mysql+pymysql://usuario:clave@host:3306/gds15?charset=utf8mb4)

import os, json
from datetime import datetime, timezone
import pika
from sqlalchemy import (
    create_engine, MetaData, Table, Column, ForeignKey,
    Integer, String, Text, select, insert, inspect
)
from sqlalchemy.dialects.mysql import insert as mysql_insert

# ── Configuración desacoplada ────────────────────────────────────────────────
RABBIT_HOST        = os.getenv("RABBIT_HOST",        "localhost")

COLA_SOLICITUD      = os.getenv("COLA_SOLICITUD",      "gds15.solicitud")

# Publish/subscribe: exchange topic donde se anuncian eventos de negocio.
# Cualquier otro servicio (auditoría, notificaciones, dashboard) puede
# suscribirse sin que este servicio sepa que existe. Lo declara intermediario.py.
EXCHANGE_EVENTOS    = os.getenv("EXCHANGE_EVENTOS",    "geriatricos.eventos")
EVENTO_GUARDADO_RK  = "gds15.resultado.guardado"

# ── Conexión a MySQL ──────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "3306")
DB_USER     = os.getenv("DB_USER",     "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("DB_NAME",     "gds15")

DB_URL = os.getenv(
    "DB_URL",
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
)

# pool_pre_ping evita el error clásico "MySQL server has gone away" cuando la
# conexión lleva rato inactiva (normal en un servicio que solo persiste cada
# vez que llega un mensaje de RabbitMQ).
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)

metadata = MetaData()

# Tabla local de pacientes: mapea el uid del expediente del hospital a un id
# interno de este servicio. El nombre se guarda solo como referencia y se
# actualiza si cambia en el expediente; NUNCA se usa para buscar.
pacientes = Table(
    "pacientes", metadata,
    Column("id",                  Integer,     primary_key=True, autoincrement=True),
    Column("uid",                 String(64),  nullable=False, unique=True),
    Column("nombre",              String(150), nullable=False),
    Column("fecha_registro",      String(40),  nullable=False),
    Column("fecha_actualizacion", String(40),  nullable=False),
)

resultados = Table(
    "resultados", metadata,
    Column("id",              Integer, primary_key=True, autoincrement=True),
    Column("paciente_id",     Integer,      ForeignKey("pacientes.id"), nullable=False, index=True),
    Column("doctor_id",       String(100)),
    Column("fecha_prueba",    String(40),   nullable=False),
    Column("respuestas_bits", Integer,      nullable=False),
    Column("puntaje",         Integer,      nullable=False),
    Column("nivel",           String(60),   nullable=False),
    Column("descripcion",     Text,         nullable=False),
    Column("fecha_registro",  String(40),   nullable=False),
)

# ── Esquema del mensaje de entrada (documentado) ──────────────────────────────
# {
#   "tipo":            "aplicar_test" | "consultar_historico",
#   "uid":             str   -> uid del expediente del paciente (aplicación general)
#   "nombre":          str   -> nombre del paciente (solo aplicar_test)
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
    "aplicar_test":        ("uid", "nombre", "doctor_id", "fecha_prueba", "respuestas_bits"),
    "consultar_historico": ("uid",),
}

def validar_mensaje(msg: dict):
    tipo = msg.get("tipo", "aplicar_test")
    faltantes = [c for c in CAMPOS_OBLIGATORIOS.get(tipo, ())
                 if c not in msg or (isinstance(msg[c], str) and not msg[c].strip())]
    if faltantes:
        raise ValueError(f"Campos obligatorios faltantes para '{tipo}': {faltantes}")
    return tipo

# ── Lógica de negocio (sin cambios respecto a tu original) ───────────────────
RESPUESTAS_PUNTUADAS = 0b110_1011_1010_1110
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
    """Crea las tablas 'pacientes' y 'resultados' si no existen. Requiere que la
    BASE DE DATOS (schema) ya exista en MySQL — SQLAlchemy crea tablas, no el schema.
    Ver instrucciones para crear la base con CREATE DATABASE."""
    insp = inspect(engine)
    if insp.has_table("resultados"):
        columnas = {c["name"] for c in insp.get_columns("resultados")}
        if "paciente_id" not in columnas:
            # create_all no altera tablas existentes: la tabla vieja (por nombre)
            # quedaría sin paciente_id. Se pide migrar a mano (ver contrato.md).
            raise RuntimeError(
                "La tabla 'resultados' tiene el esquema anterior (columna 'usuario'). "
                "Renómbrala o elimínala antes de arrancar, p. ej.: "
                "RENAME TABLE resultados TO resultados_legacy;"
            )
    metadata.create_all(engine)

def obtener_o_crear_paciente(con, uid, nombre) -> int:
    """Mapea uid del expediente -> id interno. Si el uid no existe lo da de alta;
    si existe y el nombre cambió en el expediente, lo actualiza.
    INSERT ... ON DUPLICATE KEY UPDATE es atómico, evita duplicados por uid."""
    ahora = datetime.now(timezone.utc).isoformat()
    stmt = mysql_insert(pacientes).values(
        uid=uid, nombre=nombre, fecha_registro=ahora, fecha_actualizacion=ahora,
    )
    stmt = stmt.on_duplicate_key_update(
        nombre=stmt.inserted.nombre,
        fecha_actualizacion=stmt.inserted.fecha_actualizacion,
    )
    con.execute(stmt)
    return con.execute(select(pacientes.c.id).where(pacientes.c.uid == uid)).scalar_one()

def buscar_paciente(con, uid):
    """Devuelve {id, uid, nombre} del paciente con ese uid, o None si no existe."""
    fila = con.execute(
        select(pacientes.c.id, pacientes.c.uid, pacientes.c.nombre)
        .where(pacientes.c.uid == uid)
    ).mappings().first()
    return dict(fila) if fila else None

def guardar(uid, nombre, doctor_id, fecha_prueba, respuestas_bits, puntaje, nivel, desc):
    """Devuelve (id_test, paciente_id)."""
    with engine.begin() as con:
        paciente_id = obtener_o_crear_paciente(con, uid, nombre)
        result = con.execute(
            insert(resultados).values(
                paciente_id=paciente_id,
                doctor_id=doctor_id,
                fecha_prueba=fecha_prueba,
                respuestas_bits=respuestas_bits,
                puntaje=puntaje,
                nivel=nivel,
                descripcion=desc,
                fecha_registro=datetime.now(timezone.utc).isoformat(),
            )
        )
        return result.inserted_primary_key[0], paciente_id

def consultar_historico(uid, fecha_inicio, fecha_fin):
    """Mapea uid -> id interno y devuelve (paciente, lista de resultados).
    Si el uid no está registrado devuelve (None, [])."""
    with engine.connect() as con:
        paciente = buscar_paciente(con, uid)
        if paciente is None:
            return None, []
        stmt = (
            select(
                resultados.c.id,
                resultados.c.doctor_id,
                resultados.c.fecha_prueba,
                resultados.c.puntaje,
                resultados.c.nivel,
                resultados.c.descripcion,
            )
            .where(
                resultados.c.paciente_id == paciente["id"],
                resultados.c.fecha_prueba.between(fecha_inicio, fecha_fin),
            )
            .order_by(resultados.c.fecha_prueba)
        )
        filas = con.execute(stmt).mappings().all()
        return paciente, [dict(f) for f in filas]

# ── Publish/Subscribe: anunciar eventos de negocio ────────────────────────────
def publicar_evento(ch, id_test, uid, puntaje, nivel, fecha_prueba):
    """Publica en el exchange topic de eventos. Cualquier servicio suscrito
    a EXCHANGE_EVENTOS con la routing key 'gds15.*' o 'gds15.resultado.*'
    se entera de esto sin que el servicio GDS-15 sepa quién es."""
    evento = {
        "evento":       "resultado_guardado",
        "test":         "gds15",
        "id_test":      id_test,
        "uid":          uid,
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

            uid    = str(msg["uid"]).strip()
            nombre = str(msg["nombre"]).strip()
            id_test, paciente_id = guardar(
                uid, nombre, msg.get("doctor_id"), msg["fecha_prueba"],
                bits, puntaje, nivel, desc
            )

            # Publish/Subscribe: avisar al resto del sistema que hay un
            # resultado nuevo (independiente de la respuesta RPC de abajo).
            publicar_evento(ch, id_test, uid, puntaje, nivel, msg["fecha_prueba"])

            respuesta = {
                "status":         "ok",
                "id_test":        id_test,
                "uid":            uid,
                "paciente_id":    paciente_id,
                "nombre":         nombre,
                "fecha_prueba":   msg["fecha_prueba"],
                "puntaje":        puntaje,
                "maximo":         15,
                "nivel":          nivel,
                "descripcion":    desc,
                "correlation_id": props.correlation_id,
            }

        elif tipo == "consultar_historico":
            paciente, historico = consultar_historico(
                str(msg["uid"]).strip(),
                msg.get("fecha_inicio", "2000-01-01"),
                msg.get("fecha_fin",    "2999-12-31"),
            )
            respuesta = {
                "status":         "ok",
                "paciente":       paciente,
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
    try:
        init_db()
    except Exception as e:
        print(f"[!] No se pudo conectar/crear las tablas en MySQL: {e}")
        print(f"    Revisa DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME y que la base '{DB_NAME}' exista.")
        return

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
    print(f"    BD: MySQL '{DB_NAME}' en {DB_HOST}:{DB_PORT} (usuario '{DB_USER}')")
    channel.start_consuming()

if __name__ == "__main__":
    main()