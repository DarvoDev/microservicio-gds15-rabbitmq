# interfaz.py  ─── Interfaz GDS-15 con RabbitMQ
#
# Variables de entorno (configurables):
#   RABBIT_HOST      default: localhost
#   COLA_SOLICITUD   default: gds15.solicitud
#   COLA_RESPUESTAS  default: gds15.resultados.<hostname>

from socket import gethostname, socket
import os, json, uuid, pika
from datetime import datetime

RABBIT_HOST     = os.getenv("RABBIT_HOST",    "localhost")
COLA_SOLICITUD  = os.getenv("COLA_SOLICITUD", "gds15.solicitud")
COLA_RESPUESTAS = os.getenv("COLA_RESPUESTAS", f"gds15.resultados.{gethostname()}")

PREGUNTAS = (
    ("¿En general, está satisfecho(a) con su vida?",                              "NO"),
    ("¿Ha abandonado muchas de sus tareas habituales y aficiones?",               "SI"),
    ("¿Siente que su vida está vacía?",                                           "SI"),
    ("¿Se siente con frecuencia aburrido(a)?",                                    "SI"),
    ("¿Se encuentra de buen humor la mayor parte del tiempo?",                    "NO"),
    ("¿Teme que algo malo pueda ocurrirle?",                                      "SI"),
    ("¿Se siente feliz la mayor parte del tiempo?",                               "NO"),
    ("¿Con frecuencia se siente desamparado(a), desprotegido(a)?",                "SI"),
    ("¿Prefiere usted quedarse en casa, más que salir y hacer cosas nuevas?",     "SI"),
    ("¿Cree que tiene más problemas de memoria que la mayoría de la gente?",      "SI"),
    ("¿En estos momentos, piensa que es estupendo estar vivo(a)?",                "NO"),
    ("¿Actualmente se siente un(a) inútil?",                                      "SI"),
    ("¿Se siente lleno(a) de energía?",                                           "NO"),
    ("¿Se siente sin esperanza en este momento?",                                 "SI"),
    ("¿Piensa que la mayoría de la gente está en mejor situación que usted?",     "SI"),
)

# ── Entrada de usuario ────────────────────────────────────────────────────────
def leer_respuesta(numero, texto):
    validas_si = ("SI", "SÍ", "S", "1")
    validas_no = ("NO", "N", "0")
    while True:
        entrada = input("[{:02d}/15] {}\n         (SI/NO): ".format(numero, texto)).strip().upper()
        if entrada in validas_si: return "SI"
        if entrada in validas_no: return "NO"
        print("         Respuesta no válida. Ingrese SI o NO.\n")

def recopilar_respuestas():
    return [leer_respuesta(i + 1, PREGUNTAS[i][0]) for i in range(len(PREGUNTAS))]

def codificar(respuestas):
    bits = 0
    for i, r in enumerate(respuestas):
        if r == "SI":
            bits |= (1 << i)
    return bits

# ── Comunicación con RabbitMQ ─────────────────────────────────────────────────
def enviar_y_esperar(mensaje: dict) -> dict:
    """Publica en gds15.solicitud y espera respuesta en reply_to (RPC síncrono)."""
    correlation_id = str(uuid.uuid4())
    mensaje["correlation_id"] = correlation_id
    mensaje["reply_to"]       = COLA_RESPUESTAS

    conn    = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel = conn.channel()
    channel.queue_declare(queue=COLA_SOLICITUD,  durable=True)
    channel.queue_declare(queue=COLA_RESPUESTAS, exclusive=True)

    respuesta_recibida = {}

    def on_respuesta(ch, method, props, body):
        if props.correlation_id == correlation_id:
            respuesta_recibida.update(json.loads(body))
            ch.stop_consuming()

    channel.basic_consume(queue=COLA_RESPUESTAS, on_message_callback=on_respuesta, auto_ack=True)
    channel.basic_publish(
        exchange="",
        routing_key=COLA_SOLICITUD,
        properties=pika.BasicProperties(
            reply_to=COLA_RESPUESTAS,
            correlation_id=correlation_id,
            delivery_mode=2,
        ),
        body=json.dumps(mensaje, ensure_ascii=False),
    )
    print("  [→] Mensaje enviado. Esperando respuesta...")
    channel.start_consuming()
    conn.close()
    return respuesta_recibida

# ── Modos de operación ────────────────────────────────────────────────────────
def modo_test():
    print("\n" + "=" * 62)
    print("  GDS-15 ── Escala de Depresión Geriátrica")
    print("  Responda según como se ha sentido la ÚLTIMA SEMANA")
    print("=" * 62 + "\n")

    usuario   = input("  Usuario   : ").strip()
    doctor_id = input("  Doctor ID : ").strip()
    print()

    respuestas = recopilar_respuestas()
    bits       = codificar(respuestas)

    resultado = enviar_y_esperar({
        "tipo":            "aplicar_test",
        "usuario":         usuario,
        "doctor_id":       doctor_id,
        "fecha_prueba":    datetime.now().isoformat(timespec="seconds"),
        "respuestas_bits": bits,
        "total_preguntas": 15,
    })
    mostrar_resultado(respuestas, bits, resultado)

def modo_historico():
    usuario = input("  Usuario: ").strip()

    # Pide todos los registros sin filtro de fecha
    resultado = enviar_y_esperar({
        "tipo":         "consultar_historico",
        "usuario":      usuario,
        "fecha_inicio": "2000-01-01",
        "fecha_fin":    "2999-12-31",
    })

    registros = resultado.get("resultados", [])

    if not registros:
        print(f"\n  Sin registros para '{usuario}'.\n")
        return

    # Muestra la lista
    print(f"\n  Resultados de '{usuario}':\n")
    print("  {:>4}  {:^20}  {:>7}  {}".format("ID", "Fecha", "Puntaje", "Nivel"))
    print("  " + "-" * 60)
    for r in registros:
        print("  {:>4}  {:^20}  {:>5}/15  {}".format(
            r["id"], r["fecha_prueba"], r["puntaje"], r["nivel"]))
    print()

    # Selección por ID
    try:
        id_sel = int(input("  Ingrese ID para ver detalle (0 para salir): ").strip())
    except ValueError:
        print("  ID no válido.")
        return

    if id_sel == 0:
        return

    seleccionado = next((r for r in registros if r["id"] == id_sel), None)
    if not seleccionado:
        print("  ID no encontrado.")
        return

    print("\n" + "=" * 62)
    print("  DETALLE  GDS-15")
    print("=" * 62)
    print(f"  Usuario     : {seleccionado['usuario']}")
    print(f"  Doctor      : {seleccionado.get('doctor_id', '-')}")
    print(f"  Fecha       : {seleccionado['fecha_prueba']}")
    print(f"  Puntaje     : {seleccionado['puntaje']} / 15")
    print(f"  Nivel       : {seleccionado['nivel']}")
    print(f"  Descripción : {seleccionado['descripcion']}\n")

# ── Presentación de resultados ────────────────────────────────────────────────
def mostrar_resultado(respuestas, bits, resultado):
    puntaje = resultado["puntaje"]
    print("\n" + "=" * 62)
    print("  RESULTADO  GDS-15")
    print("=" * 62)
    print(f"\n  Puntaje     : {puntaje} / 15")
    print(f"  Nivel       : {resultado['nivel']}")
    print(f"  Descripción : {resultado['descripcion']}")
    print("\n  -- Árbol de decisión --")
    print("   0- 4  {:<14}  Normal".format("<-- RESULTADO" if puntaje <= 4 else ""))
    print("   5-15  {:<14}  Presencia de síntomas depresivos".format("<-- RESULTADO" if puntaje >= 5 else ""))
    print("\n  -- Detalle por ítem --")
    print("  " + "-" * 58)
    for i, (texto, clave) in enumerate(PREGUNTAS):
        resp     = respuestas[i]
        resp_bit  = (bits >> i) & 1
        clave_bit = 1 if clave == "SI" else 0
        marca    = "[+1]" if resp_bit == clave_bit else "[ 0]"
        print(f"  P{i+1:02d} {marca}  {resp}  {texto}")
    print("  " + "-" * 58)
    print(f"  Total: {puntaje} / 15\n")

# ── Menú principal ────────────────────────────────────────────────────────────
def main():
    print("\n  GDS-15  |  Broker: {}  |  Cola entrada: {}".format(RABBIT_HOST, COLA_SOLICITUD))
    print("  [1] Aplicar test")
    print("  [2] Consultar histórico")
    opcion = input("\n  Opción: ").strip()
    if opcion == "1":
        modo_test()
    elif opcion == "2":
        modo_historico()
    else:
        print("  Opción no válida.")

if __name__ == "__main__":
    main()