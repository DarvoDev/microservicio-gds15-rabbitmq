# intermediario.py  ─── Infraestructura de mensajería GDS-15
#
# Este script NO contiene lógica de negocio ni de interfaz. Su único trabajo
# es levantar el "intermediario" entre interfaz.py y service.py:
#
#   interfaz.py  ──publish──>  [ EXCHANGE_SOLICITUD (direct) ]  ──routing_key──>  cola gds15.solicitud  ──consume──>  service.py
#                                                                                                                          │
#                                                                                                            publica evento│
#                                                                                                                          v
#                                                                              [ EXCHANGE_EVENTOS (topic) ]  <── otros posibles suscriptores
#
# Se ejecuta UNA vez para dejar creada la topología (exchanges + cola + binding).
# service.py e interfaz.py ya NO declaran nada de esto; solo lo usan.
#
# Variables de entorno (configurables):
#   RABBIT_HOST         default: localhost
#   EXCHANGE_SOLICITUD  default: geriatricos.solicitudes
#   ROUTING_KEY_GDS15   default: gds15
#   COLA_SOLICITUD      default: gds15.solicitud
#   EXCHANGE_EVENTOS    default: geriatricos.eventos

import os
import pika

RABBIT_HOST        = os.getenv("RABBIT_HOST",        "localhost")

EXCHANGE_SOLICITUD  = os.getenv("EXCHANGE_SOLICITUD",  "geriatricos.solicitudes")
ROUTING_KEY_GDS15   = os.getenv("ROUTING_KEY_GDS15",   "gds15")
COLA_SOLICITUD      = os.getenv("COLA_SOLICITUD",      "gds15.solicitud")

EXCHANGE_EVENTOS    = os.getenv("EXCHANGE_EVENTOS",    "geriatricos.eventos")


def declarar_infraestructura(channel):
    # 1) Intermediario + routing para solicitudes (interfaz -> servicio)
    channel.exchange_declare(exchange=EXCHANGE_SOLICITUD, exchange_type="direct", durable=True)
    channel.queue_declare(queue=COLA_SOLICITUD, durable=True)
    channel.queue_bind(queue=COLA_SOLICITUD, exchange=EXCHANGE_SOLICITUD, routing_key=ROUTING_KEY_GDS15)

    # 2) Publish/Subscribe para eventos de negocio (servicio -> quien quiera escuchar)
    channel.exchange_declare(exchange=EXCHANGE_EVENTOS, exchange_type="topic", durable=True)


def main():
    conn    = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel = conn.channel()

    declarar_infraestructura(channel)

    print(f"[✓] Intermediario listo en '{RABBIT_HOST}'")
    print(f"    exchange solicitudes = '{EXCHANGE_SOLICITUD}' (direct)")
    print(f"      -> cola '{COLA_SOLICITUD}' bindeada con routing_key '{ROUTING_KEY_GDS15}'")
    print(f"    exchange eventos     = '{EXCHANGE_EVENTOS}' (topic)")

    conn.close()


if __name__ == "__main__":
    main()