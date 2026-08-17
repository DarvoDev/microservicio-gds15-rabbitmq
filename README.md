# GDS-15 — Escala de Depresión Geriátrica (microservicio)

Microservicio para aplicar y consultar el histórico de la **Escala de Depresión Geriátrica de Yesavage, versión abreviada (GDS-15)**, un test clínico de 15 preguntas Sí/No usado para detectar síntomas depresivos en adultos mayores.

El sistema forma parte de una arquitectura mayor de microservicios de tests geriátricos (Katz, Lawton, SPPB, MiniCog, GDS-15, etc.), donde cada test es independiente pero comparte el mismo patrón de comunicación por mensajería asíncrona con RabbitMQ.

---

## 1. ¿Qué hace?

- **Aplica el test**: recopila las 15 respuestas del paciente, calcula el puntaje (0–15) y lo clasifica clínicamente (`Normal` o `Presencia de síntomas depresivos`).
- **Guarda el resultado**: usuario, doctor, fecha, respuestas y puntaje quedan persistidos en una base de datos SQLite.
- **Consulta histórico**: permite ver todas las pruebas previas de un paciente y el detalle de cualquiera de ellas.
- **Publica eventos**: cada resultado guardado se anuncia en un exchange de tipo publish/subscribe, para que otros servicios (auditoría, notificaciones, dashboards) puedan reaccionar sin acoplarse al flujo principal.

Toda la comunicación entre las partes se hace de forma **asíncrona vía RabbitMQ**, no por llamadas HTTP directas.

---

## 2. Arquitectura

El sistema está dividido en tres piezas independientes, cada una en su propio archivo:

```
interfaz.py ──publish──▶ [ EXCHANGE_SOLICITUD (direct) ] ──routing_key "gds15"──▶ cola gds15.solicitud ──consume──▶ service.py
                                                                                                                        │
                                                                                                          publica evento│
                                                                                                                        ▼
                                                                          [ EXCHANGE_EVENTOS (topic) ] ◀── otros posibles suscriptores
```

| Archivo | Rol |
|---|---|
| **`intermediario.py`** | Declara toda la infraestructura de mensajería (exchanges, cola, binding). Se ejecuta **una sola vez** antes que los demás. No contiene lógica de negocio ni de interfaz. |
| **`service.py`** | El microservicio en sí: consume solicitudes, calcula el puntaje, valida el mensaje, persiste en SQLite y responde vía RPC. También publica eventos de negocio. |
| **`interfaz.py`** | Cliente de línea de comandos: hace las preguntas al usuario, arma el mensaje, lo publica y espera la respuesta. |

Para el detalle exacto de los mensajes JSON de entrada/salida y el esquema de la base de datos, ver [`CONTRATO_GDS15.md`](./CONTRATO_GDS15.md).

---

## 3. Requisitos

- **Python 3.11+** (se usa `int.bit_count()`, disponible desde Python 3.10)
- **RabbitMQ** corriendo y accesible (local o remoto)
- Librería **`pika`** (cliente de RabbitMQ para Python)
- SQLite — no requiere instalación aparte, viene incluido en la librería estándar de Python (`sqlite3`)

### Instalar RabbitMQ

**Opción A — Docker (recomendada):**

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

- `5672` → puerto AMQP que usa el código Python
- `15672` → panel web de administración (`http://localhost:15672`, usuario/contraseña por defecto `guest`/`guest`)

Verifica que quedó corriendo:

```bash
docker ps
```

**Opción B — instalación nativa (Ubuntu/Debian):**

```bash
sudo apt update
sudo apt install rabbitmq-server -y
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
```

### Instalar dependencias de Python

```bash
pip install pika --break-system-packages
```

(si usas un entorno virtual, omite la bandera `--break-system-packages`)

---

## 4. Cómo usarlo

El **orden de ejecución importa**: el intermediario debe existir antes de que el servicio o la interfaz intenten usarlo.

```bash
# 1. Levantar la infraestructura de mensajería (una sola vez)
python3 intermediario.py

# 2. Levantar el servicio (queda corriendo, escuchando solicitudes)
python3 service.py

# 3. Usar la interfaz (cada vez que un usuario quiera aplicar o consultar un test)
python3 interfaz.py
```

Si `service.py` o `interfaz.py` se ejecutan antes que `intermediario.py`, fallarán porque la cola/exchange todavía no existen — es intencional, para forzar el orden correcto.

### Flujo típico

1. Corres `intermediario.py` una vez al desplegar el sistema.
2. Dejas `service.py` corriendo en segundo plano (o en un contenedor/servicio del sistema operativo).
3. Cada usuario (doctor/geriatra) ejecuta `interfaz.py` y elige:
   - **`[1] Aplicar test`** → responde las 15 preguntas SI/NO, ingresa el usuario (paciente) y el ID del doctor, y recibe el puntaje e interpretación al instante.
   - **`[2] Consultar histórico`** → ingresa el nombre del paciente y ve la lista de pruebas previas, con opción de ver el detalle de una en particular.

---

## 5. Configuración (variables de entorno)

Todas las variables tienen un valor por defecto pensado para correr todo en `localhost`. Para un despliegue distribuido, sobreescríbelas según corresponda:

| Variable | Usada en | Default | Descripción |
|---|---|---|---|
| `RABBIT_HOST` | los 3 archivos | `localhost` | Host del broker RabbitMQ |
| `EXCHANGE_SOLICITUD` | `intermediario.py`, `interfaz.py` | `geriatricos.solicitudes` | Exchange `direct`, intermediario de solicitudes |
| `ROUTING_KEY_GDS15` | `intermediario.py`, `interfaz.py` | `gds15` | Routing key propia de este test |
| `COLA_SOLICITUD` | `intermediario.py`, `service.py` | `gds15.solicitud` | Cola donde `service.py` consume |
| `EXCHANGE_EVENTOS` | `intermediario.py`, `service.py` | `geriatricos.eventos` | Exchange `topic` para publish/subscribe de eventos |
| `COLA_RESPUESTAS` | `interfaz.py` | `gds15.resultados.<hostname>` | Cola exclusiva de respuestas de cada instancia de la interfaz |
| `DB_PATH` | `service.py` | `gds15.db` | Ruta del archivo SQLite donde se guardan los resultados |

Ejemplo de uso con variables personalizadas:

```bash
RABBIT_HOST=192.168.1.50 DB_PATH=/datos/gds15.db python3 service.py
```

> **Importante:** `EXCHANGE_SOLICITUD` y `ROUTING_KEY_GDS15` deben tener el **mismo valor** en `intermediario.py` y en `interfaz.py`, o los mensajes nunca llegarán a `service.py`.

---

## 6. Estructura de archivos

```
.
├── intermediario.py     # Declara exchanges, cola y binding (correr primero)
├── service.py            # Lógica de negocio + persistencia + RPC
├── interfaz.py            # Cliente de línea de comandos
├── CONTRATO_GDS15.md     # Contrato JSON detallado de entrada/salida/eventos
├── gds15.db               # Base de datos SQLite (se crea automáticamente)
└── README.md               # Este archivo
```

---

## 7. Verificar que todo funciona

1. Con RabbitMQ corriendo, entra a `http://localhost:15672` y confirma en la pestaña **Exchanges** que existen `geriatricos.solicitudes` y `geriatricos.eventos` después de correr `intermediario.py`.
2. Corre `service.py` en una terminal — debe imprimir que quedó escuchando en la cola `gds15.solicitud`.
3. Corre `interfaz.py` en otra terminal, elige `[1] Aplicar test`, responde las preguntas y confirma que recibes el puntaje.
4. Vuelve a correr `interfaz.py`, elige `[2] Consultar histórico` con el mismo nombre de usuario, y confirma que aparece el registro que acabas de crear.

---

## 8. Notas y próximos pasos

- Actualmente la persistencia es **SQLite**, pensada para desarrollo/pruebas locales. Para integrarse al resto del sistema de microservicios (que usa MySQL), se recomienda migrar a SQLAlchemy + MySQL manteniendo el mismo contrato de mensajes.
- La validación de mensajes es manual (`validar_mensaje` en `service.py`). Si se integra con el resto del proyecto, se recomienda migrar a validación con **Pydantic**, como usan los demás microservicios (Katz, Lawton, SPPB, MiniCog).
- El detalle completo del contrato JSON (campos, tipos, ejemplos) está en [`CONTRATO_GDS15.md`](./CONTRATO_GDS15.md).