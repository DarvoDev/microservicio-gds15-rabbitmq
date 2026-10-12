# Contrato de mensajes — Microservicio GDS-15

Este documento define el contrato JSON de entrada y salida entre `interfaz.py`, `intermediario.py` y `service.py`, y resume la configuración/documentación embebida en cada archivo.

---

## 1. Arquitectura de mensajería

```
interfaz.py ──publish──▶ [ EXCHANGE_SOLICITUD (direct) ] ──routing_key "gds15"──▶ cola gds15.solicitud ──consume──▶ service.py
                                                                                                                        │
                                                                                                          publica evento│
                                                                                                                        ▼
                                                                          [ EXCHANGE_EVENTOS (topic) ] ◀── otros posibles suscriptores
```

- **`intermediario.py`**: declara toda la topología (exchanges, cola, binding). Se ejecuta **una sola vez**, antes que los demás.
- **`service.py`**: consume `gds15.solicitud`, aplica lógica de negocio, persiste en MySQL y responde por RPC. También publica eventos de negocio en `EXCHANGE_EVENTOS`.
- **`interfaz.py`**: recopila respuestas del usuario, publica la solicitud al exchange y espera la respuesta RPC en una cola exclusiva propia.

**Orden de ejecución obligatorio:**
```bash
python3 intermediario.py   # 1. una sola vez
python3 service.py         # 2. queda escuchando
python3 interfaz.py        # 3. cada vez que un usuario lo use
```

---

## 2. Variables de entorno

### `intermediario.py`
| Variable | Default | Descripción |
|---|---|---|
| `RABBIT_HOST` | `localhost` | Host del broker RabbitMQ |
| `EXCHANGE_SOLICITUD` | `geriatricos.solicitudes` | Exchange `direct` — intermediario de solicitudes |
| `ROUTING_KEY_GDS15` | `gds15` | Routing key propia de este test |
| `COLA_SOLICITUD` | `gds15.solicitud` | Cola donde `service.py` consume |
| `EXCHANGE_EVENTOS` | `geriatricos.eventos` | Exchange `topic` — publish/subscribe de eventos |

### `service.py`
| Variable | Default | Descripción |
|---|---|---|
| `RABBIT_HOST` | `localhost` | Host del broker RabbitMQ |
| `COLA_SOLICITUD` | `gds15.solicitud` | Cola creada por `intermediario.py` |
| `EXCHANGE_EVENTOS` | `geriatricos.eventos` | Exchange creado por `intermediario.py` |
| `DB_HOST` | `localhost` | Host del servidor MySQL |
| `DB_PORT` | `3306` | Puerto de MySQL |
| `DB_USER` | `root` | Usuario de MySQL |
| `DB_PASSWORD` | `""` | Contraseña del usuario de MySQL |
| `DB_NAME` | `gds15` | Nombre de la base de datos (debe existir de antemano) |
| `DB_URL` | *(vacío)* | Si se define, sobreescribe las 5 variables anteriores. Formato: `mysql+pymysql://usuario:clave@host:puerto/nombre_bd?charset=utf8mb4` |

### `interfaz.py`
| Variable | Default | Descripción |
|---|---|---|
| `RABBIT_HOST` | `localhost` | Host del broker RabbitMQ |
| `EXCHANGE_SOLICITUD` | `geriatricos.solicitudes` | Debe coincidir con lo declarado por `intermediario.py` |
| `ROUTING_KEY_GDS15` | `gds15` | Debe coincidir con lo declarado por `intermediario.py` |
| `COLA_RESPUESTAS` | `gds15.resultados.<hostname>` | Cola exclusiva propia de cada instancia de interfaz |

> **Nota:** `EXCHANGE_SOLICITUD` y `ROUTING_KEY_GDS15` deben ser **idénticas** en `intermediario.py` y en `interfaz.py`, o el mensaje nunca llegará a la cola de `service.py`.

---

## 3. Contrato de ENTRADA (interfaz → service, vía RPC)

### Identificación del paciente

El paciente **no se identifica por nombre** (puede haber homónimos). Se manejan dos identificadores:

| Identificador | Dónde vive | Uso |
|---|---|---|
| `uid` | Expediente en la aplicación general del hospital | Es lo que viaja en los mensajes. Único por paciente. |
| `paciente_id` | Tabla local `pacientes` de este servicio (PK interna) | Los resultados se guardan contra este id. El servicio mapea `uid → paciente_id` al recibir cada mensaje. |

El `nombre` se envía en `aplicar_test` solo para mantener la tabla local de pacientes; si cambia en el expediente se actualiza. Nunca se usa para buscar.

Los clientes siempre envían el `uid`, nunca el `paciente_id`: este último es interno del servicio y solo se devuelve como referencia. Ejemplo: el expediente `EXP-000123` puede corresponder al `paciente_id` `5`; quien consulte el histórico lo hace con `EXP-000123`. La explicación completa está en el [README](./README.md#identificación-del-paciente-uid-e-id).

Es el mismo sobre en ambos casos (`aplicar_test` y `consultar_historico`); cambia qué campos son obligatorios.

### Campos comunes del sobre RPC

| Campo | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `tipo` | `string` | Sí | `"aplicar_test"` o `"consultar_historico"` |
| `correlation_id` | `string` (UUID) | Sí, lo agrega `interfaz.py` automáticamente | Identifica la solicitud para casar la respuesta RPC |
| `reply_to` | `string` | Sí, lo agrega `interfaz.py` automáticamente | Cola exclusiva donde `service.py` debe publicar la respuesta |

### 3.1 `tipo = "aplicar_test"`

Campos obligatorios validados por `service.py` → `CAMPOS_OBLIGATORIOS["aplicar_test"]`:
`uid`, `nombre`, `doctor_id`, `fecha_prueba`, `respuestas_bits` (strings vacíos cuentan como faltantes)

```json
{
  "tipo": "aplicar_test",
  "uid": "EXP-000123",
  "nombre": "Juan Pérez López",
  "doctor_id": "string",
  "fecha_prueba": "2026-08-16T10:30:00",
  "respuestas_bits": 22014,
  "total_preguntas": 15,
  "correlation_id": "3f6a1e2c-...",
  "reply_to": "gds15.resultados.equipo-pc01"
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `uid` | `string` (máx. 64) | UID del expediente del paciente en la aplicación general. Si no existe en la tabla local, se da de alta. |
| `nombre` | `string` (máx. 150) | Nombre del paciente. Se guarda/actualiza en la tabla local `pacientes`. |
| `doctor_id` | `string` | Identificador del geriatra que aplica el test (equivalente a `idGeriatra`) |
| `fecha_prueba` | `string` (ISO 8601) | Fecha en que se aplicó el test |
| `respuestas_bits` | `int` | Las 15 respuestas del GDS-15 codificadas en bits (bit `i` = 1 si la respuesta a la pregunta `i+1` fue "SI") |
| `total_preguntas` | `int` | Constante, `15` |

### 3.2 `tipo = "consultar_historico"`

Campo obligatorio: `uid`

```json
{
  "tipo": "consultar_historico",
  "uid": "EXP-000123",
  "fecha_inicio": "2000-01-01",
  "fecha_fin": "2999-12-31",
  "correlation_id": "9b2d4a11-...",
  "reply_to": "gds15.resultados.equipo-pc01"
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `uid` | `string` | UID del expediente del paciente del cual se quiere el histórico. El servicio lo mapea a su `paciente_id` interno. |
| `fecha_inicio` | `string` | Filtro opcional, default `"2000-01-01"` si se omite |
| `fecha_fin` | `string` | Filtro opcional, default `"2999-12-31"` si se omite |

---

## 4. Contrato de SALIDA (service → interfaz, vía RPC)

### 4.1 Respuesta a `aplicar_test`

```json
{
  "status": "ok",
  "id_test": 17,
  "uid": "EXP-000123",
  "paciente_id": 5,
  "nombre": "Juan Pérez López",
  "fecha_prueba": "2026-08-16T10:30:00",
  "puntaje": 4,
  "maximo": 15,
  "nivel": "Normal",
  "descripcion": "0 - 4 puntos. Sin indicios de depresión.",
  "correlation_id": "3f6a1e2c-..."
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `status` | `string` | `"ok"` o `"error"` |
| `id_test` | `int` | ID autogenerado del registro guardado en MySQL |
| `uid` | `string` | Eco del UID del expediente |
| `paciente_id` | `int` | ID interno del paciente en este servicio |
| `nombre` | `string` | Eco del nombre del paciente |
| `fecha_prueba` | `string` | Eco de la fecha recibida |
| `puntaje` | `int` | Puntaje calculado, 0–15 |
| `maximo` | `int` | Constante, `15` |
| `nivel` | `string` | `"Normal"` o `"Presencia de síntomas depresivos"` |
| `descripcion` | `string` | Descripción textual del nivel |
| `correlation_id` | `string` | Mismo valor recibido en la solicitud |

**Árbol de decisión clínico** (documentado en `service.py`):
- `0–4` puntos → Normal
- `5–15` puntos → Presencia de síntomas depresivos

### 4.2 Respuesta a `consultar_historico`

```json
{
  "status": "ok",
  "paciente": {
    "id": 5,
    "uid": "EXP-000123",
    "nombre": "Juan Pérez López"
  },
  "resultados": [
    {
      "id": 12,
      "doctor_id": "string",
      "fecha_prueba": "2026-07-01T09:00:00",
      "puntaje": 6,
      "nivel": "Presencia de síntomas depresivos",
      "descripcion": "5 o más puntos."
    }
  ],
  "correlation_id": "9b2d4a11-..."
}
```

- `paciente`: datos de la tabla local (`id` interno, `uid`, `nombre`). Es `null` si el `uid` nunca ha sido registrado (en ese caso `resultados` viene vacío).
- `resultados`: lista (puede venir vacía) de objetos con las columnas: `id`, `doctor_id`, `fecha_prueba`, `puntaje`, `nivel`, `descripcion`.

### 4.3 Respuesta de error

Se devuelve cuando falta un campo obligatorio, `tipo` es desconocido, o cualquier excepción no controlada:

```json
{
  "status": "error",
  "error": "Campos obligatorios faltantes para 'aplicar_test': ['doctor_id']",
  "correlation_id": "3f6a1e2c-..."
}
```

---

## 5. Contrato de EVENTO (publish/subscribe)

`service.py` publica este evento en `EXCHANGE_EVENTOS` (topic) cada vez que se guarda un resultado exitosamente, **independiente de la respuesta RPC**. Cualquier servicio suscrito con routing key `gds15.*` o `gds15.resultado.*` puede consumirlo sin acoplarse al flujo RPC.

- **Exchange:** `geriatricos.eventos` (topic)
- **Routing key:** `gds15.resultado.guardado`

```json
{
  "evento": "resultado_guardado",
  "test": "gds15",
  "id_test": 17,
  "uid": "EXP-000123",
  "puntaje": 4,
  "nivel": "Normal",
  "fecha_prueba": "2026-08-16T10:30:00",
  "timestamp": "2026-08-16T15:30:00.123456+00:00"
}
```

---

## 6. Persistencia (MySQL)

Tablas `pacientes` y `resultados`, creadas por `init_db()` en `service.py` vía SQLAlchemy (`metadata.create_all`). La **base de datos** (schema) debe existir de antemano; SQLAlchemy solo crea las tablas dentro de ella.

### Tabla `pacientes`

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| `id` | `INTEGER PK AUTO_INCREMENT` | No | ID interno del paciente en este servicio |
| `uid` | `VARCHAR(64) UNIQUE` | No | UID del expediente en la aplicación general |
| `nombre` | `VARCHAR(150)` | No | Nombre del paciente (referencia, se actualiza si cambia) |
| `fecha_registro` | `VARCHAR(40)` | No | Timestamp UTC del alta |
| `fecha_actualizacion` | `VARCHAR(40)` | No | Timestamp UTC de la última vez que se recibió el paciente |

El alta/actualización se hace con `INSERT ... ON DUPLICATE KEY UPDATE` sobre `uid`, así que es atómica y nunca duplica un `uid`.

### Tabla `resultados`

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| `id` | `INTEGER PK AUTO_INCREMENT` | No | ID interno del test |
| `paciente_id` | `INTEGER FK → pacientes.id` | No | Paciente (id interno) |
| `doctor_id` | `VARCHAR(100)` | Sí | Geriatra |
| `fecha_prueba` | `VARCHAR(40)` | No | Fecha clínica de aplicación |
| `respuestas_bits` | `INTEGER` | No | Respuestas codificadas |
| `puntaje` | `INTEGER` | No | Puntaje 0–15 |
| `nivel` | `VARCHAR(60)` | No | Nivel clínico |
| `descripcion` | `TEXT` | No | Descripción del nivel |
| `fecha_registro` | `VARCHAR(40)` | No | Timestamp UTC de inserción en el sistema |

### Migración desde el esquema anterior (por nombre)

Versiones previas guardaban el nombre en `resultados.usuario`. `create_all` no altera tablas existentes, así que si `service.py` detecta la tabla vieja (sin `paciente_id`) se niega a arrancar. Antes de arrancar, renombra (o elimina) la tabla vieja:

```sql
RENAME TABLE resultados TO resultados_legacy;
```

Los registros viejos no tienen `uid`, por lo que no se migran automáticamente.

---

## 7. Codificación de `respuestas_bits`

Constante de referencia en `service.py`:

```python
RESPUESTAS_PUNTUADAS = 0b110_1011_1010_1110
MASCARA = (1 << 15) - 1
```

El puntaje se calcula como `15 - (respuestas_bits XOR RESPUESTAS_PUNTUADAS) & MASCARA` contando bits en 1 (cantidad de respuestas que **no** coinciden con la clave de puntuación se resta de 15).

Cada bit `i` (0 a 14) representa la pregunta `i+1` del GDS-15: `1` si el paciente respondió "SI", `0` si respondió "NO". El bit se marca como acierto (+1 punto) si coincide con la clave esperada de esa pregunta (columna 2 de `PREGUNTAS` en `interfaz.py`: `"SI"` o `"NO"`).