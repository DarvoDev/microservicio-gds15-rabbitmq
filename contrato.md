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
- **`service.py`**: consume `gds15.solicitud`, aplica lógica de negocio, persiste en SQLite y responde por RPC. También publica eventos de negocio en `EXCHANGE_EVENTOS`.
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
| `DB_PATH` | `gds15.db` | Ruta del archivo SQLite |

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

Es el mismo sobre en ambos casos (`aplicar_test` y `consultar_historico`); cambia qué campos son obligatorios.

### Campos comunes del sobre RPC

| Campo | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `tipo` | `string` | Sí | `"aplicar_test"` o `"consultar_historico"` |
| `correlation_id` | `string` (UUID) | Sí, lo agrega `interfaz.py` automáticamente | Identifica la solicitud para casar la respuesta RPC |
| `reply_to` | `string` | Sí, lo agrega `interfaz.py` automáticamente | Cola exclusiva donde `service.py` debe publicar la respuesta |

### 3.1 `tipo = "aplicar_test"`

Campos obligatorios validados por `service.py` → `CAMPOS_OBLIGATORIOS["aplicar_test"]`:
`usuario`, `doctor_id`, `fecha_prueba`, `respuestas_bits`

```json
{
  "tipo": "aplicar_test",
  "usuario": "string",
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
| `usuario` | `string` | Identificador del paciente (equivalente a `idPaciente` en el resto del proyecto) |
| `doctor_id` | `string` | Identificador del geriatra que aplica el test (equivalente a `idGeriatra`) |
| `fecha_prueba` | `string` (ISO 8601) | Fecha en que se aplicó el test |
| `respuestas_bits` | `int` | Las 15 respuestas del GDS-15 codificadas en bits (bit `i` = 1 si la respuesta a la pregunta `i+1` fue "SI") |
| `total_preguntas` | `int` | Constante, `15` |

### 3.2 `tipo = "consultar_historico"`

Campo obligatorio: `usuario`

```json
{
  "tipo": "consultar_historico",
  "usuario": "string",
  "fecha_inicio": "2000-01-01",
  "fecha_fin": "2999-12-31",
  "correlation_id": "9b2d4a11-...",
  "reply_to": "gds15.resultados.equipo-pc01"
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `usuario` | `string` | Paciente del cual se quiere el histórico |
| `fecha_inicio` | `string` | Filtro opcional, default `"2000-01-01"` si se omite |
| `fecha_fin` | `string` | Filtro opcional, default `"2999-12-31"` si se omite |

---

## 4. Contrato de SALIDA (service → interfaz, vía RPC)

### 4.1 Respuesta a `aplicar_test`

```json
{
  "status": "ok",
  "id_test": 17,
  "usuario": "string",
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
| `id_test` | `int` | ID autogenerado del registro guardado en SQLite (`lastrowid`) |
| `usuario` | `string` | Eco del paciente evaluado |
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
  "resultados": [
    {
      "id": 12,
      "usuario": "string",
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

`resultados` es una lista (puede venir vacía) de objetos con las columnas: `id`, `usuario`, `doctor_id`, `fecha_prueba`, `puntaje`, `nivel`, `descripcion`.

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
  "usuario": "string",
  "puntaje": 4,
  "nivel": "Normal",
  "fecha_prueba": "2026-08-16T10:30:00",
  "timestamp": "2026-08-16T15:30:00.123456+00:00"
}
```

---

## 6. Persistencia (SQLite)

Tabla `resultados`, creada por `init_db()` en `service.py`:

| Columna | Tipo | Nulo | Descripción |
|---|---|---|---|
| `id` | `INTEGER PK AUTOINCREMENT` | No | ID interno |
| `usuario` | `TEXT` | No | Paciente |
| `doctor_id` | `TEXT` | Sí | Geriatra |
| `fecha_prueba` | `TEXT` | No | Fecha clínica de aplicación |
| `respuestas_bits` | `INTEGER` | No | Respuestas codificadas |
| `puntaje` | `INTEGER` | No | Puntaje 0–15 |
| `nivel` | `TEXT` | No | Nivel clínico |
| `descripcion` | `TEXT` | No | Descripción del nivel |
| `fecha_registro` | `TEXT` | No | Timestamp UTC de inserción en el sistema |

---

## 7. Codificación de `respuestas_bits`

Constante de referencia en `service.py`:

```python
RESPUESTAS_PUNTUADAS = 0b110_1011_1011_1110
MASCARA = (1 << 15) - 1
```

El puntaje se calcula como `15 - (respuestas_bits XOR RESPUESTAS_PUNTUADAS) & MASCARA` contando bits en 1 (cantidad de respuestas que **no** coinciden con la clave de puntuación se resta de 15).

Cada bit `i` (0 a 14) representa la pregunta `i+1` del GDS-15: `1` si el paciente respondió "SI", `0` si respondió "NO". El bit se marca como acierto (+1 punto) si coincide con la clave esperada de esa pregunta (columna 2 de `PREGUNTAS` en `interfaz.py`: `"SI"` o `"NO"`).