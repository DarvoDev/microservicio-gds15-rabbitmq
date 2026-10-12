# Formato del UID del paciente — Microservicio GDS-15

Este documento describe qué valores acepta el microservicio como `uid` del paciente, cómo se normalizan y cómo cambiar el formato cuando se conozca el de la aplicación general del hospital.

Para saber qué es el `uid` y en qué se diferencia del `id` interno, ver la sección *Identificación del paciente* del [README](./README.md#identificación-del-paciente-uid-e-id).

---

## 1. Regla actual

El `uid` lo asigna la aplicación general del hospital. Mientras no se conozca su formato real, el servicio solo exige unas reglas básicas que evitan errores de captura:

| Regla | Detalle |
|---|---|
| Caracteres permitidos | Letras `A–Z`, dígitos `0–9`, guion `-` y guion bajo `_` |
| Longitud | De 1 a 64 caracteres (tamaño de la columna `pacientes.uid`) |
| Espacios | No se permiten en medio; los de los extremos se quitan |
| Acentos y `ñ` | No se permiten |

Patrón por defecto (expresión regular):

```
^[A-Z0-9_-]{1,64}$
```

---

## 2. Normalización

Antes de validar, el `uid` recibido se normaliza:

1. Se quitan los espacios al inicio y al final.
2. Se convierte a **mayúsculas**.

Por eso `" exp-000123 "`, `"exp-000123"` y `"EXP-000123"` son el mismo paciente y se guardan como `EXP-000123`.

La conversión a mayúsculas es coherente con la base de datos: la columna `pacientes.uid` usa la intercalación `utf8mb4_0900_ai_ci`, que no distingue mayúsculas ni acentos. Aunque no se normalizara, MySQL ya trataría `exp-1` y `EXP-1` como el mismo `uid`; normalizar evita que se vean distintos en pantalla.

> Si algún día la aplicación del hospital usa uids que **solo** se distinguen por mayúsculas (por ejemplo, identificadores en base64), habrá que quitar la conversión a mayúsculas y cambiar la intercalación de la columna a una que sí las distinga (`utf8mb4_bin`).

---

## 3. Ejemplos

| Entrada | Resultado |
|---|---|
| `EXP-000123` | Válido → `EXP-000123` |
| ` exp-000123 ` | Válido → `EXP-000123` |
| `123456` | Válido → `123456` |
| `TMP-9243B31D` | Válido → `TMP-9243B31D` (uid de prueba) |
| `HOSP_2026_0001` | Válido → `HOSP_2026_0001` |
| `EXP 123` | Inválido: espacio en medio |
| `EXP-123.` | Inválido: el punto no está permitido |
| `ÑANDU-1` | Inválido: la `Ñ` no está permitida |
| *(vacío)* | Inválido (en la interfaz, al aplicar un test, genera un uid de prueba) |
| 65 caracteres o más | Inválido: excede el tamaño de la columna |

---

## 4. Dónde se valida

| Componente | Qué hace | ¿Es obligatoria? |
|---|---|---|
| `service.py` (`normalizar_uid`) | Normaliza y valida el `uid` de cada mensaje `aplicar_test` y `consultar_historico`. Si no cumple, responde `status: "error"` y no guarda nada. | **Sí.** Es la validación que manda, porque otros clientes (por ejemplo, la aplicación general) pueden enviar mensajes sin pasar por la interfaz. |
| `interfaz.py` (`leer_uid`) | Revisa el `uid` al capturarlo y lo vuelve a pedir si no cumple, sin esperar la respuesta del servicio. | No. Solo mejora la experiencia de quien captura. |

Respuesta de error del servicio:

```json
{
  "status": "error",
  "error": "uid inválido 'EXP 123': debe cumplir el formato ^[A-Z0-9_-]{1,64}$",
  "correlation_id": "3f6a1e2c-..."
}
```

---

## 5. Cambiar el formato (`UID_PATRON`)

El patrón se lee de la variable de entorno `UID_PATRON`. Cuando se conozca el formato real del hospital, basta con definirla; no hay que tocar código.

- Se evalúa sobre el `uid` **ya normalizado** (en mayúsculas), así que el patrón debe usar letras mayúsculas.
- Debe coincidir con el `uid` completo (se usa `fullmatch`; los `^` y `$` son opcionales pero recomendables).
- Aunque el patrón permita más, el servicio siempre rechaza uids de más de 64 caracteres.
- Debe definirse con el **mismo valor** en `service.py` y en `interfaz.py`. Si difieren, la interfaz podría aceptar un uid que el servicio rechaza (o al revés).

Ejemplos de patrones:

| Formato del hospital | `UID_PATRON` |
|---|---|
| `EXP-` seguido de 6 dígitos (`EXP-000123`) | `^EXP-[0-9]{6}$` |
| Solo números, de 6 a 10 dígitos | `^[0-9]{6,10}$` |
| Prefijo de 3 letras, guion y año + consecutivo (`HGP-2026-00045`) | `^[A-Z]{3}-[0-9]{4}-[0-9]{5}$` |

Uso en PowerShell (en cada terminal donde se ejecute el servicio o la interfaz):

```powershell
$env:UID_PATRON='^EXP-[0-9]{6}$'
python .\service.py
```

macOS / Linux:

```bash
UID_PATRON='^EXP-[0-9]{6}$' python3 service.py
```

> Usa comillas simples para que la terminal no interprete `$` ni `{}`.

### Uids de prueba (`TMP-`)

Si en la interfaz se deja vacío el UID al aplicar un test, se genera uno con la forma `TMP-` + 8 caracteres hexadecimales (por ejemplo, `TMP-9243B31D`). Es solo para pruebas. Si se configura un `UID_PATRON` que no acepte ese formato, los uids de prueba serán rechazados por el servicio.

### Pacientes ya registrados

Cambiar `UID_PATRON` no modifica los pacientes que ya están en la tabla `pacientes`. Si alguno no cumple el nuevo formato, sus datos se conservan, pero ya no se podrá consultar su histórico ni aplicarle tests con ese uid. Antes de cambiar el patrón, revisa los uids existentes con la opción `[3] Listar pacientes` de la interfaz.
