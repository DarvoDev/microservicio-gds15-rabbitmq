# Guía de ejecución — GDS-15 (Windows + Docker)

Guía práctica para levantar el microservicio GDS-15 en Windows usando Docker para RabbitMQ y MySQL.

> Esta guía usa **puertos alternativos** (`3307` para MySQL, `8081` para Adminer) porque en esta máquina los puertos `3306` y `8080` ya están ocupados por otros programas. Si en tu equipo están libres, puedes usar los estándar.

---

## Requisitos

- **Docker Desktop** abierto y con el motor corriendo ("Engine running").
- **Python 3.10+** (probado con 3.13).
- PowerShell, ubicado en la carpeta del proyecto.

Comprobar:

```powershell
docker ps
python --version
```

---

## Primera vez (instalación)

### 1. Levantar RabbitMQ

```powershell
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Verifica en <http://localhost:15672> (usuario `guest`, contraseña `guest`).

### 2. Levantar MySQL (puerto 3307)

```powershell
docker run -d --name mysql-gds15 -e MYSQL_ROOT_PASSWORD=changeme -e MYSQL_DATABASE=gds15 -p 3307:3306 mysql:8.0
```

Espera unos 30 segundos y revisa:

```powershell
docker logs --tail 5 mysql-gds15
```

Debe aparecer `ready for connections ... port: 3306`. Adentro del contenedor el puerto es 3306; `3307` es solo el puerto expuesto hacia Windows. Si ves `port: 0`, MySQL todavía está inicializando: espera y vuelve a revisar.

### 3. Entorno virtual y dependencias

```powershell
python -m venv venv
venv\Scripts\activate
pip install pika sqlalchemy pymysql cryptography
```

### 4. Crear exchanges, cola y binding (una sola vez)

> El archivo se llama **`indermediario.py`** (no `intermediario.py` como dice el README).

```powershell
python .\indermediario.py
```

Salida esperada:

```
[✓] Intermediario listo en 'localhost'
    exchange solicitudes = 'geriatricos.solicitudes' (direct)
      -> cola 'gds15.solicitud' bindeada con routing_key 'gds15'
    exchange eventos     = 'geriatricos.eventos' (topic)
```

### 5. Arrancar el servicio (terminal 1, queda abierta)

```powershell
venv\Scripts\activate
$env:DB_PASSWORD="changeme"; $env:DB_PORT="3307"
python .\service.py
```

Debe indicar `BD: MySQL 'gds15' en localhost:3307` y que está escuchando en `gds15.solicitud`.

### 6. Usar la interfaz (terminal 2)

```powershell
cd C:\Users\gativ\OneDrive\Documentos\SERVICIO\evaluador
venv\Scripts\activate
python .\interfaz.py
```

- `[1] Aplicar test` → responder las 15 preguntas; devuelve puntaje e interpretación.
- `[2] Consultar histórico` → usar el mismo nombre de paciente; debe aparecer el registro recién creado.

---

## Siguientes veces

Los contenedores ya existen, **no** repetir `docker run`:

```powershell
docker start rabbitmq mysql-gds15 adminer
```

Después seguir desde el **paso 5**. El paso 4 no es necesario salvo que se haya borrado el contenedor de RabbitMQ.

Para apagar todo:

```powershell
docker stop rabbitmq mysql-gds15 adminer
```

---

## Ver la base de datos visualmente

### Opción A — Adminer (navegador)

```powershell
docker run -d --name adminer -p 8081:8080 adminer
```

Abrir <http://localhost:8081>:

| Campo    | Valor                        |
|----------|------------------------------|
| System   | `MySQL`                      |
| Server   | `host.docker.internal:3307`  |
| Username | `root`                       |
| Password | `changeme`                   |
| Database | `gds15`                      |

### Opción B — MySQL Workbench / DBeaver / HeidiSQL

| Campo      | Valor       |
|------------|-------------|
| Host       | `127.0.0.1` |
| Puerto     | `3307`      |
| Usuario    | `root`      |
| Contraseña | `changeme`  |
| Base       | `gds15`     |

### Opción C — consola

```powershell
docker exec -it mysql-gds15 mysql -uroot -pchangeme -e "SELECT * FROM gds15.resultados;"
```

> La tabla `resultados` se crea al arrancar `service.py` por primera vez; las filas aparecen al aplicar tests.

---

## Posibles inconvenientes

### `ports are not available ... bind: Only one usage of each socket address`

El puerto ya lo usa otro programa (ej. MySQL nativo de Windows en 3306, Tomcat/Jenkins en 8080). El contenedor queda **creado pero detenido**.

Solución: borrar el contenedor y recrearlo con otro puerto externo.

```powershell
docker rm mysql-gds15
docker run -d --name mysql-gds15 -e MYSQL_ROOT_PASSWORD=changeme -e MYSQL_DATABASE=gds15 -p 3307:3306 mysql:8.0
```

Ver quién ocupa un puerto:

```powershell
netstat -ano | findstr :3306
```

El último número es el PID; buscarlo en Administrador de tareas → Detalles.

### `Conflict. The container name "/xxx" is already in use`

Ya existe un contenedor con ese nombre (aunque esté detenido o haya fallado al arrancar).

```powershell
docker rm xxx        # borrar y volver a crear
# o, si estaba bien configurado:
docker start xxx
```

### `Access denied for user 'root'@'localhost' (using password: YES)`

`service.py` se está conectando al MySQL equivocado (normalmente al nativo de Windows en 3306) o con contraseña incorrecta.

- Falta `$env:DB_PORT="3307"`.
- Falta o está mal `$env:DB_PASSWORD="changeme"`.

Las variables `$env:` **se pierden al cerrar la terminal**; hay que definirlas de nuevo en cada terminal donde se corra `service.py`.

### `Can't connect to MySQL server`

- MySQL aún está inicializando (primer arranque tarda ~30 s). Revisar `docker logs --tail 5 mysql-gds15`.
- El contenedor está detenido: `docker ps` / `docker start mysql-gds15`.

### `docker logs` no muestra nada

El contenedor nunca arrancó (casi siempre por un puerto ocupado). Revisar con `docker ps -a` la columna STATUS.

### Error de conexión a RabbitMQ (`AMQPConnectionError`)

- Contenedor detenido: `docker start rabbitmq`.
- Docker Desktop no está abierto.

### `NOT_FOUND - no queue 'gds15.solicitud'` / `no exchange`

No se ejecutó `indermediario.py` (paso 4), o se recreó el contenedor de RabbitMQ. Correrlo de nuevo.

### `python .\intermediario.py` → archivo no encontrado

El archivo real se llama `indermediario.py`.

### Aparece un cuadro "Inicie sesión para obtener acceso" en `localhost:8080`

No es Adminer: es otro programa que ocupa el 8080. Cancelar y usar Adminer en el 8081 (ver arriba).

### `venv\Scripts\activate` bloqueado por la política de ejecución

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### `ModuleNotFoundError: No module named 'pika'` (u otro)

El entorno virtual no está activo en esa terminal (el prompt debe empezar con `(venv)`). Activarlo o reinstalar dependencias.

### `python` no se reconoce

Usar `py` en su lugar (`py .\service.py`).

### `interfaz.py` se queda esperando respuesta

`service.py` no está corriendo o falló al arrancar. Revisar la terminal 1.

---

## Resumen de comandos (día a día)

```powershell
# Terminal 1
docker start rabbitmq mysql-gds15
cd C:\Users\gativ\OneDrive\Documentos\SERVICIO\evaluador
venv\Scripts\activate
$env:DB_PASSWORD="changeme"; $env:DB_PORT="3307"
python .\service.py

# Terminal 2
cd C:\Users\gativ\OneDrive\Documentos\SERVICIO\evaluador
venv\Scripts\activate
python .\interfaz.py
```
