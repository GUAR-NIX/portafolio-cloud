# Guía paso a paso del pipeline serverless de sismos

## Requisitos

- Python 3.12, Docker Desktop y PowerShell 7.
- Terraform 1.13 (en el PATH o en `tools\terraform.exe`); no se necesitan AWS CLI ni una cuenta.
- Las únicas credenciales son `test/test` y el proveedor Terraform rechaza endpoints que no sean localhost.

## Ejecución local

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
python descargar_datos.py
$env:PYTHONPATH='src'
python scripts\ejecutar_demo.py
python scripts\generar_figuras.py
pytest -q -m "not integracion"
```

Para simular un flujo continuo contra LocalStack, `scripts\productor.py --cada 5` descarga el feed de la última hora
de USGS cada 5 minutos y lo sube a S3 (`--feed dia|semana` y `--iteraciones N` para acotarlo); un fallo de red se
registra y el ciclo sigue.

Para la integración, abra Docker Desktop y ejecute `scripts\verificar_localstack.ps1`. El script levanta
LocalStack 4.8.1 (edición community, sin token), empaqueta Lambda con Pydantic, aplica Terraform solo contra
`localhost:4566`, corre `pytest -m integracion`, mide con `scripts\medir_integracion.py` (volumen, duplicados,
cuarentena, latencias y arranque en frío) y regenera las figuras. La medición completa tarda unos 4 minutos.
Al terminar:

```powershell
docker compose down -v
```

### Problemas conocidos en Windows

- Si `docker info` falla con `docker-secrets-engine\engine.sock`, cierre Docker Desktop, renombre
  `%LOCALAPPDATA%\docker-secrets-engine` y vuelva a abrirlo.
- LocalStack crea un contenedor por entorno Lambda: necesita montar `/var/run/docker.sock` (ya está en Compose).
- Si un `update_function_configuration` llega con una invocación en curso, LocalStack puede cancelarla
  (`CancelledError`) y perder ese evento; el script de medición espera 5 s tras cada cambio.

## Reproducción histórica

`descargar_datos.py` guarda tres feeds y una consulta FDSN acotada a Ecuador durante 2024. Si el archivo
ya existe, usa la copia local. `--forzar` reemplaza deliberadamente la caché. Cada descarga se escribe primero
con sufijo `.parcial`, se valida como JSON y luego se renombra, de modo que un apagado no deja un archivo válido
a medias.

## Preguntas de entrevista

### 1. ¿Por qué la clave de DynamoDB es el id del USGS?

Porque el identificador del evento es estable y permite una escritura condicional atómica. Dos entregas del
mismo archivo compiten por la misma clave y solo una puede insertar.

### 2. ¿Qué entrega ofrece S3 a Lambda?

Al menos una vez. Puede haber duplicados y no se debe asumir orden, por eso la idempotencia vive en la capa de
persistencia y no en el productor.

### 3. ¿Por qué separar validación de los adaptadores AWS?

La regla de negocio se prueba sin red ni emulador. Los adaptadores quedan pequeños y las pruebas de integración
cubren el cableado real.

### 4. ¿Qué hace la escritura condicional?

`attribute_not_exists(id_evento)` convierte la inserción en una operación compare-and-set. Un duplicado produce
`ConditionalCheckFailedException`, que se registra como duplicado esperado.

### 5. ¿Por qué una cuarentena en S3?

Conserva los errores estructurados sin bloquear los eventos válidos del mismo lote y permite reprocesar después
de corregir el productor.

### 6. ¿Cómo se evita desplegar accidentalmente en AWS?

El proveedor usa credenciales ficticias, omite consultas de cuenta y valida que el endpoint sea localhost. La
guía nunca pide credenciales reales.

### 7. ¿LocalStack reemplaza una prueba en AWS?

No. Comprueba contratos y flujo local (aquí encontró un endpoint mal inyectado y una cuarentena que no se
podía serializar), pero no reproduce límites, IAM completo, latencia ni fallos regionales. Antes de producción
haría falta un entorno aislado de AWS con presupuesto y políticas.

### 8. ¿Por qué SNS y no correo directo?

SNS desacopla detección y consumidores. Se pueden agregar correo, SQS o webhooks sin cambiar la Lambda de
ingesta.

### 9. ¿Cómo mediría la latencia de extremo a extremo?

Aquí se midió desde el cliente: reloj al hacer `put_object`, sondeo de DynamoDB con `get_item` y de una cola SQS
suscrita a SNS. Con Lambda caliente dio p50 72 ms / p95 186 ms hasta DynamoDB y p50 165 ms / p95 271 ms hasta la
alerta (n = 30). En AWS propagaría un `correlation_id`, usaría X-Ray y calcularía p50/p95/p99 en CloudWatch.

### 10. ¿Cómo manejaría una tormenta de eventos?

Usaría lotes, límites de concurrencia, DLQ, reintentos con jitter, alarmas por throttling y capacidad on-demand o
autoescalada. La clave idempotente evita que los reintentos dupliquen datos.

### 11. ¿Qué es un arranque en frío y cuánto pesa aquí?

Es la primera invocación tras crear o cambiar la función: hay que levantar el entorno e importar dependencias. En
el emulador costó p50 3.357 ms frente a 72 ms en caliente (5 frente a 30 mediciones). Se mitiga con paquetes
pequeños, imports perezosos o concurrencia aprovisionada si la latencia lo exige.

### 12. ¿Qué pasó al subir dos veces el mismo feed?

La segunda entrega recibió 2.080 eventos, insertó 0, contó 2.080 duplicados y no emitió alertas. La idempotencia
está en DynamoDB, no en la memoria de la Lambda, así que funciona aunque la repetición caiga en otro contenedor.

### 13. ¿Por qué una sola Lambda y no una Lambda de alertas aparte?

Porque a este volumen la separación solo agrega piezas (DynamoDB Streams, otra función, otro rol) sin resolver un
problema real: la alerta ya depende de que la escritura condicional haya insertado un evento nuevo, así que los
reenvíos no alertan. El costo aceptado es que, si SNS falla justo después de guardar, el reintento de S3 ve el
evento como duplicado y esa alerta se pierde (la ingesta no). Con más volumen o alertas críticas se separaría con
Streams o con una cola SQS delante de una Lambda de alertas, con DLQ.

## Equivalencia en AWS real

Se conservarían S3, Lambda, DynamoDB y SNS, pero se eliminarían los endpoints locales, se aplicarían políticas IAM
de mínimo privilegio, cifrado KMS, DLQ, alarmas y despliegues por ambientes. Este proyecto no ejecuta ese paso.

