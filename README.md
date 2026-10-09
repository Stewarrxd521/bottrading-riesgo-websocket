# Bot Trading Riesgo - Shorts a ganadores de Binance Futures

Aplicación web lista para Render que monitorea los ganadores de Binance USDT-M Futures y abre tramos **short** cuando un símbolo supera niveles de ganancia de 24h.

## Estrategia implementada

- **Casi sin REST** (para evitar baneos de IP de Binance):
  - La única petición REST de datos es `exchangeInfo`, una vez al arrancar, y se guarda en disco. Si el caché tiene menos de `SYMBOL_REFRESH_HOURS`, el arranque no hace ninguna. Puede ir por un proxy (`REST_PROXY_URL`); si el proxy falla se reintenta una vez por la IP directa.
  - No se consulta ni se cambia el leverage: se usa el que ya tenga la cuenta.
  - REST solo se usa además para enviar órdenes reales (`PAPER_MODE=false` y `LIVE_TRADING=true`).
- Precios, cambio 24h y volumen por WebSocket (`!ticker@arr`, `!markPrice@arr`, `<symbol>@bookTicker`).
- Velas de **todos** los símbolos solo por WebSocket (`kline_ws.py`), sin backfill REST:
  - **Una conexión por intervalo** (`KLINE_INTERVALS`, por defecto `1m`) con todos los símbolos suscritos (Binance admite 1024 streams por conexión).
  - Se guardan las últimas `KLINE_HISTORY` velas cerradas por símbolo en un buffer circular de numpy: 48 bytes por vela en `float64` (28 en `float32`), frente a ~300 bytes con objetos Python. Con 819 símbolos y 1500 velas son ~60 MB por intervalo (~35 MB en `float32`).
  - Las velas en formación se descartan sin parsear el JSON; solo se procesa el cierre.
  - La confirmación de entrada exige que la vela 1m del minuto anterior haya cerrado alcista.
- Abre short en tramos configurables cuando el cambio 24h supera estos niveles:
  - `50%, 75%, 100%, 150%, 200%, 250%`
- Tamaño de cada tramo:
  - `5, 5, 10, 20, 40, 80 USDT`
- Cierra toda la posición cuando la ganancia no realizada llega al 50% del capital colocado:
  - Ejemplo: posición de `5 USDT` -> cierre con `2.5 USDT` de ganancia.
- Muestra en una página web:
  - Ganadores detectados.
  - Posiciones abiertas.
  - PnL no realizado.
  - Operaciones cerradas.
  - Eventos del bot.

## Seguridad

El bot arranca por defecto en **PAPER_MODE=true**, por lo que simula las órdenes y no envía operaciones reales.

Para operar real en Binance Futures debes configurar todas estas variables de entorno:

```bash
PAPER_MODE=false
LIVE_TRADING=true
BINANCE_API_KEY=tu_api_key
BINANCE_API_SECRET=tu_api_secret
```

> Usa primero paper trading. Un short contra monedas que suben 100%-250% puede liquidarse si no hay control de margen, apalancamiento y pérdidas.

## Variables de entorno principales

| Variable | Default | Descripción |
| --- | --- | --- |
| `PAPER_MODE` | `true` | Simula órdenes si está en `true`. |
| `LIVE_TRADING` | `false` | Habilita órdenes reales si también `PAPER_MODE=false`. |
| `ENTRY_LEVELS` | `50,75,100,150,200,250` | Niveles de subida 24h para abrir tramos. |
| `ENTRY_NOTIONALS` | `5,5,10,20,40,80` | USDT por tramo. |
| `TAKE_PROFIT_FRACTION` | `0.5` | Ganancia objetivo sobre el notional total. |
| `SCAN_INTERVAL_SECONDS` | `60` | Frecuencia mínima de consulta REST para refrescar ganadores y actualizar la lista seguida por WebSocket. |
| `MAX_SYMBOLS` | `120` | Máximo de ganadores a evaluar por escaneo. |
| `MIN_GAIN_TO_SHOW` | `0` | Filtro mínimo de porcentaje para mostrar ganadores en la tabla. |
| `INCLUDE_SPOT_WINNERS` | `false` | Conservado solo para el fallback manual REST; el escaneo operativo usa futures por WebSocket. |
| `REST_PROXY_URL` | vacío | Proxy HTTP para la única petición REST del arranque (`exchangeInfo`). Configúralo como secreto. |
| `SYMBOL_REFRESH_HOURS` | `12` | Antigüedad máxima del caché de `exchangeInfo` en disco antes de volver a pedirlo al arrancar. |
| `KLINE_INTERVALS` | `1m` | Intervalos de velas, separados por comas. Cada uno es una conexión WebSocket. `1m` siempre se incluye. |
| `KLINE_HISTORY` | `2` | Velas cerradas guardadas por símbolo e intervalo. |
| `KLINE_DTYPE` | `float64` | `float32` usa la mitad de RAM a cambio de precisión. |
| `KLINE_WS_COMPRESSION` | `false` | Compresión deflate en las conexiones de velas (menos tráfico, más CPU). |
| `KLINE_ALLOW_NO_DATA` | `false` | Si es `true`, permite entrar cuando aún no hay vela cerrada reciente. |
| `KLINE_STREAMS_PER_CONN` | `1024` | Máximo de símbolos por conexión; solo si se supera se abre otra para el mismo intervalo. |
| `STATE_FILE` | `/tmp/bottradingriesgo_state.json` | Archivo usado para compartir el último estado útil entre reinicios/workers. |
| `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN` | vacío | Base de datos donde se guarda el estado para recuperarlo tras un reinicio. Sin ellas solo hay copia local. |
| `STATE_KEY_PREFIX` | `botshort` | Prefijo de las claves en Upstash (cámbialo si compartes la base con otro bot). |
| `STATE_TRADES_MAX` | `5000` | Cierres guardados en el historial remoto. |
| `STATE_HEARTBEAT_S` | `40` | Cada cuánto renueva el control y guarda MFE/MAE. Menos segundos = más comandos de Upstash. |
| `STATE_OWNER_LEASE_S` | `120` | Si la instancia dueña muere sin apagarse bien, la siguiente espera como mucho esto para tomar el control. |
| `STATE_BOOT_WAIT_S` | `20` | Segundos que espera el estado antes de abrir los WebSockets; si no llega, lo aplica en cuanto llegue (mientras tanto no opera). |

## Recuperación tras reinicio (Upstash Redis)

En Render free el disco se borra en cada reinicio, redeploy o spin-down. Para no perder las operaciones abiertas, el bot guarda su estado en **Upstash Redis** (plan gratis, API REST, sin librerías extra):

- `botshort:state`: documento con las posiciones **abiertas** y todos sus datos (tramos, precio de entrada, cantidad, SL y si es manual, MFE/MAE, `trade_id`), además de cooldowns, secuencia de `trade_id`, SL global (solo si se cambió desde la web), bloqueos por precio y PnL realizado. Al cerrarse una posición desaparece del documento.
- `botshort:trades`: historial de cierres (últimos `STATE_TRADES_MAX`), para que `/api/stats` y el CSV sobrevivan a los reinicios.
- `botshort:owner`: qué instancia controla el estado. Caduca sola (`STATE_OWNER_LEASE_S`) si esa instancia muere sin avisar.

Cómo se comporta:

- Solo una instancia opera a la vez. Al arrancar, la instancia toma el control, lee el estado y lo restaura **antes** de abrir los WebSockets, así no abre duplicados.
- En un deploy, Render arranca la instancia nueva antes de parar la vieja. La nueva **espera sin operar** (el dashboard muestra `esperando control` y rechaza cierres y cambios de SL con 409) mientras la vieja sigue gestionando las posiciones. Cuando Render apaga la vieja (SIGTERM), esta deja de operar, guarda por última vez y libera el control; la nueva lo toma en unos segundos con todo lo que hizo la vieja.
- Si la instancia dueña muere de golpe (sin SIGTERM), la siguiente toma el control cuando caduca (como mucho `STATE_OWNER_LEASE_S`, 2 minutos por defecto).
- Si a una instancia le quitan el control, pasa a **standby** (no abre ni cierra nada) y lo retoma sola si la otra desaparece.
- Si Upstash no responde, el bot no abre posiciones nuevas, pero sigue cerrando las que ya tiene. Los cambios se reintentan hasta que Upstash vuelve.
- Aperturas, cierres y cambios de SL se guardan al instante; MFE/MAE va con el latido (cada `STATE_HEARTBEAT_S`).
- Coste: Upstash cuenta cada comando de los scripts, así que son unos **200.000-270.000 comandos al mes** de los 500.000 gratis. Usa una base de datos solo para este bot; si la compartes con otro uso intensivo podrías pasarte del límite gratis.
- Funciona igual en Render o en cualquier otro sitio (tu PC, otro hosting): basta con las dos variables. Si arrancas una copia local con las mismas credenciales mientras la de Render está activa, la local espera sin operar; solo toma el control si la otra se apaga.
- Si la URL o el token están mal (por ejemplo, la URL de **QStash**, que es el servicio de colas de Upstash y no guarda datos), el log y el panel lo dicen con un ⛔ y el bot no abre ni cierra posiciones hasta corregirlo. Para operar sin guardar, quita las dos variables.

Configuración:

1. Crea una base de datos Redis gratis en [upstash.com](https://upstash.com), en la región más cercana a tu servicio de Render.
2. En la pestaña *REST API* copia `UPSTASH_REDIS_REST_URL` y `UPSTASH_REDIS_REST_TOKEN`.
3. Añádelas como variables de entorno (secretas) del servicio en Render.

El chip **estado guardado** del dashboard muestra `upstash · ok` cuando todo va bien. `GET /api/recovery` devuelve el último documento guardado.

Pruebas (necesitan `redis-server` instalado; Upstash se simula con un Redis real para ejecutar los scripts Lua):

- Unitarias: `python -m pytest tests`.
- Punta a punta (Binance y Upstash simulados, reinicios con `kill -9`, deploy con solapamiento y SIGTERM, Upstash caído): `python -m tests.e2e_restart`.

## Ejecutar local

```bash
pip install -r requirements.txt
python app.py
```

Abre `http://localhost:8000`.

## Diagnóstico de pantalla vacía

Si el bot abre posiciones en los logs pero la página no las muestra, revisa en la web el bloque **Estado API crudo**. La página ahora renderiza un snapshot inicial del servidor y luego refresca `/api/status`; si falla JavaScript, fetch o el endpoint, el error queda visible en **Último error / diagnóstico**.

## Deploy en Render

El archivo `render.yaml` incluye el servicio web y fija `PYTHON_VERSION=3.12.13` para evitar que Render use Python 3.14, donde dependencias con extensiones nativas pueden compilar desde fuente y fallar. En Render configura las variables de entorno necesarias y despliega el repositorio.

