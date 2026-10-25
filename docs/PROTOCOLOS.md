# Protocolos

Se datallan los 3 protocolos del sistema: el **externo** (cliente ↔ ArticleHandler), el **interno** (entre procesos, por RabbitMQ) y el de **control** (EOF / PROGRESS / DONE / FLUSH). 

---

## 1. Protocolo externo

Es el que usan el cliente y el ArticleHandler. Va sobre **TCP**, con un socket por cliente.

### Framing

Todo mensaje va con un prefijo de largo:

```
[4 bytes: largo del payload][payload]
```

El `ACK` es un frame de **largo 0**, sin payload. El `MAX_FRAME` fijado en 16 MB: si un frame lo supera, el AH responde `ERROR` y cierra.

### Mensajes

El primer byte del payload es el tipo:

| Tipo | Mensaje | Quién lo manda |
|---|---|---|
| 1 | BATCH | cliente |
| 2 | EOF | cliente |
| 3 | RESULT | ArticleHandler |
| 4 | ERROR | ArticleHandler |

El `ACK` no lleva tipo (es largo 0).

### BATCH

El cliente lee el dataset y manda lotes por **cantidad de artículos** (`BATCH_SIZE`, ej. 1000). 

```
[1B type=1]
[4B count]
por cada artículo:
  [4B docId]                 # identifier numérico del dataset
  [4B len(name)][name]
  [4B len(url)][url]
  [4B dateModified]          # epoch UTC (segundos)
  [1B inLanguage]            # 0 = en, 1 = fr
  [1B sections_flags]        # bit0 = biography, bit1 = biographie
  [4B len(abstract)][abstract]
  [4B len(imageUrl)][imageUrl]
```

`dateModified` **epoch UTC en segundos** (4 bytes), para conservar hora/minuto/segundo. 
`sections_flags` solo marca `biography`/`biographie`, que es lo único que mira Q2. 
El año para los filtros se deriva de `dateModified`.
El cliente espera un **ACK** después de cada batch.

### EOF

```
[1B type=2]
[4B total_articles]
[4B total_batches]
```

El AH compara esos totales con lo que recibió. 
Si no coinciden, devuelve `ERROR`. 
Si coinciden, publica el EOF interno de cada cola.

### RESULT

El AH devuelve los resultados de las **5 queries**
Cada `Article` se manda una sola vez y el AH lo proyecta por query.

```
[1B type=3]
[1B resultType]        # 1..5
[4B docId]             # 0 cuando no aplica (Q2 o agregados)
[4B partId]
[1B flags]             # bit0 = isLastPart, bit1 = isSessionEnd
[4B len(payload)][payload]
```

- `flags.bit0` (`isLastPart`): último fragmento de un resultado (ej. una imagen de Q2 partida en varios pedazos).
- `flags.bit1` (`isSessionEnd`): último resultado de toda la sesión. Va en 1 una sola vez.
- Si una query no tiene resultados, igualmente se envía  `RESULT` vacío con `flags.bit0 = 1`, para que el AH sepa que terminó.
- Después de `flags.bit1 = 1`, el AH cierra. Si el cliente ve EOF sin ese bit, lo toma como truncado.

### ERROR

```
[1B type=4]
[4B code]
[4B len(message)][message]
```

### Responsabilidades

- El **clientId** lo asigna el `session_manager` del AH al aceptar la conexión. No viaja en el protocolo.
- El AH tiene dos hilos: `ingest_thread` (lee batches y manda ACK) y `egress_thread` (manda RESULT). Como escriben por el mismo socket, hay un lock de escritura.
- El cliente tiene un reader thread que demuxa ACK / RESULT / ERROR. El sender espera el ACK de cada batch.

---

## 2. Protocolo interno (plano de datos)

Es el que usan los procesos entre sí, por **RabbitMQ**. 

### Envelope común

Todo mensaje interno arranca igual:

```
[1B type]
[4B clientId]          # numérico, lo asigna el AH
[body]
```

- `type` identifica el tipo (tabla abajo). Los de datos van de 1 a 10; los de control, de 101 a 104.
- `clientId` es numérico (`u32`) y va en **todos** los mensajes internos.
- Si `type >= 101`, el mensaje es de control (sección 3).

### Primitivas

Las mismas para todos los tipos:

| Tipo | Bytes |
|---|---|
| `u8` | 1 |
| `u16` | 2 (big-endian) |
| `u32` | 4 (big-endian) |
| `string` | `[u32 len][UTF-8]` |
| `bytes` | `[u32 len][raw]` |
| `list<T>` | `[u32 count][T...]` |

### Tipos del plano de datos

| Code | Tipo | Va de → a |
|---|---|---|
| 1 | `ARTICLE` | ArticleHandler → Q1/Q2/Q3/Q4/Q5 |
| 2 | `WORD_Q3` | Tokenizer → Q3 Reduce |
| 3 | `WORD_Q4` | Tokenizer → Q4 Counter |
| 4 | `IMG_REQ` | Q2 Filter → Image Downloader |
| 5 | `WORDCOUNT_SUM` | Q5 Counter → Q5 Accumulator (≤ 2020) |
| 6 | `WORDCOUNT_CAND` | Q5 Counter → Q5 Average+Filter (≥ 2021) |
| 7 | `Q3_PARTIAL` | Q3 Reduce → Q3 Join |
| 8 | `Q4_PARTIAL` | Q4 Counter → Q4 Join |
| 9 | `Q5_ACC_PARTIAL` | Q5 Accumulator → Q5 Average+Filter |
| 10 | `RESULT` | etapa final → QueryResults → ArticleHandler |

### `ARTICLE` (1)

Para no enviar datos innecesarios el ArticleHandler **proyecta** según la query. El body lleva una máscara de campos y solo los campos presentes:

```
[1B fields_mask]
  bit0: docId
  bit1: name
  bit2: url
  bit3: dateModified
  bit4: inLanguage
  bit5: sections_flags
  bit6: abstract
  bit7: imageUrl
```

Los campos van en ese orden, solo si el bit está en 1:

```
docId:          u32      # identifier del dataset
name:           string
url:            string
dateModified:   u32      # epoch UTC
inLanguage:     u8       # 0 = en, 1 = fr
sections_flags: u8       # bit0 = biography, bit1 = biographie
abstract:       string
imageUrl:       string
```

Máscaras que usa el ArticleHandler:

| Cola | Campos |
|---|---|
| `Q1_Metadata` | docId, url, dateModified, inLanguage |
| `Q2_Sections` | docId, name, url, dateModified, sections_flags, imageUrl |
| `Q34_Abstract` | docId, inLanguage, abstract |
| `Q5_Abstract` | docId, url, dateModified, abstract |

El **año se deriva de `dateModified`** 
Q1 devuelve `dateModified`
Q2/Q5 filtran por el año derivado. 
`sections_flags` solo marca `biography`/`biographie`.

### `WORD_Q3` (2)

```
word:        string
inLanguage:  u8      # 0 = en, 1 = fr
```

### `WORD_Q4` (3)

```
docId:  u32
word:   string
```

### `IMG_REQ` (4)

```
name:      string
url:       string
imageUrl:  string
```

### `WORDCOUNT_SUM` (5)

```
wordCount: u32
```

### `WORDCOUNT_CAND` (6)

```
wordCount: u32
docId:     u32
url:       string
```

### `Q3_PARTIAL` (7)

```
words: list<string>
```

### `Q4_PARTIAL` (8)

```
pairs: list< [string word][u32 count] >
```

### `Q5_ACC_PARTIAL` (9)

```
sum:   u32
count: u32
```

### `RESULT` (10)

```
resultType: u8      # 1..5
docId:      u32     # 0 cuando no aplica
partId:     u32
flags:      u8      # bit0 = isLastPart, bit1 = isSessionEnd
payload:    bytes
```

El `payload` depende de `resultType`:

| Query | Payload |
|---|---|
| Q1 | `url: string`, `dateModified: u32` (epoch UTC) |
| Q2 | `name: string`, `url: string`, `chunk: bytes` |
| Q3 | `list<string>` de palabras |
| Q4 | `list< [string word][u32 count] >` |
| Q5 | `list< [string url][u32 wordCount] >` |

Para Q1, el cliente formatea `dateModified` (epoch UTC) a la fecha/hora de salida. 

- Q2 se **fragmenta**: cada `RESULT` lleva un `chunk` y usa `partId`/`flags.bit0`.
- Si una query no tiene resultados, la etapa final emite un `RESULT` con `payload` vacío y `flags.bit0 = 1`.
- El ArticleHandler **reenvía** este `RESULT` al cliente (protocolo externo), sacando el `clientId` (la conexión ya identifica al cliente).

### `ingest` / `result` / `serializer`

- `serializer`: implementa las primitivas y el encode/decode de cada tipo. Es el único que conoce los bytes.
- `ingest`: para cada etapa, sabe **qué tipos entrantes** espera y los valida/proyecta (ej. `Q3 Reduce` espera `WORD_Q3`).
- `result`: para cada etapa, arma y encoda **qué tipos salientes** produce (ej. `Tokenizer` produce `WORD_Q3` y `WORD_Q4`).

---

## 3. Protocolo de control

Es el que coordina el cierre de cada etapa. **Todo va por `clientId`** y usa el mismo envelope de la sección 2:

```
[1B type]
[4B clientId]
[body]
```

### Tipos

| Code | Tipo | Quién lo emite → quién lo consume |
|---|---|---|
| 101 | `EOF` | ArticleHandler → una réplica → control (broadcast) |
| 102 | `PROGRESS` | réplica → control (broadcast) |
| 103 | `DONE` | réplica → etapa siguiente |
| 104 | `FLUSH` | cualquier réplica → control (broadcast) |

### `EOF` (101)

```
total: u32
```

El ArticleHandler lo publica  en la cola de datos, después de todos los datos de ese cliente. `total` es cuántos mensajes se mandaron a esa cola.

Una sola réplica lo toma y lo republica al canal de control, el canal hace broadcast a todas las réplicas (incluida ella).

### `PROGRESS` (102)

```
controllerType: u8
nodeId:         u16      # índice de réplica
quantity:       u32
```

`quantity` es el delta procesado desde el último reporte. El canal de control lo difunde a todas las réplicas, así que todas reconstruyen la misma suma global por cliente.

### `DONE` (103)

```
controllerType: u8
nodeId:         u16      # índice de réplica
quantity:       u32
```

`quantity` es cuántos mensajes de salida produjo la réplica. 
La etapa siguiente lo consume. En Q3, Q4 y Q5, el Join o el Average+Filter cuentan las M réplicas. En Q1 y Q2, el ArticleHandler cuenta las N.

### `FLUSH` (104)

```
flushId: u32
```

El `group` (que corre dentro de cada controller) mantiene el acumulado por `clientId` y lo compara contra el `total` del EOF. Cuando:

```
Σ(clientId) >= total(clientId) - B * N
```

donde `B` es `PROGRESS_BATCH_SIZE` y `N` es la cantidad de réplicas de esa etapa. Ahí una réplica publica `FLUSH{clientId, flushId}` al canal de control y se hace broadcast. Al recibirlo, cada réplica manda su pendiente (< B) y, para ese cliente, pasa a reportar **cada mensaje** hasta que cierre. Así el último remanente no queda sin reportar.

El umbral se evalúa tanto al recibir un `PROGRESS` como al recibir el `EOF`. Si no, con clientes chicos (`total < B`) no se dispara nunca.

### `controllerType`

| Code | Controller |
|---|---|
| 1 | Q1 Controller |
| 2 | Q2 Filter |
| 3 | Image Downloader |
| 4 | Tokenizer |
| 5 | Q3 Reduce |
| 6 | Q3 Join |
| 7 | Q4 Counter |
| 8 | Q4 Join |
| 9 | Q5 Counter |
| 10 | Q5 Accumulator |
| 11 | Q5 Average+Filter |

### Cierre

Cuando `Σ == total`, la etapa cierra. Cada réplica emite su salida y su `DONE`. El cierre es **por conteo exacto**, no por timeout. Si falta reportar algo, `Σ < total` y no cierra.