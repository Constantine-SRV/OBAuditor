# OBAuditor — Архитектура и алгоритмы

## Оглавление
1. [Обзор](#обзор)
2. [Структура проекта](#структура-проекта)
3. [Инициализация БД и схема таблиц](#инициализация-бд-и-схема-таблиц)
4. [Получение списка файлов](#получение-списка-файлов)
5. [Алгоритм определения позиции чтения](#алгоритм-определения-позиции-чтения)
6. [Обработка ротации файлов](#обработка-ротации-файлов)
7. [Определение IP узла](#определение-ip-узла)
8. [Разбор строк лога](#разбор-строк-лога)
9. [Фильтрация служебных логинов](#фильтрация-служебных-логинов)
10. [Запись данных в БД](#запись-данных-в-бд)
11. [Предотвращение дублей и пропусков](#предотвращение-дублей-и-пропусков)
12. [Поддержка нескольких экземпляров сервиса](#поддержка-нескольких-экземпляров-сервиса)
13. [Обработка LOGOFF](#обработка-logoff)
14. [Поток данных end-to-end](#поток-данных-end-to-end)

---

## Обзор

OBAuditor читает логи OceanBase (SERVER и PROXY), извлекает события логина/логоффа
и записывает их в таблицу `sessions` базы `admintools`.

**Стек:** Java 23, OceanBase JDBC, стандартный javax.xml (нет внешних зависимостей кроме драйвера).

---

## Структура проекта

```
src/
├── Main.java                        # Точка входа
├── model/
│   ├── AppConfig.java               # POJO конфигурации (paths, collectorId, connection)
│   ├── AppConfigReader.java         # Читает config.xml → AppConfig; fallback collectorId → hostname
│   ├── ConnectionConfig.java        # POJO подключения к БД (hosts, user, password, database)
│   ├── LoginEvent.java              # POJO события: LOGIN_OK / LOGIN_FAIL / LOGOFF
│   ├── LogFileRecord.java           # POJO строки таблицы logfiles (offset, fileIp, collectorId)
│   └── PasswordEnricher.java        # Подставляет пароль из env или интерактивно
├── db/
│   ├── DbInitializer.java           # Создаёт базу admintools и таблицы sessions, logfiles
│   ├── LogFileDao.java              # CRUD для таблицы logfiles (с фильтром по collectorId)
│   └── SessionDao.java              # INSERT IGNORE логинов; loadOpenProxySessids(); updateLogoff()
└── log/
    ├── LogFileProcessor.java        # Оркестратор: обходит директории, управляет offset-ами,
    │                                #   загружает Set<Long> openSessions перед чтением файла
    ├── LogLineHandler.java          # Диспетчер строк → парсер → SessionDao;
    │                                #   ведёт Set открытых сессий, обрабатывает LOGOFF
    ├── LogLine.java                 # POJO одной строки лога (raw, timestamp, tid, traceId)
    ├── ObServerLineParser.java      # Парсер observer.log (статический, без состояния)
    └── ObProxyLineParser.java       # Парсер obproxy.log (stateful, HashMap по cs_id)
```

---

## Инициализация БД и схема таблиц

**Класс:** `DbInitializer.initialize()`

1. Подключение к системному тенанту (база `oceanbase`)
2. `CREATE DATABASE admintools` если не существует
3. Подключение к `admintools`
4. `CREATE TABLE sessions` если не существует
5. `CREATE TABLE logfiles` если не существует

Проверка через `information_schema` — не использует `IF NOT EXISTS` чтобы видеть факт создания в логе.

### Таблица `sessions`

Одна строка = одна сессия. Логин и логофф в одной строке.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT UNSIGNED AI | Суррогатный PK |
| `source` | VARCHAR(8) | `SERVER` или `PROXY` |
| `server_ip` | VARCHAR(64) NOT NULL DEFAULT '' | IP узла-источника лога (для UNIQUE KEY) |
| `cluster_name` | VARCHAR(128) NOT NULL DEFAULT '' | Имя кластера (PROXY) или пустая строка |
| `session_id` | BIGINT UNSIGNED | `sessid` (SERVER) или `server_sessid` (PROXY) |
| `login_time` | DATETIME(6) | Время логина из лога |
| `logoff_time` | DATETIME(6) NULL | Время логоффа; NULL = сессия открыта |
| `is_success` | TINYINT(1) | 1=LOGIN_OK, 0=LOGIN_FAIL |
| `client_ip` | VARCHAR(64) | IP клиента |
| `tenant_name` | VARCHAR(128) | Тенант |
| `user_name` | VARCHAR(128) | Пользователь |
| `error_code` | INT | Код ошибки при FAIL |
| `` `ssl` `` | CHAR(1) | Y/N (только SERVER) — backtick: зарезервированное слово |
| `client_type` | VARCHAR(16) | JDBC / JAVA / OCI / OBCLIENT / MYSQL_CLI |
| `proxy_sessid` | BIGINT UNSIGNED | proxy_sessid — ключ для закрытия сессий через LOGOFF |
| `cs_id` | BIGINT UNSIGNED | Client session id (PROXY) |
| `server_node_ip` | VARCHAR(64) | IP OBServer-узла из тела строки лога |
| `from_proxy` | TINYINT(1) | 1 = коннект пришёл через OBProxy |

**UNIQUE KEY** `uk_sess (source, server_ip, cluster_name, session_id, login_time)` — защита от дублей.

**Важно:** `server_ip` и `cluster_name` NOT NULL DEFAULT '' — потому что NULL != NULL в MySQL/OceanBase,
NULL в составном UNIQUE KEY не защищает от дублей.

### Таблица `logfiles`

Состояние обработки каждого лог-файла.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT AI | PK |
| `collector_id` | VARCHAR(128) NOT NULL | Идентификатор сервиса-коллектора |
| `file_dir` | VARCHAR(512) | Директория |
| `file_name` | VARCHAR(256) | Имя файла |
| `file_type` | VARCHAR(16) | `SERVER` или `PROXY` |
| `file_size` | BIGINT | Последний известный размер в байтах |
| `last_line_num` | BIGINT | **Байтовый offset** (не номер строки!) |
| `last_timestamp` | VARCHAR(32) | Метка времени последней обработанной строки |
| `last_tid` | INT | Thread ID последней обработанной строки |
| `last_trace_id` | VARCHAR(64) | Trace ID последней обработанной строки |
| `file_ip` | VARCHAR(64) | IP узла-источника (SERVER: из заголовка, PROXY: из лога) |

**UNIQUE KEY** `uq_collector_dir_name (collector_id, file_dir(255), file_name)` —
`file_dir(255)` prefix потому что полный путь может превышать лимит ключа OceanBase.

---

## Получение списка файлов

**Класс:** `LogFileProcessor.processDirectory()`

```
Main
 └─ LogFileProcessor.processServerDirs(List<String> dirs)   // для каждой директории
 └─ LogFileProcessor.processProxyDirs(List<String> dirs)
      └─ processDirectory(dirPath, fileType, namePrefixes)
           ├─ File.listFiles() с фильтром по имени:
           │    SERVER: observer.log  + observer.log.*
           │    PROXY:  obproxy.log   + obproxy.log.*
           ├─ dao.loadByDir(collectorId, dirPath)  → Map<fileName, LogFileRecord>
           └─ Сортировка: ротированные файлы (*.log.DATE) первыми, активный (*.log) — последним
                → processFile() для каждого файла
```

**Сортировка** важна: ротированный файл дочитывается до конца раньше чем начинается
чтение нового активного файла. Это сохраняет хронологический порядок событий.

---

## Алгоритм определения позиции чтения

**Класс:** `LogFileProcessor.processFile()` + `readAndProcess()`

```
processFile():
  1. Берём LogFileRecord из БД (или создаём новый с offset=0)
  2. startOffset = record.lastLineNum  (байтовый offset)
  3. Если нет изменений (currentSize == record.fileSize && offset >= size) → пропускаем

readAndProcess():
  4. FileInputStream → FileChannel
  5. channel.position(startOffset)   // прыжок к нужному байту, без построчного перебора
  6. BufferedReader читает строки с текущей позиции до EOF
  7. После EOF: record.lastLineNum = channel.position()  // сохраняем новый offset
```

**Почему offset, а не номер строки:**
Построчный перебор больших файлов на сетевых шарах (\\server\share) крайне медленный.
`FileChannel.position()` позволяет пропустить гигабайты за O(1).

---

## Обработка ротации файлов

**Класс:** `LogFileProcessor.shouldResetToStart()`

OceanBase ротирует логи переименованием:
`observer.log` → `observer.log.20260310195115446`, создаётся новый `observer.log`.

Детектирование ротации (только для активного файла — без суффикса даты):

```
shouldResetToStart(record, currentSize):
  1. currentSize < record.fileSize  → файл стал меньше = новый файл = ротация
  2. lastTimestamp старше 10 минут  → сервис не работал, файл мог сменяться несколько раз
                                       → читаем сначала чтобы не пропустить события

При ротации:
  - offset сбрасывается в 0
  - lastTimestamp / lastTid / lastTraceId очищаются
  - fileIp НЕ сбрасывается (хост не меняется при ротации файла)
```

**Почему ротированные файлы обрабатываются первыми:**
После переименования `observer.log` → `observer.log.DATE` в таблице logfiles
появляется новая запись для `observer.log.DATE` с offset=0.
Сортировка гарантирует что ротированный файл дочитается полностью до начала чтения нового активного.

---

## Определение IP узла

IP хранится в `logfiles.file_ip` и используется как `server_ip` в таблице `sessions`.

### SERVER (`observer.log`)

**Класс:** `LogFileProcessor.readServerIpFromFirstLine()`

Первая строка каждого файла содержит:
```
[timestamp] INFO  New syslog file info: [address: "192.168.55.205:2882", ...]
```
Паттерн: `address:\s*"(\d+\.\d+\.\d+\.\d+):\d+"` → `192.168.55.205`

Читается один раз при первой обработке файла, сохраняется в `file_ip`.

### PROXY (`obproxy.log`)

**Класс:** `LogFileProcessor.readAndProcess()` + `ObProxyLineParser`

Первая строка файла не содержит IP прокси-хоста. IP виден в строках `server session born`:
```
server session born(... local_ip:{192.168.55.200:37288}, ...)
```
Паттерн: `\blocal_ip:\{(\d+\.\d+\.\d+\.\d+):\d+\}` → `192.168.55.200`

Алгоритм:
```
Если file_ip не заполнен (первый запуск или новый файл):
  - флаг needProxyIp = true
  - при каждой строке "server session born" применяем паттерн
  - при нахождении:
      record.fileIp = IP
      dao.updateFileIp(record)        // сохраняем сразу, не ждём конца файла
      handler.setServerIp(IP)         // обновляем handler для текущих вставок
      needProxyIp = false
Если file_ip уже заполнен → сканирование не нужно, берём из record.fileIp
```

**Также:** IP конкретного OBServer-узла (куда подключён клиент через прокси)
виден в строке `succ to set proxy_sessid`:
```
server_ip={192.168.55.205:2881}
```
Паттерн: `\bserver_ip=\{(\d+\.\d+\.\d+\.\d+):\d+\}` → `192.168.55.205`
Пишется в `sessions.server_node_ip` — отдельное поле, не путать с `server_ip`.

---

## Разбор строк лога

### SERVER — `ObServerLineParser` (статический класс)

**LOGIN событие** — строка содержит `MySQL LOGIN`:
```
[ts] INFO [SERVER] process (obmp_connect.cpp:518) [...] MySQL LOGIN(
  direct_client_ip="...", client_ip=..., tenant_name=..., user_name=...,
  sessid=..., proxy_sessid=..., use_ssl=..., proc_ret=...,
  from_proxy=..., from_java_client=..., from_jdbc_client=..., from_oci_client=...,
  conn->client_type_=...)
```
- `proc_ret=0` → LOGIN_OK, иначе LOGIN_FAIL
- `sessid` — BIGINT UNSIGNED, парсится через `Long.parseUnsignedLong()` (значения > Long.MAX_VALUE)
- Тип клиента: **приоритет флагам** `from_jdbc_client` / `from_java_client` / `from_oci_client`
  над `conn->client_type_` (флаги точнее — например `client_type_=1` (OBCLIENT) но `from_jdbc_client=true` → JDBC)
- `from_proxy` → `LoginEvent.fromProxy` → `sessions.from_proxy`

**LOGOFF событие** — строка содержит `connection close`:
```
[ts] INFO [RPC.OBMYSQL] destroy (...) [...] connection close(
  sessid=..., proxy_sessid=..., tenant_id=..., from_proxy=...)
```

### PROXY — `ObProxyLineParser` (stateful, один экземпляр на файл)

Хранит незавершённые состояния в `HashMap<Long, BornInfo>` и `HashMap<Long, FailInfo>`.
Ключ — `cs_id` (client session id).

**LOGIN_OK** — два шага:

| Шаг | Строка | Что извлекаем |
|---|---|---|
| 1 | `server session born` | `cs_id`, `cluster_name`, `tenant_name`, `user_name` → `bornMap` |
| 2 | `succ to set proxy_sessid` | `cs_id`, `proxy_sessid`, `server_sessid`, `client_addr`, `server_ip={...}` → испускаем событие |

**LOGIN_FAIL** — два шага:

| Шаг | Строка | Что извлекаем |
|---|---|---|
| 1 | `error_transfer` + `OB_MYSQL_COM_LOGIN` | `sm_id` (как cs_id), `client_ip`, `timestamp` → `failMap` |
| 2 | `client session do_io_close` | `cs_id`, `proxy_sessid`, `cluster`, `tenant`, `user` → испускаем событие |

**LOGOFF** — одна строка `handle_server_connection_break` + `COM_QUIT`:
```
[ts] ... handle_server_connection_break ... COM_QUIT ...
  cs_id=..., proxy_sessid=..., proxy_user_name=user@tenant#cluster, client_ip={...}
```

---

## Фильтрация служебных логинов

**Класс:** `ObServerLineParser.parseLogin()`

Отсекаем до создания `LoginEvent`:
```java
if ("ocp_monitor".equals(userName) || "proxy_ro".equals(userName)) return null;
if ("127.0.0.1".equals(clientIp) && "root".equals(userName))       return null;
```

**Класс:** `ObProxyLineParser.handleBorn()`

```java
if ("proxyro".equals(info.user) || info.tenant == null) return; // не кладём в bornMap
```

Если `bornMap` не содержит `cs_id` — шаг 2 (`succ to set proxy_sessid`) не испустит событие.

---

## Запись данных в БД

**Класс:** `LogLineHandler.handle()` → `SessionDao`

```
LogLineHandler.handle(LogLine):
  1. Вызвать парсер (SERVER → ObServerLineParser.parse(), PROXY → proxyParser.parse())
  2. Если null → пропустить
  3. Если LOGIN_OK или LOGIN_FAIL → sessionDao.insertLogin(event, serverIp)
  4. Если LOGOFF → sessionDao.updateLogoff(proxySessid, eventTime)
```

**`SessionDao.insertLogin()`:**
```sql
INSERT IGNORE INTO sessions
  (source, server_ip, cluster_name, session_id, login_time,
   is_success, client_ip, tenant_name, user_name,
   error_code, `ssl`, client_type, proxy_sessid, cs_id,
   server_node_ip, from_proxy)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**BIGINT UNSIGNED и JDBC** — везде одно правило:
- **Запись:** `setString(idx, Long.toUnsignedString(val))` — избегаем signed overflow
- **Чтение:** `Long.parseUnsignedLong(rs.getString("col"))` — `rs.getLong()` бросает "Out of range" для значений > Long.MAX_VALUE

`server_node_ip`:
- SOURCE=SERVER: совпадает с `fileServerIp`
- SOURCE=PROXY: берётся из `event.serverIp` (поле `server_ip={...}` из строки лога)

---

## Предотвращение дублей и пропусков

### Дубли

1. **`INSERT IGNORE`** + UNIQUE KEY `uk_sess (source, server_ip, cluster_name, session_id, login_time)` —
   повторная вставка одного события тихо игнорируется на уровне БД.

2. **Байтовый offset** — при следующем запуске чтение начинается точно с того байта где остановились,
   уже обработанные строки физически не читаются.

### Пропуски

1. **Ротированные файлы обрабатываются первыми** — события из старого файла не теряются.

2. **`shouldResetToStart()`** — если сервис не работал более 10 минут, весь активный файл
   перечитывается с начала. `INSERT IGNORE` защищает от дублей при этом.

3. **`fileIp` не сбрасывается при ротации** — IP прокси-хоста не теряется.

4. **`updateFileIp()` вызывается сразу** при нахождении IP в PROXY-логе, не дожидаясь
   конца файла — если процесс упадёт посередине, IP не придётся искать заново.

---

## Поддержка нескольких экземпляров сервиса

**Классы:** `AppConfig.collectorId`, `AppConfigReader`, `LogFileRecord.collectorId`, `LogFileDao`

Когда несколько сервисов читают файлы локально (каждый на своём хосте), пути совпадают:
`/data/oceanbase/log/observer.log` — одинаковый путь на разных машинах.

**Решение:** `collector_id` в таблице `logfiles`.

```xml
<!-- config.xml -->
<CollectorId>192.168.55.100</CollectorId>  <!-- IP сервера где запущен сервис -->
<!-- Если пусто → берётся hostname автоматически -->
```

- `LogFileDao.loadByDir(collectorId, fileDir)` — каждый сервис видит только свои записи
- UNIQUE KEY включает `collector_id` первым полем
- При старте: `AppConfigReader` читает `<CollectorId>`, если пусто → `InetAddress.getLocalHost().getHostName()`
- `Main.java` передаёт `config.collectorId` в `new LogFileProcessor(conn, config.collectorId)`

---

## Обработка LOGOFF

**Классы:** `SessionDao.loadOpenProxySessids()`, `SessionDao.updateLogoff()`,
`LogFileProcessor.loadOpenSessions()`, `LogLineHandler.handleLogoff()`

### Ключ для закрытия — proxy_sessid

`proxy_sessid` — 64-битное значение которое **кодирует `proxy_id` экземпляра OBProxy** в старших битах
(версия 1: 8 бит, до 255 проксей; версия 2: 13 бит, до 8191 проксей) плюс счётчик сессий в младших.
Благодаря этому `proxy_sessid` **глобально уникален в рамках кластера** при условии
что каждый OBProxy настроен с уникальным `proxy_id`.

Это позволяет закрыть **обе строки одновременно** (source=SERVER и source=PROXY)
одним UPDATE без разделения по source:
```sql
UPDATE sessions SET logoff_time = ?
WHERE proxy_sessid = ? AND logoff_time IS NULL
```

### Set открытых сессий

Перед началом чтения каждого файла `LogFileProcessor.loadOpenSessions()` загружает из БД
`proxy_sessid` всех открытых успешных сессий этого узла:

```sql
-- SessionDao.loadOpenProxySessids(serverIp)
SELECT proxy_sessid FROM sessions
WHERE server_ip = ? AND logoff_time IS NULL AND is_success = 1
  AND proxy_sessid IS NOT NULL
```

Результат — `Set<Long> openSessions` — передаётся в конструктор `LogLineHandler`.

В процессе чтения файла Set обновляется:
- **LOGIN_OK** → `openSessions.add(proxySessid)` (новые сессии из текущего файла)
- **LOGOFF** → `openSessions.remove(proxySessid)` + `updateLogoff()`

### Алгоритм обработки LOGOFF в LogLineHandler

```
handleLogoff(event):
  proxySessid = event.proxySessid  (SERVER) или event.proxySessionId  (PROXY)

  если proxySessid == null:
    → пропустить: прямое подключение без прокси, proxy_sessid отсутствует

  inSet = openSessions.remove(proxySessid)

  если НЕ inSet:
    → сессия открылась до начала файла или до первого запуска сервиса
    → fallback: всё равно вызываем updateLogoff (может закрыть "старую" сессию в БД)
    → logoffMissCount++

  updated = sessionDao.updateLogoff(proxySessid, event.eventTime)
    если updated > 0 → logoffCount++   // закрыты SERVER + PROXY строки
    если updated == 0 → сессия уже закрыта или не найдена
```

### Счётчики в итоговой строке лога

```
[LogFileProcessor] file.log — done. processed=N events=N inserted=N
    logoff=N        // успешно закрытых сессий (updateLogoff вернул > 0)
    logoffMiss=N    // логоффы для сессий не найденных в Set (fallback)
    offset=N ip=...
```

### Прямые подключения без прокси

Сессии с `from_proxy=false` (клиент подключён напрямую к OBServer, минуя OBProxy)
могут иметь `proxy_sessid=0` или NULL в SERVER-логе.
Такие LOGOFF строки пропускаются: `LOGOFF skipped (no proxy_sessid)`.
Закрытие через `proxy_sessid` для них невозможно.

---

## Поток данных end-to-end

```
config.xml
    │
    ▼
AppConfigReader.read()
    │  collectorId (из конфига или hostname)
    │  obServerLogPaths, obProxyLogPaths
    │  systemTenantConnection
    ▼
Main.main()
    ├─ DbInitializer.initialize()
    │      └─ CREATE TABLE sessions, logfiles (если нет)
    │
    └─ LogFileProcessor(conn, collectorId)
           ├─ processServerDirs()
           └─ processProxyDirs()
                  │
                  ▼
           processDirectory(dirPath, fileType)
                  ├─ LogFileDao.loadByDir(collectorId, dirPath)
                  ├─ File.listFiles() → сортировка (ротированные 1-ми)
                  └─ processFile(file)
                         ├─ Определить startOffset (из logfiles)
                         ├─ shouldResetToStart() ?
                         ├─ readServerIpFromFirstLine()  [SERVER]
                         ├─ SessionDao.loadOpenProxySessids(serverIp)
                         │      → Set<Long> openSessions
                         │
                         ├─ LogLineHandler(fileType, fileName,
                         │                serverIp, conn, openSessions)
                         │
                         └─ readAndProcess(file, offset)
                                ├─ FileChannel.position(offset)
                                ├─ for each line:
                                │    [PROXY] найти local_ip → fileIp
                                │    LogLineHandler.handle(LogLine)
                                │         ├─ ObServerLineParser.parse()
                                │         │  или proxyParser.parse()
                                │         │
                                │         ├─ LOGIN_OK/FAIL:
                                │         │    SessionDao.insertLogin()
                                │         │    → INSERT IGNORE INTO sessions
                                │         │    LOGIN_OK: openSessions.add(proxySessid)
                                │         │
                                │         └─ LOGOFF:
                                │              openSessions.remove(proxySessid)
                                │              SessionDao.updateLogoff()
                                │              → UPDATE sessions
                                │                SET logoff_time = ?
                                │                WHERE proxy_sessid = ?
                                │                  AND logoff_time IS NULL
                                │                (закрывает SERVER + PROXY сразу)
                                │
                                └─ record.lastLineNum = channel.pos()

                  LogFileDao.insert() или update()
```