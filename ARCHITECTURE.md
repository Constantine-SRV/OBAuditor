# OBAuditor — Архитектура и алгоритмы

## Оглавление
1. [Обзор](#обзор)
2. [Структура проекта](#структура-проекта)
3. [Конфигурация](#конфигурация)
4. [Инициализация БД и схема таблиц](#инициализация-бд-и-схема-таблиц)
5. [Получение списка файлов](#получение-списка-файлов)
6. [Алгоритм определения позиции чтения](#алгоритм-определения-позиции-чтения)
7. [Обработка ротации файлов](#обработка-ротации-файлов)
8. [Определение IP узла](#определение-ip-узла)
9. [Разбор строк лога](#разбор-строк-лога)
10. [Фильтрация служебных логинов](#фильтрация-служебных-логинов)
11. [Запись данных в БД](#запись-данных-в-бд)
12. [Предотвращение дублей и пропусков](#предотвращение-дублей-и-пропусков)
13. [Поддержка нескольких экземпляров сервиса](#поддержка-нескольких-экземпляров-сервиса)
14. [Обработка LOGOFF](#обработка-logoff)
15. [DDL/DCL аудит](#ddldcl-аудит)
    - 15.1 [Режимы работы](#151-режимы-работы)
    - 15.2 [Алгоритм collect()](#152-алгоритм-collect)
    - 15.3 [Хардкод фильтрации](#153-хардкод-фильтрации-ddldcl-событий)
    - 15.4 [Динамические объекты ddl_dcl_audit_targets](#154-динамические-объекты-ddl_dcl_audit_targets)
    - 15.5 [Debug-режим](#155-debug-режим)
16. [Управление размером таблиц](#управление-размером-таблиц)
17. [Пересылка в rsyslog](#пересылка-в-rsyslog)
18. [Уровни логирования](#уровни-логирования)
19. [Поток данных end-to-end](#поток-данных-end-to-end)

---

## Обзор

OBAuditor читает логи OceanBase (SERVER и PROXY), извлекает события логина/логоффа,
собирает DDL/DCL операции из `GV$OB_SQL_AUDIT` и записывает всё в базу `admintools`.
В конце каждого прогона новые события пересылаются в rsyslog по UDP.

**Стек:** Java 21 (Temurin), OceanBase JDBC, стандартный javax.xml (нет внешних зависимостей кроме драйвера).

**Однопоточная модель.** Один экземпляр = один узел. На каждом узле кластера запускается
свой экземпляр — все пишут в одну базу `admintools`, разделяясь по `collector_id`.

**Управление транзакциями.** `autoCommit=true` — каждый INSERT/UPDATE коммитится немедленно,
предотвращая многочасовые блокировки при обрыве соединения. Для атомарных операций
используется локальный `setAutoCommit(false)` → `commit()` → восстановление.

---

## Структура проекта

```
src/
├── Main.java
├── model/
│   ├── AppConfig.java               # logLevel, ignoredUsers, ddlDclAuditMode, cleanup, rsyslog settings
│   ├── AppConfigReader.java
│   ├── ConnectionConfig.java        # ob_query_timeout=30s
│   ├── LoginEvent.java
│   ├── LogFileRecord.java
│   └── PasswordEnricher.java
├── db/
│   ├── DbInitializer.java           # Создаёт все таблицы включая rsyslog_cursor
│   ├── LogFileDao.java
│   ├── SessionDao.java              # insertLogin, updateLogoff, updateLogoffDirect
│   ├── DdlDclAuditDao.java          # Курсор request_time + динамические targets
│   └── CleanupDao.java              # COUNT(*)+OFFSET алгоритм
└── log/
    ├── LogFileProcessor.java
    ├── LogLineHandler.java
    ├── LogLine.java
    ├── ObServerLineParser.java
    ├── ObProxyLineParser.java
    └── RsyslogSender.java           # UDP-пересылка login/logoff/ddl событий
```

---

## Конфигурация

**Файл:** `config.xml` | **Классы:** `AppConfig`, `AppConfigReader`

| Параметр | По умолчанию | Описание |
|---|---|---|
| `CollectorId` | hostname | Уникальный ID экземпляра |
| `LogLevel` | `INFO` | DEBUG / INFO / ERROR |
| `IgnoredUsers/User` | ocp_monitor, proxy_ro, proxyro | УЗ исключённые из аудита логинов |
| `DdlDclAuditMode` | `0` | 0=выкл, 1=основной, 2=резервный |
| `Cleanup/CleanupMinute` | `-1` | Минута часа для очистки (-1=выкл) |
| `Cleanup/MaxDdlDclAuditRows` | `500000` | Макс. строк в ddl_dcl_audit_log |
| `Cleanup/MaxSessionsRows` | `500000` | Макс. строк в sessions |
| `Rsyslog/Host` | `` | Хост rsyslog (пусто = отключено) |
| `Rsyslog/Port` | `514` | UDP-порт rsyslog |
| `Rsyslog/BatchSize` | `500` | Записей за один батч |
| `ObProxyLogPaths/Path` | — | Пути до логов OBProxy |
| `ObServerLogPaths/Path` | — | Пути до логов OBServer |
| `SystemTenantConnection` | — | Подключение к OB |

```xml
<AppConfig>
    <CollectorId></CollectorId>
    <LogLevel>ERROR</LogLevel>
    <IgnoredUsers>
        <User>ocp_monitor</User>
        <User>proxy_ro</User>
        <User>proxyro</User>
    </IgnoredUsers>
    <DdlDclAuditMode>1</DdlDclAuditMode>
    <Cleanup>
        <CleanupMinute>0</CleanupMinute>
        <MaxDdlDclAuditRows>500000</MaxDdlDclAuditRows>
        <MaxSessionsRows>500000</MaxSessionsRows>
    </Cleanup>
    <Rsyslog>
        <Host>192.168.55.200</Host>
        <Port>514</Port>
        <BatchSize>500</BatchSize>
    </Rsyslog>
    <ObProxyLogPaths><Path>/data/obc1/obproxy/log</Path></ObProxyLogPaths>
    <ObServerLogPaths><Path>/data/obc1/observer/log</Path></ObServerLogPaths>
    <SystemTenantConnection>
        <Hosts><Host>192.168.55.205:2881</Host></Hosts>
        <User>ob_cluster_admin@sys</User>
        <Password></Password>
        <Database>oceanbase</Database>
    </SystemTenantConnection>
</AppConfig>
```

---

## Инициализация БД и схема таблиц

**Класс:** `DbInitializer.initialize()`

1. `sessions` — логины/логоффы
2. `logfiles` — состояние чтения файлов
3. `audit_collector_state` — курсор DDL/DCL коллектора (одна строка, id=1)
4. `ddl_dcl_audit_log` — DDL/DCL события
5. `ddl_dcl_audit_targets` — объекты для дополнительного DML-аудита
6. `rsyslog_cursor` — курсор пересылки событий в rsyslog

### Таблица `sessions`

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGINT UNSIGNED AI | PK |
| `source` | VARCHAR(8) | SERVER / PROXY |
| `server_ip` | VARCHAR(64) NOT NULL DEFAULT '' | IP источника (для UNIQUE KEY) |
| `cluster_name` | VARCHAR(128) NOT NULL DEFAULT '' | Кластер (PROXY) или '' |
| `session_id` | BIGINT UNSIGNED | sessid / server_sessid |
| `login_time` | DATETIME(6) | Время логина |
| `logoff_time` | DATETIME(6) NULL | NULL = открыта |
| `is_success` | TINYINT(1) | 1=OK, 0=FAIL |
| `client_ip` | VARCHAR(64) | IP клиента (`direct_client_ip`) |
| `tenant_name` / `user_name` | VARCHAR(128) | Тенант / пользователь |
| `error_code` | INT | Код ошибки при FAIL |
| `` `ssl` `` | CHAR(1) | Y/N |
| `client_type` | VARCHAR(16) | JDBC/JAVA/OCI/OBCLIENT/MYSQL_CLI |
| `proxy_sessid` | BIGINT UNSIGNED | proxy_sessid |
| `cs_id` | BIGINT UNSIGNED | Client session id (PROXY) |
| `server_node_ip` | VARCHAR(64) | IP OBServer из тела лога |
| `from_proxy` | TINYINT(1) | 1 = через OBProxy |

**UNIQUE KEY** `uk_sess (source, server_ip, cluster_name, session_id, login_time)`

**Индексы:** `idx_login_time`, `idx_user`, `idx_open (logoff_time)`, **`idx_proxy_sessid`**

> `idx_proxy_sessid` критически важен — без него `UPDATE WHERE proxy_sessid=?` при каждом
> LOGOFF вызывает full scan и может повесить транзакцию на часы.

### Таблица `logfiles`

| Колонка | Описание |
|---|---|
| `collector_id` | ID коллектора |
| `file_dir` / `file_name` | Путь к файлу |
| `file_type` | SERVER / PROXY |
| `file_size` | Размер при последней обработке |
| `last_line_num` | **Байтовый offset** (не номер строки) |
| `file_ip` | IP узла-источника |

**UNIQUE KEY** `uq_collector_dir_name (collector_id, file_dir(255), file_name)`

### Таблица `audit_collector_state`

Одна строка (id=1) — глобальный курсор DDL/DCL коллектора.

| Колонка | Описание |
|---|---|
| `id` | PK (всегда 1) |
| `collector_id` | Идентификатор коллектора |
| `last_request_time` | `request_time` последней обработанной записи `GV$OB_SQL_AUDIT` |
| `updated_at` | Wall-clock время последнего успешного сбора (для резервного режима) |

### Таблица `ddl_dcl_audit_log`

| Колонка | Описание |
|---|---|
| `request_id` / `svr_ip` | Ключ дедупликации (`UNIQUE KEY uq_req`) |
| `tenant_id` / `tenant_name` | Тенант |
| `user_id` / `user_name` | Пользователь |
| `db_name` / `stmt_type` | База / тип оператора |
| `query_sql` | SQL (очищен от ведущих `/* ... */` комментариев) |
| `request_ts` | Время выполнения |
| `ret_code` / `elapsed_time` | Результат / время |

### Таблица `ddl_dcl_audit_targets`

Объекты для дополнительного DML-аудита. Управляется через SQL — без перекомпиляции.

| Колонка | Описание |
|---|---|
| `tenant_id` | NULL = все тенанты |
| `db_name` | NULL = любая база |
| `object_name` | Имя таблицы / процедуры / вьюшки |
| `description` | Описание |
| `is_active` | 1=активен, 0=отключён |

### Таблица `rsyslog_cursor`

Курсоры пересылки событий в rsyslog. Три строки: `login`, `logoff`, `ddl`.
Создаются автоматически через `INSERT IGNORE` при первом запуске `RsyslogSender`.

| Колонка | Описание |
|---|---|
| `event_type` | PK: `login` / `logoff` / `ddl` |
| `last_id` | Последний отправленный `id` из соответствующей таблицы |
| `last_time` | Для `logoff`: `logoff_time` последней отправленной записи |
| `updated_at` | Время последней успешной отправки |

---

## Получение списка файлов

```
processDirectory(dirPath, fileType):
  File.listFiles() → фильтр (observer.log*, obproxy.log*)
  loadByDir(collectorId, dirPath) → Map<fileName, LogFileRecord>
  Сортировка: ротированные первыми, активный последним
  → processFile() для каждого
```

---

## Алгоритм определения позиции чтения

```
processFile():
  startOffset = record.lastLineNum  (байтовый offset из logfiles)
  если size не изменился → пропускаем

readAndProcess():
  FileChannel.position(startOffset)  // O(1) прыжок к нужному байту
  BufferedReader → строки до EOF
  record.lastLineNum = channel.position()
```

---

## Обработка ротации файлов

```
shouldResetToStart():
  currentSize < record.fileSize  → ротация (файл стал меньше)
  lastTimestamp старше 10 минут  → читаем сначала

При ротации: offset=0, timestamp/tid/traceId очищаются, fileIp сохраняется.
```

---

## Определение IP узла

**SERVER:** первая строка файла → `address: "192.168.55.205:2882"` → `192.168.55.205`

**PROXY:** строка `server session born` → `local_ip:{192.168.55.200:37288}` → `192.168.55.200`
`updateFileIp()` вызывается сразу при нахождении.

IP OBServer-узла клиента → `sessions.server_node_ip`

---

## Разбор строк лога

### SERVER — `ObServerLineParser`

- **LOGIN** (`MySQL LOGIN`): `direct_client_ip` как `client_ip`; приоритет флагам `from_jdbc/java/oci_client`
- **LOGOFF** (`connection close`): `sessid`, `proxy_sessid`, `tenant_id`, `from_proxy`

### PROXY — `ObProxyLineParser` (stateful)

- **LOGIN_OK**: `server session born` → bornMap; `succ to set proxy_sessid` → событие
- **LOGIN_FAIL**: `error_transfer+OB_MYSQL_COM_LOGIN` → failMap; `client session do_io_close` → событие
- **LOGOFF**: `handle_server_connection_break + COM_QUIT`

---

## Фильтрация служебных логинов

```java
if (ignoredUsers.contains(userName)) return null;  // список из конфига
```

---

## Запись данных в БД

```
LOGIN_OK/FAIL → sessionDao.insertLogin(event, serverIp)
  LOGIN_OK + proxySessid != null → openSessions.add(proxySessid)
LOGOFF → handleLogoff(event)
```

---

## Предотвращение дублей и пропусков

1. `INSERT IGNORE` + UNIQUE KEY
2. Байтовый offset — точное продолжение
3. Ротированные файлы первыми
4. `shouldResetToStart()` — перечитывает при простое > 10 мин
5. `fileIp` не сбрасывается при ротации
6. `updateFileIp()` сразу при нахождении IP

---

## Поддержка нескольких экземпляров сервиса

`collector_id` в `logfiles` изолирует записи разных узлов.

---

## Обработка LOGOFF

### Два пути

```
proxySessid == null || == 0  (прямое подключение):
  UPDATE sessions SET logoff_time=?
  WHERE source='SERVER' AND server_ip=? AND cluster_name='' AND session_id=? AND logoff_time IS NULL

proxySessid > 0  (через OBProxy):
  UPDATE sessions SET logoff_time=?
  WHERE proxy_sessid=? AND logoff_time IS NULL
  -- закрывает SERVER и PROXY строки одновременно
```

> `cluster_name=''` в прямом UPDATE позволяет OceanBase использовать полный префикс UNIQUE KEY.

### Reconciliation

```sql
UPDATE sessions p JOIN sessions s ON s.proxy_sessid = p.proxy_sessid
  AND s.source='SERVER' AND s.is_success=0 AND s.logoff_time IS NOT NULL
SET p.logoff_time=s.logoff_time, p.is_success=0, p.error_code=s.error_code
WHERE p.source='PROXY' AND p.logoff_time IS NULL
```

Порядок: SERVER → PROXY → `syncFailedProxySessions()`

---

## DDL/DCL аудит

### 15.1 Режимы работы

| Режим | Поведение |
|---|---|
| `0` | Отключён |
| `1` | Основной — собирает при каждом запуске |
| `2` | Резервный — только если `updated_at` в `audit_collector_state` старше 2 минут |

**Рекомендация:** один узел с режимом `1`, остальные с `2`.

### 15.2 Алгоритм `collect()`

```
1. last_rt ← SELECT last_request_time FROM audit_collector_state WHERE id=1
2. new_rt  ← SELECT MAX(request_time) FROM GV$OB_SQL_AUDIT
             WHERE is_inner_sql=0 AND request_time > last_rt
3. targets ← SELECT * FROM ddl_dcl_audit_targets WHERE is_active=1
4. INSERT IGNORE INTO ddl_dcl_audit_log
   SELECT ... FROM GV$OB_SQL_AUDIT
   WHERE is_inner_sql=0
     AND request_time > last_rt AND request_time <= new_rt
     AND <глобальные исключения>
     AND (<хардкод фильтр> OR <динамические targets>)
5. UPDATE audit_collector_state SET last_request_time=new_rt, updated_at=NOW(6)
```

`INSERT IGNORE` + UNIQUE KEY `uq_req (svr_ip, request_id)` защищают от дублей
при параллельной работе нескольких коллекторов.

### 15.3 Хардкод фильтрации DDL/DCL событий

Жёстко прописан в коде — не может быть изменён через конфиг или таблицу targets.

**Глобальные исключения** (верхний уровень WHERE, не могут быть обойдены dynamic targets):

```sql
-- OceanBase JDBC добавляет /* comment */ перед каждым запросом, поэтому
-- в GV$OB_SQL_AUDIT query_sql начинается не с ключевого слова — нужен ведущий '%'.
AND query_sql NOT LIKE '%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'
AND query_sql NOT LIKE '%UPDATE sessions SET logoff_time%'
AND query_sql NOT LIKE '%UPDATE sessions p JOIN sessions s%'
```

**Фильтр включения** (внутри AND (...)):

```sql
-- DDL по stmt_type
stmt_type IN (
    'CREATE_TABLE','ALTER_TABLE','DROP_TABLE',
    'CREATE_INDEX','DROP_INDEX',
    'CREATE_VIEW','DROP_VIEW',
    'CREATE_DATABASE','DROP_DATABASE',
    'TRUNCATE_TABLE','RENAME_TABLE',
    'CREATE_TENANT','DROP_TENANT',
    'DROP_USER','RENAME_USER',
    'GRANT','REVOKE','ALTER_USER','SET_PASSWORD'
)

-- User management через LIKE (нет отдельного stmt_type)
OR (
    query_sql LIKE '%CREATE USER%'
    OR query_sql LIKE '%ALTER USER%'
    OR query_sql LIKE '%lock_user(%'
    OR query_sql LIKE '%unlock_user(%'
)

-- DELETE/UPDATE таблиц аудита (требование безопасников)
OR (
    stmt_type IN ('DELETE', 'UPDATE')
    AND (
        query_sql LIKE '%admintools.sessions%'
        OR query_sql LIKE '%admintools.ddl_dcl_audit_log%'
        OR (db_name = 'admintools' AND query_sql LIKE '%sessions%')
        OR (db_name = 'admintools' AND query_sql LIKE '%ddl_dcl_audit_log%')
    )
)
```

`stmt_type NOT IN ('VARIABLE_SET')` — исключает мусор.
`query_sql` очищается от ведущего `/* ... */` через `REGEXP_REPLACE`.

> **Важно:** глобальные исключения вынесены на уровень выше OR-блока намеренно.
> Dynamic targets матчат имена объектов внутри текста нашего же INSERT IGNORE —
> query_sql коллектора содержит все LIKE-условия в своём WHERE, включая названия
> аудируемых таблиц. Если exclusion-паттерны находятся внутри OR, dynamic target
> обходит их через свою ветку OR.

### 15.4 Динамические объекты `ddl_dcl_audit_targets`

Аудит DML-операций над конкретными объектами без перекомпиляции.

**Добавление:**
```sql
INSERT INTO admintools.ddl_dcl_audit_targets (tenant_id, db_name, object_name, description)
VALUES (1002, 'testdb', 'HISTORY_OPERATION', 'Аудит DML по таблице истории операций');
```

**Логика LIKE:**

| `db_name` | Условие в SQL |
|---|---|
| задан | `query_sql LIKE '%db.obj%' OR (db_name='db' AND query_sql LIKE '%obj%')` |
| NULL | `query_sql LIKE '%obj%'` |

**Отключение:** `UPDATE ddl_dcl_audit_targets SET is_active=0 WHERE id=N`

Targets загружаются в начале каждого `collect()` — изменения применяются в следующем прогоне.

### 15.5 Debug-режим

При `LogLevel=DEBUG` перед INSERT выводится полный SQL с подставленными параметрами.
Можно скопировать, заменить INSERT на SELECT и запустить `EXPLAIN` в клиенте.

---

## Управление размером таблиц

**Класс:** `CleanupDao`. Запускается в минуту `config.cleanupMinute`.
Разные минуты на разных узлах (0, 20, 40) — гарантированное удаление раз в час.

**Алгоритм:**
```
COUNT(*) ≤ maxRows → выход
offset = COUNT(*) - maxRows
boundary = SELECT id FROM table ORDER BY id ASC LIMIT 1 OFFSET offset
DELETE FROM table WHERE id < boundary
```

`MAX(id) - maxRows` давал неверный результат из-за пропусков AUTO_INCREMENT.
`COUNT(*) + LIMIT/OFFSET` гарантирует точное число оставшихся строк.

---

## Пересылка в rsyslog

**Класс:** `RsyslogSender`. Вызывается из `Main` в конце каждого прогона (после cleanup),
если `config.rsyslogHost` не пустой.

**Протокол:** UDP, RFC 3164. `<134>` = facility local0, severity info.
Один `DatagramSocket` на весь вызов `send()`.

**Три типа событий с независимыми курсорами:**

| Тип | Источник | Курсор |
|---|---|---|
| `login` | `sessions` (все строки) | `last_id` |
| `logoff` | `sessions` (где `logoff_time IS NOT NULL`) | `(last_time, last_id)` |
| `ddl` | `ddl_dcl_audit_log` | `last_id` |

**Курсор logoff — пара `(logoff_time, id)`** вместо просто `id`. Причина: `logoff_time`
обновляется у существующей строки. Сессия с `id=50` открылась давно, но закрылась
сегодня — курсор только по `id` пропустил бы её если уже прошёл мимо 50.
Курсор по `(logoff_time ASC, id ASC)` гарантирует корректную обработку.

**Батчинг:** все новые записи отправляются за один цикл запуска через несколько
батчей подряд. Размер батча задаётся `BatchSize` в конфиге.

**Поведение при ошибке:** пишем в stderr, курсор не двигается. Данные остаются
в таблицах и будут отправлены при следующем успешном прогоне.

**Формат сообщений:**
```
LOGIN  result=OK|FAIL source=SERVER|PROXY user=... tenant=... client_ip=... session_id=... client_type=... time=...
LOGOFF source=... user=... tenant=... client_ip=... session_id=... login_time=... logoff_time=...
DDL    user=... tenant=... db=... stmt=... ret=... sql=... time=...
```

SQL в DDL-событиях обрезается до 256 символов.

---

## Уровни логирования

| Уровень | Что выводится |
|---|---|
| `ERROR` | Ошибки stderr + `[Main] Done.` |
| `INFO` | + заголовок, итог по каждому изменившемуся файлу, DDL/DCL события, очистка, rsyslog |
| `DEBUG` | + детали файлов, каждый LOGOFF, полный SQL к GV$OB_SQL_AUDIT, каждый UDP-пакет |

```
[Main] Done. v20260422-1 Total time: 943 ms | lines: 37490 | inserted: 3 | logoff: 3 | logoffMiss: 0 | ddlDcl: 1 | cleanedDdlDcl: 0 | cleanedSessions: 0 | rsyslogLogin: 5 | rsyslogLogoff: 4 | rsyslogDdl: 1
```

---

## Поток данных end-to-end

```
config.xml → AppConfigReader.read()
    │
    ▼
Main.main()
    ├─ ObServerLineParser.setIgnoredUsers()
    ├─ DbInitializer.initialize()
    │      └─ CREATE TABLE sessions, logfiles,
    │                      audit_collector_state, ddl_dcl_audit_log,
    │                      ddl_dcl_audit_targets, rsyslog_cursor
    │
    ├─ LogFileProcessor(conn, config)  [autoCommit=true]
    │      ├─ processServerDirs() / processProxyDirs()
    │      └─ for each file:
    │           offset → FileChannel.position() → lines → parse()
    │           LOGIN  → INSERT IGNORE INTO sessions
    │           LOGOFF (proxy>0) → UPDATE sessions WHERE proxy_sessid=?
    │           LOGOFF (proxy=0) → UPDATE sessions WHERE source='SERVER'
    │                               AND server_ip=? AND cluster_name='' AND session_id=?
    │
    ├─ SessionDao.syncFailedProxySessions()
    │
    ├─ DdlDclAuditDao.collect()  (если mode > 0)
    │      ├─ [mode=2] shouldCollectFallback() → updated_at > 2 мин?
    │      ├─ last_rt ← audit_collector_state
    │      ├─ new_rt  ← MAX(request_time) FROM GV$OB_SQL_AUDIT WHERE > last_rt
    │      ├─ targets ← ddl_dcl_audit_targets WHERE is_active=1
    │      └─ INSERT IGNORE INTO ddl_dcl_audit_log
    │           SELECT ... FROM GV$OB_SQL_AUDIT
    │           WHERE request_time > last_rt AND request_time <= new_rt
    │             AND query_sql NOT LIKE '%INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'
    │             AND query_sql NOT LIKE '%UPDATE sessions SET logoff_time%'
    │             AND query_sql NOT LIKE '%UPDATE sessions p JOIN sessions s%'
    │             AND (<хардкод DDL/DCL> OR <динамические targets>)
    │           UPDATE audit_collector_state SET last_request_time=new_rt
    │
    ├─ CleanupDao  (если текущая минута == cleanupMinute)
    │      COUNT(*) → OFFSET → boundary → DELETE WHERE id < boundary
    │
    └─ RsyslogSender.send()  (если rsyslogHost не пустой)
           login  → SELECT FROM sessions WHERE id > last_id      → UDP → update cursor
           logoff → SELECT FROM sessions WHERE logoff_time IS NOT NULL
                    AND (logoff_time > last_time OR (logoff_time = last_time AND id > last_id))
                                                                  → UDP → update cursor
           ddl    → SELECT FROM ddl_dcl_audit_log WHERE id > last_id → UDP → update cursor

[Main] Done. vYYYYMMDD-N | lines | inserted | logoff | logoffMiss | ddlDcl | cleaned* | rsyslogLogin | rsyslogLogoff | rsyslogDdl
```
