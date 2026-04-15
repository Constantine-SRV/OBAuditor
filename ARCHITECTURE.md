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
    - 15.2 [Почему RANGE SCAN](#152-почему-range-scan-а-не-full-scan)
    - 15.3 [Таблица курсоров ddl_dcl_audit_checkpoint](#153-таблица-курсоров-ddl_dcl_audit_checkpoint)
    - 15.4 [Алгоритм collect()](#154-алгоритм-collect)
    - 15.5 [Хардкод фильтрации](#155-хардкод-фильтрации-ddldcl-событий)
    - 15.6 [Динамические объекты ddl_dcl_audit_targets](#156-динамические-объекты-ddl_dcl_audit_targets)
    - 15.7 [Debug-режим](#157-debug-режим)
16. [Управление размером таблиц](#управление-размером-таблиц)
17. [Уровни логирования](#уровни-логирования)
18. [Поток данных end-to-end](#поток-данных-end-to-end)

---

## Обзор

OBAuditor читает логи OceanBase (SERVER и PROXY), извлекает события логина/логоффа,
собирает DDL/DCL операции из `GV$OB_SQL_AUDIT` и записывает всё в базу `admintools`.

**Стек:** Java 21 (Temurin), OceanBase JDBC, стандартный javax.xml (нет внешних зависимостей кроме драйвера).

**Однопоточная модель.** Сервис намеренно однопоточный: лог-файлы одного узла обрабатываются
строго последовательно. В реальном развёртывании **один экземпляр = один узел**. На каждом
узле кластера запускается свой экземпляр — все пишут в одну базу `admintools`, разделяясь по `collector_id`.

**Управление транзакциями.** `autoCommit=true` — каждый INSERT/UPDATE коммитится немедленно,
предотвращая многочасовые блокировки при обрыве соединения. Для атомарных операций
(очистка таблиц) используется локальный `setAutoCommit(false)` → `commit()` → восстановление.

---

## Структура проекта

```
src/
├── Main.java
├── model/
│   ├── AppConfig.java               # logLevel, ignoredUsers, ddlDclAuditMode, cleanup settings
│   ├── AppConfigReader.java
│   ├── ConnectionConfig.java        # ob_query_timeout=30s
│   ├── LoginEvent.java
│   ├── LogFileRecord.java
│   └── PasswordEnricher.java
├── db/
│   ├── DbInitializer.java           # Создаёт все таблицы включая ddl_dcl_audit_targets
│   ├── LogFileDao.java
│   ├── SessionDao.java              # insertLogin, updateLogoff, updateLogoffDirect
│   ├── DdlDclAuditDao.java          # RANGE SCAN + динамические targets
│   └── CleanupDao.java              # COUNT(*)+OFFSET алгоритм
└── log/
    ├── LogFileProcessor.java
    ├── LogLineHandler.java
    ├── LogLine.java
    ├── ObServerLineParser.java
    └── ObProxyLineParser.java
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

Создаёт базу `admintools` и таблицы (проверка через `information_schema`):

1. `sessions` — логины/логоффы
2. `logfiles` — состояние чтения файлов
3. `ddl_dcl_audit_checkpoint` — курсоры DDL/DCL коллектора
4. `ddl_dcl_audit_log` — DDL/DCL события
5. `ddl_dcl_audit_targets` — объекты для дополнительного DML-аудита

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

> `idx_proxy_sessid` критически важен — без него UPDATE при LOGOFF вызывает full scan
> и может повесить транзакцию на часы.

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

### Таблица `ddl_dcl_audit_checkpoint`

| Колонка | Описание |
|---|---|
| `svr_ip` | IP OBServer |
| `svr_port` | **RPC-порт (2882)** из `DBA_OB_UNITS.svr_port` |
| `tenant_id` | ID тенанта |
| `last_request_id` | Последний обработанный request_id |
| `updated_at` | Время последнего сбора (для резервного режима) |

**PRIMARY KEY (svr_ip, svr_port, tenant_id)** — совпадает с ключом `GV$OB_SQL_AUDIT`.

### Таблица `ddl_dcl_audit_log`

| Колонка | Описание |
|---|---|
| `request_id` / `svr_ip` | Ключ дедупликации |
| `tenant_id` / `tenant_name` | Тенант |
| `user_id` / `user_name` | Пользователь |
| `db_name` / `stmt_type` | База / тип оператора |
| `query_sql` | SQL (очищен от `/* ... */` комментариев) |
| `request_ts` | Время выполнения |
| `ret_code` / `elapsed_time` | Результат / время |

**UNIQUE KEY** `uq_req (svr_ip, request_id)`

### Таблица `ddl_dcl_audit_targets`

Объекты для дополнительного DML-аудита. Управляется через SQL — без перекомпиляции.

| Колонка | Описание |
|---|---|
| `id` | PK |
| `tenant_id` | NULL = все тенанты |
| `db_name` | NULL = любая база |
| `object_name` | Имя таблицы / процедуры / вьюшки |
| `description` | Описание (для чего аудируем) |
| `is_active` | 1=активен, 0=отключён |
| `created_at` | Дата добавления |

---

## Получение списка файлов

```
processDirectory(dirPath, fileType):
  File.listFiles() → фильтр по имени (observer.log*, obproxy.log*)
  loadByDir(collectorId, dirPath) → Map<fileName, LogFileRecord>
  Сортировка: ротированные первыми, активный последним
  → processFile() для каждого
```

---

## Алгоритм определения позиции чтения

```
processFile():
  startOffset = record.lastLineNum (байтовый offset)
  если size не изменился → пропускаем

readAndProcess():
  FileChannel.position(startOffset)  // O(1) прыжок
  BufferedReader → строки до EOF
  record.lastLineNum = channel.position()
```

---

## Обработка ротации файлов

```
shouldResetToStart():
  currentSize < record.fileSize     → ротация
  lastTimestamp старше 10 минут     → читаем сначала

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
// if ("127.0.0.1".equals(directClientIp)) return null;  // опционально
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
| `1` | Основной — собирает всегда |
| `2` | Резервный — собирает если `MIN(updated_at)` > 2 мин (основной упал) |

**Рекомендация:** один узел с режимом `1`, остальные с `2`.

### 15.2 Почему RANGE SCAN, а не FULL SCAN

`GV$OB_SQL_AUDIT` (`__all_virtual_sql_audit`) имеет PK `(svr_ip, svr_port, tenant_id, request_id)`.

Фильтр по `request_time` → **TABLE FULL SCAN** (100k строк всегда).

Фильтр по всем 4 компонентам PK → **TABLE RANGE SCAN**:
```sql
WHERE svr_ip = ? AND svr_port = ? AND tenant_id = ? AND request_id > ?
```

### 15.3 Таблица курсоров `ddl_dcl_audit_checkpoint`

Один ряд на `(svr_ip, svr_port, tenant_id)`. `svr_port` = RPC-порт (2882) из `DBA_OB_UNITS.svr_port` — именно его использует `GV$OB_SQL_AUDIT` в PK.

`ensureCursors()` синхронизирует таблицу с кластером при каждом вызове:
- `INSERT IGNORE` для новых комбинаций из `DBA_OB_UNITS JOIN __all_server`
- `DELETE` выбывших серверов/тенантов

### 15.4 Алгоритм `collect()`

```
1. ensureCursors()
2. loadCheckpoints() → List<CheckpointRow>
3. loadTargets() → List<AuditTarget>  ← из ddl_dcl_audit_targets

4. для каждого CheckpointRow:
   targets = filterTargets(all, cp.tenantId)  ← tenant_id=NULL или совпадает
   buildInsertSql(targets)  ← динамически с OR-условиями из targets

   INSERT IGNORE INTO ddl_dcl_audit_log
   SELECT ... FROM GV$OB_SQL_AUDIT
   WHERE svr_ip=? AND svr_port=? AND tenant_id=? AND request_id > ?
     AND <фильтр DDL/DCL хардкод + динамические targets>
   ORDER BY request_id LIMIT 5000

   если inserted > 0:
     max_id = SELECT MAX(request_id) FROM ddl_dcl_audit_log
              WHERE svr_ip=? AND tenant_id=? AND request_id > last  ← фильтр по tenant_id!
     UPDATE checkpoint SET last_request_id=max_id

   если inserted == 5000 → catch-up (повторить этот чекпоинт)
```

> **Важно:** запрос за новым `max_id` обязательно фильтрует по `tenant_id`.
> Без этого значения из разных тенантов смешиваются и курсор может уйти далеко вперёд,
> пропуская события текущего тенанта.

### 15.5 Хардкод фильтрации DDL/DCL событий

Жёстко прописан в коде — не может быть изменён через конфиг (требование безопасников):

```sql
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
OR (
    query_sql NOT LIKE 'INSERT IGNORE INTO admintools.ddl_dcl_audit_log%'
    AND (
        query_sql LIKE '%CREATE USER%'
        OR query_sql LIKE '%ALTER USER%'
        OR query_sql LIKE '%lock_user(%'
        OR query_sql LIKE '%unlock_user(%'
    )
)
```

`stmt_type NOT IN ('VARIABLE_SET')` — исключаем мусор.
`query_sql` очищается от leading `/* ... */` через `REGEXP_REPLACE`.

### 15.6 Динамические объекты `ddl_dcl_audit_targets`

Для аудита DML-операций над конкретными объектами (таблицами, процедурами) — без перекомпиляции.

**Добавление объекта:**
```sql
INSERT INTO admintools.ddl_dcl_audit_targets (tenant_id, db_name, object_name, description)
VALUES (1002, 'testdb', 'HISTORY_OPERATION', 'Аудит DML по таблице истории операций');
```

**Логика LIKE для каждой записи:**

| `db_name` | Условие в SQL |
|---|---|
| задан | `query_sql LIKE '%db.obj%' OR (db_name='db' AND query_sql LIKE '%obj%')` |
| NULL | `query_sql LIKE '%obj%'` |

**Фильтрация по тенанту:** если `tenant_id` задан — применяется только к соответствующему чекпоинту. NULL = все тенанты.

**Отключение без удаления:** `UPDATE ddl_dcl_audit_targets SET is_active=0 WHERE id=N`

Targets загружаются в начале каждого `collect()` — изменения применяются в следующем прогоне.

### 15.7 Debug-режим

При `LogLevel=DEBUG` выводится полный SQL с подставленными параметрами:

```
[DdlDclAuditDao] SQL:
INSERT IGNORE INTO admintools.ddl_dcl_audit_log (...)
SELECT ... FROM oceanbase.GV$OB_SQL_AUDIT
WHERE svr_ip    = '192.168.55.205'
  AND svr_port  = 2882
  AND tenant_id = 1002
  AND request_id > 142256791
  AND ...
  OR (query_sql LIKE '%testdb.HISTORY_OPERATION%'
      OR (db_name = 'testdb' AND query_sql LIKE '%HISTORY_OPERATION%'))
ORDER BY request_id LIMIT 5000
```

Заменить `INSERT IGNORE INTO ... SELECT` на просто `SELECT` или добавить `EXPLAIN` — и запустить в клиенте.

---

## Управление размером таблиц

**Класс:** `CleanupDao`. Запускается в минуту `config.cleanupMinute`.
Расставляя `0`, `20`, `40` на разных узлах — гарантированное удаление раз в час.

**Алгоритм (корректный для AUTO_INCREMENT с пропусками):**
```
COUNT(*) ≤ maxRows → выход
offset = COUNT(*) - maxRows
boundary = SELECT id FROM table ORDER BY id ASC LIMIT 1 OFFSET offset
DELETE FROM table WHERE id < boundary
```

Старый `MAX(id) - maxRows` давал неверный результат из-за пропусков AUTO_INCREMENT.

---

## Уровни логирования

| Уровень | Что выводится |
|---|---|
| `ERROR` | Ошибки stderr + `[Main] Done.` |
| `INFO` | + заголовок, итог по каждому изменившемуся файлу, DDL/DCL события, очистка |
| `DEBUG` | + детали файлов, каждый LOGOFF, полный SQL к GV$OB_SQL_AUDIT с параметрами |

```
[Main] Done. v20260415-2 Total time: 943 ms | lines: 37490 | inserted: 3 | logoff: 3 | logoffMiss: 0 | ddlDcl: 1 | cleanedDdlDcl: 0 | cleanedSessions: 0
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
    │                      ddl_dcl_audit_checkpoint, ddl_dcl_audit_log,
    │                      ddl_dcl_audit_targets
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
    │      ├─ ensureCursors()  → INSERT IGNORE/DELETE в checkpoint
    │      ├─ loadTargets()    → List<AuditTarget> из ddl_dcl_audit_targets
    │      └─ for each (svr_ip, svr_port, tenant_id, last_request_id):
    │           targets = filterTargets(all, tenant_id)
    │           INSERT IGNORE INTO ddl_dcl_audit_log
    │           SELECT ... FROM GV$OB_SQL_AUDIT
    │           WHERE svr_ip=? AND svr_port=? AND tenant_id=?
    │             AND request_id > ?            ← TABLE RANGE SCAN
    │             AND <хардкод DDL/DCL>
    │             OR  <динамические targets>
    │           ORDER BY request_id LIMIT 5000
    │           [catch-up если inserted==5000]
    │           max_id = MAX(request_id) WHERE svr_ip=? AND tenant_id=? AND request_id>last
    │           UPDATE checkpoint SET last_request_id=max_id
    │
    └─ CleanupDao  (если минута совпадает)
           COUNT(*) → OFFSET → boundary → DELETE WHERE id < boundary

[Main] Done. vYYYYMMDD-N | lines | inserted | logoff | logoffMiss | ddlDcl | cleaned*
```
