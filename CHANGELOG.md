# OBAuditor — История изменений

## v20260415 (2026-04-21)

**Кастомные объекты аудита**

Добавлена таблица `ddl_dcl_audit_targets`. Позволяет аудировать DML-операции над конкретными
таблицами, процедурами и вьюшками без перекомпиляции:

```sql
INSERT INTO admintools.ddl_dcl_audit_targets (tenant_id, db_name, object_name, description)
VALUES (1002, 'testdb', 'HISTORY_OPERATION', 'Аудит DML по таблице истории операций');
```

Изменения применяются при следующем запуске сервиса. Для отключения: `SET is_active = 0`.

**Аудит DELETE/UPDATE таблиц журналов**

В хардкод фильтра добавлен перехват `DELETE` и `UPDATE` по таблицам `admintools.sessions`
и `admintools.ddl_dcl_audit_log`. Служебные UPDATE сервиса (закрытие логоффов, reconciliation)
исключены автоматически.

---

## v20260409 (2026-04-09)

**DDL/DCL аудит из GV$OB_SQL_AUDIT**

Сбор DDL/DCL событий: CREATE/DROP/ALTER TABLE/INDEX/VIEW/DATABASE, GRANT, REVOKE, SET_PASSWORD,
CREATE USER, ALTER USER, lock_user(), unlock_user(). Режимы: основной (1) и резервный (2).

**Управление размером таблиц**

Плановое удаление старых строк по минуте часа. Разные минуты на разных узлах (0, 20, 40)
гарантируют удаление минимум раз в час.

---

## v20260316 (2026-03-16)

**Уровни логирования:** DEBUG / INFO / ERROR через `config.xml`.

**Список игнорируемых УЗ в конфиге:** `<IgnoredUsers>` — без перекомпиляции.

---

## v20260311 (2026-03-11)

**Обработка LOGOFF:** закрытие сессий по `proxy_sessid` и по `session_id` для прямых подключений.
Reconciliation PROXY-строк для неудачных логинов (`syncFailedProxySessions`).

---

## v20260309 (2026-03-09)

Первоначальный релиз: чтение логов OBServer и OBProxy, аудит логинов/логоффов,
байтовый offset, ротация файлов, поддержка нескольких коллекторов, CI через GitHub Actions.