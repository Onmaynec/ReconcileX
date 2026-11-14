# ReconcileX

ReconcileX — backend для сверки банковских выписок, платёжных реестров и внутренних операций. Не проводит реальные платежи и не подключается к банкам. Техническая постановка: PV-FIN-03, редакция 1.0.

**Текущий статус:** версия 0.3.0, Batch Reconciliation & Load Testing, инженерный backend; полная промышленная приёмка ещё не завершена. Полный акт приёмки по ТЗ не подписан; непроверенные требования перечислены в docs/STATUS.md.

## Состав

Python 3.12, FastAPI, SQLAlchemy, PostgreSQL 16, Alembic, отдельный worker, SILO (S3-совместимое хранилище), автоматические резервные копии и Docker Compose. HTTP JSON API документируется автоматически: http://127.0.0.1:8000/docs и /openapi.json.

Денежные суммы хранятся только как целые minor units. Сверка возможна внутри одного счёта, валюты и периода. Автоматическая сверка 1:1 не выбирает произвольную запись при двух одинаково подходящих кандидатах. Группы 1:n и n:1 создаются вручную. Для крупных групп требуется независимый контролёр.

## Локальный запуск

1. Скопируйте .env.example в .env и замените DB_PASSWORD, JWT_SECRET, S3_SECRET_KEY, DEMO_PASSWORD на свои уникальные длинные значения.
2. Выполните: docker compose up --build -d
3. Создайте тестовые справочники и пользователей: docker compose run --rm api python -m app.seed
4. По необходимости заполните 150000 строк через команду demo_import ниже, затем откройте http://127.0.0.1:8000/docs и вызовите POST /api/v1/auth/token с телом JSON: {"username":"operator","password":"пароль из DEMO_PASSWORD"}.
5. Скопируйте access_token в Authorize Swagger UI. Источники — GET /api/v1/sources; периоды — GET /api/v1/periods; профили — GET /api/v1/profiles?source_id=...; правила — GET /api/v1/rules?source_id=....

Демо-роли: operator, controller, analyst, admin, auditor. Все создаются с паролем DEMO_PASSWORD только на демонстрационном стенде. Не запускайте seed с общим паролем на рабочем сервере.

## Демонстрация

1. Оператор вызывает POST /api/v1/imports/preview, указывая source_id, profile_id, period_id, kind (external/internal) и CSV/XLSX.
2. POST /api/v1/imports/commit проводит валидный пакет атомарно; повтор файла SHA-256 по тому же источнику возвращает уже имеющийся batch.
3. POST /api/v1/reconciliation/run?period_id=... с заголовком Idempotency-Key ставит фоновое задание.
4. GET /api/v1/issues?source_id=...&period_id=... показывает неоднозначности.
5. Оператор формирует ручную группу через POST /api/v1/match-groups. Необходимы external_ids, internal_ids, rule_id, period_id, source_id, comment и Idempotency-Key. Большую группу утверждает другой пользователь, controller, через /approve с expected_version.
6. Контролёр вызывает POST /api/v1/periods/{id}/close. Закрытый отчёт /api/v1/reports/period/{id} читается из сохранённого финансового snapshot.

Пример: внутренние +1000,00 и комиссия −20,00 сверяются с внешними 980,00. Все числа внутри хранятся как 100000, −2000 и 98000.

## Разработка и тестирование

При наличии PostgreSQL и настроенной DATABASE_URL:

- python -m pip install -r requirements.txt
- alembic upgrade head
- python -m app.seed
- python -m pytest -q

В CI используется настоящий PostgreSQL 16, а не SQLite. Основные регрессии находятся в tests/test_acceptance.py.

Сгенерировать демонстрационные файлы без импорта: python scripts/generate_demo.py --rows 150000 --out demo-output.

Загрузить все шесть форматов (150000 синтетических записей) сразу в PostgreSQL и SILO (S3-совместимое хранилище) после создания тестовых справочников:

docker compose run --rm api python -m app.demo_import --rows 150000

Повтор команды с тем же количеством строк не должен менять финансовые записи или пакеты. Запуск с другим количеством строк после первой загрузки создаёт другие файлы и может конфликтовать с естественными ключами; для проверки другого набора используйте новый тестовый стенд. Генератор не содержит реальных персональных или банковских данных.

## Безопасность и эксплуатация

API доступен только на loopback хоста в локальном Compose. Для внешнего доступа нужен TLS-терминирующий reverse-proxy. Нельзя использовать значения секретов из .env.example. Пароли Argon2id; источник проверяется сервером на каждом маршруте чтения и изменения. PostgreSQL запрещает UPDATE/DELETE проведённых исходных records, batch и audit.

Инструкция по автоматическому резервированию и проверке восстановления приведена в docs/OPERATIONS.md. Статус реализации, включая непроверенные Must, указан в docs/STATUS.md. Архитектурные решения — docs/ADR.md. Таблица статуса требований — docs/STATUS.md.

Лицензия MIT.


## Обновление v0.2.0

Контейнер backup создаёт резервную копию при старте, затем ежедневно в 03:00 UTC (настраивается BACKUP_HOUR_UTC). Хранятся 7 завершённых архивов по умолчанию, все дампы и S3-объекты проверяются по SHA-256. Доступны команды:

~~~sh
docker compose run --rm backup once
docker compose run --rm backup verify
docker compose run --rm backup restore-check
docker compose run --rm backup status --max-age-hours 36
~~~

Restore-check восстанавливает данные только во временную PostgreSQL БД, проверяет финансовые суммы и ссылки на оригиналы и не удаляет рабочие записи. Backup volume расположен на той же машине: организуйте его копирование на независимый носитель.

Опциональный TLS reverse proxy включается командой docker compose -f compose.yaml -f compose.tls.yaml up --build -d после установки валидных сертификатов в certs/ и TRUSTED_HOSTS в .env. Подробности — docs/SECURITY.md.

Измерения производительности на синтетических массивах: python -m scripts.benchmark --external 80000 --internal 100000 --preview 20000 --trials 3 --output demo-output/performance.json. Сквозная нагрузочная приёмка автоматической сверки 180000 строк ещё не выполнена; см. docs/PERFORMANCE.md.

История изменений — CHANGELOG.md; точный статус требований — docs/STATUS.md.

## Обновление v0.3.0

Для одиночного правила с обязательным reference и допуском по сумме 0 worker использует индексированный поиск строк по дате и пакетные операции PostgreSQL. Обратная неоднозначность и независимое согласование крупных операций сохранены; остальные правила продолжают выполняться прежним алгоритмом.

Для полноценного нагрузочного испытания 180000 **синтетических** строк на изолированной тестовой PostgreSQL:

~~~sh
python -m scripts.e2e_benchmark --pairs 80000 --internal-extra 20000 --external-extra 0 --output demo-output/e2e-180k.json
~~~

Для замера p50/p95 нескольких независимых прогонов добавьте --runs 3; каждый прогон создаёт собственные синтетические записи, поэтому заранее проверьте диск. На GitHub можно запустить workflow «Full Reconciliation Benchmark» вручную в Actions, не загружая тестовыми данными основную БД. Протокол и ограничения — docs/PERFORMANCE.md. В обычном CI проверяется уменьшенный end-to-end набор.
