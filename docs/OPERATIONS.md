# Эксплуатация и восстановление

Проверки: GET /health/live, GET /health/ready, docker compose ps, docker compose logs -f api worker.

Для резервного копирования нужны и PostgreSQL, и MinIO. Их снимки должны соответствовать одной согласованной точке времени. Перед архивом остановите поступление новых импортов и завершите worker.

Экспорт PostgreSQL:
docker compose exec -T postgres pg_dump -U reconcilex -Fc reconcilex > reconcilex.backup

Отдельно сохраните volume minio_data или содержимое bucket reconcilex с контролем SHA-256. Восстановление на изолированном стенде: pg_restore -U reconcilex -d reconcilex --clean --if-exists reconcilex.backup. Эта команда перезаписывает объекты целевой БД; никогда не запускайте без отдельной резервной копии.

Изменение правил после закрытия допустимо, но не меняет frozen snapshot. При аварии worker job с истекшим lease может быть обработан повторно без подтверждений-дублей. При 409 на записи проверьте natural key или активную group; не редактируйте DB вручную.

Production: поставить TLS reverse proxy, раздельные учётные записи, шифрование/хранилище ключей, регулярные бэкапы, восстановление на тестовом стенде, мониторинг доступности. В текущем Compose автоматическое ежедневное резервирование не настроено.
