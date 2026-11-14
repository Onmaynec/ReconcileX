# Шесть синтетических форматов

python scripts/generate_demo.py --rows 150000 --out demo-output

Скрипт создаёт шесть независимых файлов, всего 150000 валидных строк, а также invalid_rows.csv с 12 ошибками, duplicate_rows.csv с повтором естественного ключа и manifest.json с профилями. Данные не содержат сведений реальных клиентов.

| Имя файла | Месяц | Источник | Тип | Формат |
| --- | --- | --- | --- | --- |
| 01_external_rub_aug.csv | 2026-08 | demo-rouble | external | UTF-8, запятая, ISO-дата |
| 02_internal_rub_aug.csv | 2026-08 | demo-rouble | internal | CP1251, точка с запятой, русские названия колонок |
| 03_external_eur_sep.csv | 2026-09 | demo-euro | external | UTF-8, табуляция |
| 04_internal_eur_sep.csv | 2026-09 | demo-euro | internal | UTF-8, вертикальная черта |
| 05_external_rub_oct.xlsx | 2026-10 | demo-rouble | external | XLSX |
| 06_internal_rub_oct.xlsx | 2026-10 | demo-rouble | internal | XLSX |

В каждом профиле manifest содержатся mapping, date_format, decimal_separator, encoding и delimiter. Для загрузки через API создайте соответствующий ImportProfile через POST /api/v1/profiles. Период и источник должны совпадать со строкой в manifest. Для сценария комиссии используются августовские записи +1000,00 и −20,00 против внешней +980,00.

В течение одного месяца у одной пары операций может различаться день проводки; для автоматической сверки используйте подходящее окно дат. Дубли и невалидные строки проверяются отдельным файлом и должны отвергаться целиком.
