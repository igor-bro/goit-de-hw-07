# goit-de-hw-07

Потік Prefect читає `olympic_dataset.athlete_event_results` на курсовій MySQL і пише підсумок у `kushneruk.medal_counts`. Завдання дозволяє Prefect замість Airflow, тому оркестрація локальна, а база спільна.

Пароль лежить у `.env`. Файл у git не потрапляє. Зразок без пароля: `.env.example`.

## Запуск

```bash
py -3.11 -m pip install -r requirements.txt
prefect server start
```

Інтерфейс: http://127.0.0.1:4200

В іншому вікні, з цієї теки:

```bash
py -3.11 medal_flow.py 5
py -3.11 medal_flow.py 35
```

Аргумент це секунди `time.sleep` перед перевіркою. 5 секунд менше за поріг 30, потік завершується успішно. 35 секунд більше за поріг, задача `check_for_correctness` падає.

Таблиця:

```bash
py -3.11 -c "import medal_flow; c=medal_flow.connect(); cur=c.cursor(); cur.execute('SELECT id, medal_type, count, created_at FROM kushneruk.medal_counts ORDER BY id');
[print(r) for r in cur.fetchall()]"
```
