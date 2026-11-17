import os
import random
import sys
import time

import pymysql
from prefect import flow, task


def load_env():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


load_env()

MEDALS = ("Bronze", "Silver", "Gold")
TASK_BY_MEDAL = {
    "Bronze": "calc_Bronze",
    "Silver": "calc_Silver",
    "Gold": "calc_Gold",
}
MAX_AGE_SECONDS = 30


def connect():
    password = os.environ.get("MYSQL_PASSWORD", "")
    if not password:
        raise RuntimeError("Немає MYSQL_PASSWORD. Скопіюйте .env.example у .env і впишіть пароль.")
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "217.61.57.46"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "neo_data_admin"),
        password=password,
        database=os.environ.get("MYSQL_DATABASE", "neo_data"),
        connect_timeout=20,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


@task(name="create_table")
def create_table():
    sql = """
    CREATE TABLE IF NOT EXISTS kushneruk.medal_counts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(16) NOT NULL,
        `count` INT NOT NULL,
        created_at DATETIME NOT NULL
    )
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS kushneruk")
            cur.execute(sql)
    print("Таблиця kushneruk.medal_counts готова.")


@task(name="pick_medal")
def pick_medal() -> str:
    medal = random.choice(MEDALS)
    print(f"Обрано медаль: {medal}")
    return medal


@task(name="pick_medal_task")
def pick_medal_task(medal: str) -> str:
    chosen = TASK_BY_MEDAL[medal]
    print(f"Далі виконується лише {chosen}.")
    return chosen


def store_count(medal: str):
    insert_sql = """
    INSERT INTO kushneruk.medal_counts (medal_type, `count`, created_at)
    SELECT %s, COUNT(*), NOW()
    FROM olympic_dataset.athlete_event_results
    WHERE medal = %s
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (medal, medal))
            cur.execute(
                """
                SELECT id, medal_type, `count` AS medal_count, created_at
                FROM kushneruk.medal_counts
                WHERE id = LAST_INSERT_ID()
                """
            )
            row = cur.fetchone()
    print(
        f"{medal}: у olympic_dataset.athlete_event_results знайдено "
        f"{row['medal_count']} рядків. Запис id={row['id']}, created_at={row['created_at']}."
    )


@task(name="calc_Bronze")
def calc_bronze():
    store_count("Bronze")


@task(name="calc_Silver")
def calc_silver():
    store_count("Silver")


@task(name="calc_Gold")
def calc_gold():
    store_count("Gold")


@task(name="generate_delay")
def generate_delay(seconds: int):
    print(f"Затримка {seconds} с перед перевіркою свіжості запису.")
    time.sleep(seconds)


@task(name="check_for_correctness")
def check_for_correctness():
    sql = """
    SELECT id, medal_type, `count` AS medal_count, created_at,
           TIMESTAMPDIFF(SECOND, created_at, NOW()) AS age_seconds
    FROM kushneruk.medal_counts
    ORDER BY id DESC
    LIMIT 1
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    if row is None:
        raise RuntimeError("У kushneruk.medal_counts немає жодного запису.")
    age = int(row["age_seconds"])
    print(
        f"Останній запис id={row['id']} {row['medal_type']} "
        f"count={row['medal_count']} created_at={row['created_at']} вік={age} с."
    )
    if age > MAX_AGE_SECONDS:
        raise RuntimeError(
            f"Запис старший за {MAX_AGE_SECONDS} с (вік {age} с). Сенсор падає."
        )
    print(f"Запис не старший за {MAX_AGE_SECONDS} с.")


@flow(name="olympic_medal_counts", log_prints=True)
def olympic_medal_counts(delay_seconds: int = 5):
    create_table()
    medal = pick_medal()
    chosen = pick_medal_task(medal)
    if chosen == "calc_Bronze":
        calc_bronze()
    elif chosen == "calc_Silver":
        calc_silver()
    elif chosen == "calc_Gold":
        calc_gold()
    generate_delay(delay_seconds)
    check_for_correctness()


if __name__ == "__main__":
    delay = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    olympic_medal_counts(delay_seconds=delay)
