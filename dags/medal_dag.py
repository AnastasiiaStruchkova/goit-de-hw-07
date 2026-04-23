from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import random
import time

#  Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Custom sensor: перевіряє, що останній запис не старший за 30 сек
class RecentRecordSensor(BaseSensorOperator):
    """
    Повертає True, якщо найновіший запис у таблиці medals
    було вставлено не більше ніж 30 секунд тому.
    """

    @apply_defaults
    def __init__(self, mysql_conn_id: str = "goit_mysql_db_Anastasiia_Struchkova", table: str = "anastasiia_struchkova_medals", **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.table = table

    def poke(self, context):
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        sql = f"""
            SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW())
            FROM {self.table}
        """
        result = hook.get_first(sql)
        if result is None or result[0] is None:
            self.log.info("Таблиця порожня — sensor fail")
            return False
        age_seconds = result[0]
        self.log.info("Вік останнього запису: %s сек", age_seconds)
        return age_seconds <= 30


# Python-функції для завдань
def _pick_medal(**context):
    """Task 2: випадково обирає тип медалі."""
    medal = random.choice(["Bronze", "Silver", "Gold"])
    context["ti"].xcom_push(key="medal", value=medal)
    return medal


def _branch(**context):
    """Task 3: повертає id наступного завдання залежно від медалі."""
    medal = context["ti"].xcom_pull(task_ids="pick_medal", key="medal")
    branch_map = {
        "Bronze": "count_bronze",
        "Silver": "count_silver",
        "Gold":   "count_gold",
    }
    return branch_map[medal]


def _count_and_insert(medal_type: str, **context):
    """Tasks 4a/4b/4c: рахує записи й вставляє рядок у таблицю anastasiia_struchkova_medals."""
    hook = MySqlHook(mysql_conn_id="goit_mysql_db_Anastasiia_Struchkova")

    # Підраховуємо кількість записів для даного типу медалі
    count_sql = """
        SELECT COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = %s
    """
    count = hook.get_first(count_sql, parameters=(medal_type,))[0]

    # Вставляємо результат у нашу таблицю
    insert_sql = """
        INSERT INTO anastasiia_struchkova_medals (medal_type, count, created_at)
        VALUES (%s, %s, NOW())
    """
    hook.run(insert_sql, parameters=(medal_type, count))


def _sleep(**context):
    """Task 5: затримка виконання."""
    sleep_seconds = 10          # змініти на менше, щоб sensor пройшов успішно
    time.sleep(sleep_seconds)


# DAG
with DAG(
    dag_id="medal_count_dag",
    default_args=default_args,
    description="Домашнє завдання: медалі з Olympic dataset",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["homework", "mysql"],
) as dag:

    # Task 1 — CREATE TABLE
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="goit_mysql_db_Anastasiia_Struchkova",
        sql="""
            CREATE TABLE IF NOT EXISTS anastasiia_struchkova_medals (
                id         INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10)  NOT NULL,
                count      INT          NOT NULL,
                created_at DATETIME     NOT NULL
            );
        """,
    )

    # Task 2 — вибір випадкової медалі
    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=_pick_medal,
    )

    # Task 3 — розгалуження
    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=_branch,
    )

    # Tasks 4a/4b/4c — підрахунок і запис
    count_bronze = PythonOperator(
        task_id="count_bronze",
        python_callable=_count_and_insert,
        op_kwargs={"medal_type": "Bronze"},
    )

    count_silver = PythonOperator(
        task_id="count_silver",
        python_callable=_count_and_insert,
        op_kwargs={"medal_type": "Silver"},
    )

    count_gold = PythonOperator(
        task_id="count_gold",
        python_callable=_count_and_insert,
        op_kwargs={"medal_type": "Gold"},
    )

    # Task 5 — затримка
    sleep_task = PythonOperator(
        task_id="sleep_35_sec",
        python_callable=_sleep,
        trigger_rule="one_success",   # виконується після будь-якого успішного count_*
    )

    # Task 6 — сенсор перевірки свіжого запису
    check_recent = RecentRecordSensor(
        task_id="check_recent_record",
        mysql_conn_id="goit_mysql_db_Anastasiia_Struchkova",
        table="anastasiia_struchkova_medals",
        poke_interval=5,
        timeout=60,
        mode="poke",
    )

    # Залежності
    create_table >> pick_medal >> branch_task
    branch_task >> [count_bronze, count_silver, count_gold]
    [count_bronze, count_silver, count_gold] >> sleep_task >> check_recent
