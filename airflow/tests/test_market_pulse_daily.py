from datetime import timedelta
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = Path("/opt/airflow/dags")

def load_dag():
    dag_bag = DagBag(dag_folder=str(DAGS_FOLDER))
    assert dag_bag.import_errors == {}
    return dag_bag.get_dag("market_pulse_daily")


def test_dag_imports() -> None:
    dag = load_dag()
    assert dag is not None


def test_dag_tasks_and_dependencies() -> None:
    dag = load_dag()
    assert set(dag.task_ids) == {
        "resolve_as_of",
        "collect_batch",
        "inspect_batch_summary",
        "ready_for_warehouse",
    }

    assert dag.get_task("resolve_as_of").downstream_task_ids == {"collect_batch"}
    assert dag.get_task("collect_batch").downstream_task_ids == {"inspect_batch_summary"}
    assert dag.get_task("inspect_batch_summary").downstream_task_ids == {
        "ready_for_warehouse"
    }

    collect_task = dag.get_task("collect_batch")
    assert collect_task.retries == 2
    assert collect_task.execution_timeout == timedelta(minutes=30)
