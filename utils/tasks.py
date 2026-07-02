import logging

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param

FORCE_REBUILD = "force_rebuild"


def force_rebuild_params(description: str | None = None) -> dict:
    """
    Ajoute le paramètre force_rebuild au DAG param.
    Ce paramètre est ensuite utilisé par force_rebuild_requested() lorsqu'on veut
    bypass des short-circuit.
    Pour ajouter d'autres paramètres à un DAG utiliser le format suivant :
    `params={...} | force_rebuild_params(),`

    Args:
        description(str, None); Optionnel. Description du bouton force_rebuild sur l'UI Airflow.
    """
    return {
        FORCE_REBUILD: Param(
            False,
            type="boolean",
            description=description
            or "Reconstruit et pousse sur datagouv quelque soit le résultat du test check_if_modif.",
        )
    }


def force_rebuild_requested(dag_run, context: str = "") -> bool:
    """
    Retourne vrai lorsqu'un DAG est lancé avec le paramètre `force_rebuild` à vrai.
    Utile pour bypass des short-circuit.
    Voire les fonction `check_if_modif()`.
    """
    conf = getattr(dag_run, "conf", None) or {}
    if conf.get(FORCE_REBUILD):
        suffix = f" for {context}" if context else ""
        logging.info(f"force_rebuild=True{suffix}, bypassing short-circuit.")
        return True
    return False


def clean_up_folder(folder: str, recreate: bool = False, **kwargs) -> BashOperator:
    cmd = f"rm -rf {folder}"
    task_id = "clean_up"
    if recreate:
        cmd += f" && mkdir -p {folder}"
        task_id += "_create_folder"
    return BashOperator(task_id=task_id, bash_command=cmd, **kwargs)
