from airflow.operators.bash import BashOperator


def clean_up_folder(folder: str, recreate: bool = False, **kwargs) -> BashOperator:
    cmd = f"rm -rf {folder}"
    task_id = "clean_up"
    if recreate:
        cmd += f" && mkdir -p {folder}"
        task_id += "_create_folder"
    return BashOperator(task_id=task_id, bash_command=cmd, **kwargs)
