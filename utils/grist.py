import requests
import json
import pandas as pd

from datagouvfr_data_pipelines.config import (
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)

headers = {
    "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}


def erase_table_content(doc_id, table_id, ids=None):
    """Empty some (if specified) or all rows of a table. Doesn't touch the columns"""
    if ids is None:
        records = requests.get(
            GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/records",
            headers=headers,
        ).json()["records"]
    r = requests.post(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/data/delete",
        headers=headers,
        json=ids or [r["id"] for r in records]
    )
    r.raise_for_status()


def rename_table_columns(doc_id, table_id, new_columns):
    """
    Delete and recreate columns in the table
    Intended to be used when the table is empty prevent unwanted behaviour
    'new_columns' can be a list or a dict:
        - list: ids and labels are set to the same values
        - dict: the exact grist pattern is expected (see how it's constructed for the list case)
    """
    current_columns = requests.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns",
        headers=headers,
    ).json()["columns"]
    ids = [c["id"] for c in current_columns]
    for i in ids:
        r = requests.delete(
            GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns/{i}",
            headers=headers,
        )
        r.raise_for_status()
    if isinstance(new_columns, list):
        _columns = {"columns": [{
            "id": col,
            "fields": {
                "label": col
            },
        } for col in new_columns]}
    elif isinstance(new_columns, dict):
        _columns = new_columns
    else:
        raise ValueError("'new_columns' should be a list or dict")
    r = requests.post(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns",
        headers=headers,
        json=_columns,
    )
    r.raise_for_status()
    return r.json()


def chunkify(df: pd.DataFrame, chunk_size: int = 100):
    start = 0
    length = df.shape[0]
    if length <= chunk_size:
        yield df
        return
    while start + chunk_size <= length:
        yield df[start:chunk_size + start]
        start = start + chunk_size
    if start < length:
        yield df[start:]


def recordify(df, returned_columns):
    if returned_columns and df.columns.to_list() != returned_columns:
        df = df.rename(
            {col: new_col for col, new_col in zip(df.columns.to_list(), returned_columns)},
            axis=1,
        )
    records = json.loads(df.to_json(orient="records"))
    return {"records": [{"fields": r} for r in records]}


def df_to_grist(df, doc_id, table_id):
    """
    Uploads a pd.DataFrame to a grist table (in chunks to avoid 413 errors)
    If the table(_id) already exists, replace it entirely, otherwise create it
    """
    tables = requests.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/",
        headers=headers,
    )
    tables.raise_for_status()
    tables = [t["id"] for t in tables.json()['tables']]
    returned_columns = None
    if table_id not in tables:
        print(f"Creating table '{doc_id}/tables/{table_id}' in grist")
        table = {"tables": [
            {
                "id": table_id,
                "columns": [{
                    "id": col,
                    "fields": {
                        "label": col
                    },
                } for col in df.columns]
            }
        ]}
        # creating table
        r = requests.post(
            GRIST_API_URL + "docs/" + doc_id + "/tables",
            headers=headers,
            json=table
        )
        r.raise_for_status()
    else:
        print(f"Updating table '{doc_id}/tables/{table_id}' (already exists) in grist")
        erase_table_content(doc_id, table_id)
        # some column names are not accepted by grist (e.g 'id'), so we get the new names
        # to potentially replace them so that the upload doesn't crash
        returned_columns = rename_table_columns(doc_id, table_id, df.columns.to_list())["columns"]
        returned_columns = [c["id"] for c in returned_columns]
    # fill it up
    res = []
    for chunk in chunkify(df):
        r = requests.post(
            GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/records",
            headers=headers,
            json=recordify(chunk, returned_columns)
        )
        r.raise_for_status()
        res += r.json()["records"]
    return res
