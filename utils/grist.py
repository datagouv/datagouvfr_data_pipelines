import requests
import json
import pandas as pd

from datagouvfr_data_pipelines.config import (
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)

GRIST_UI_URL = GRIST_API_URL.replace("api", "o/datagouv")
headers = {
    "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}


def handle_grist_error(response):
    try:
        response.raise_for_status()
    except Exception:
        raise Exception(f"Grist error: '{response.json()['error']}'")


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
    handle_grist_error(r)


def rename_table_columns(doc_id, table_id, new_columns):
    """
    Delete and recreate columns in the table
    Intended to be used when the table is empty to prevent unwanted behaviour
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
        handle_grist_error(r)
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
    handle_grist_error(r)
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


def get_real_columns(doc_id, table_id, df):
    r = requests.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns",
        headers=headers,
    ).json()
    returned_columns = {
        c["fields"]["label"]: c["id"] for c in r["columns"]
    }
    if list(returned_columns.keys()) != df.columns.to_list():
        raise ValueError(
            "Columns of the existing table don't match with sent data:\n"
            f"- existing: {list(returned_columns.keys())}\n"
            f"- sent: {df.columns.to_list()}"
        )
    # some column ids are not accepted by grist (e.g 'id'), so we get the new ids
    # to potentially replace them so that the upload doesn't crash
    return list(returned_columns.values())


def df_to_grist(df, doc_id, table_id, append=False):
    """
    Uploads a pd.DataFrame to a grist table (in chunks to avoid 413 errors)
    If the table(_id) already exists:
        - if append: append records at the end of the table (only if columns match)
        - else: replace it entirely
    Otherwise create it
    """
    tables = requests.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/",
        headers=headers,
    )
    handle_grist_error(tables)
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
        # create the table
        r = requests.post(
            GRIST_API_URL + "docs/" + doc_id + "/tables",
            headers=headers,
            json=table
        )
        handle_grist_error(r)
        returned_columns = get_real_columns(doc_id, table_id, df)
    else:
        if append:
            print(f"Appending records to '{doc_id}/tables/{table_id}' in grist")
            returned_columns = get_real_columns(doc_id, table_id, df)
        else:
            print(f"Erasing and refilling '{doc_id}/tables/{table_id}' in grist")
            erase_table_content(doc_id, table_id)
            # some column ids are not accepted by grist (e.g 'id'), so we get the new ids
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
        handle_grist_error(r)
        res += r.json()["records"]
    return res
