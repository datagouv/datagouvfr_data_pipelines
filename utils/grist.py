from typing import Optional, Union
import requests
import json
import pandas as pd

from datagouvfr_data_pipelines.utils.retry import RequestRetry
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


def handle_grist_error(response: requests.Response):
    try:
        response.raise_for_status()
    except Exception:
        raise Exception(f"Grist error: '{response.json()['error']}'")


def erase_table_content(doc_id: str, table_id: str, ids: Optional[list] = None):
    """Empty some (if specified) or all rows of a table. Doesn't touch the columns"""
    if ids is None:
        records = RequestRetry.get(
            GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/records",
            headers=headers,
        ).json()["records"]
    r = RequestRetry.post(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/data/delete",
        headers=headers,
        json=ids or [r["id"] for r in records]
    )
    handle_grist_error(r)


def rename_table_columns(doc_id: str, table_id: str, new_columns: Union[list, dict]):
    """
    Delete and recreate columns in the table
    Intended to be used when the table is empty to prevent unwanted behaviour
    'new_columns' can be a list or a dict:
        - list: ids and labels are set to the same values
        - dict: the exact grist pattern is expected (see how it's constructed for the list case)
    """
    current_columns = RequestRetry.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns",
        headers=headers,
    ).json()["columns"]
    ids = [c["id"] for c in current_columns]
    for i in ids:
        r = RequestRetry.delete(
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
    r = RequestRetry.post(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns",
        headers=headers,
        json=_columns,
    )
    handle_grist_error(r)
    return {label: k['id'] for label, k in zip(new_columns, r.json()["columns"])}


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


def recordify(df: pd.DataFrame, returned_columns: Optional[dict]):
    """Renames columns (if ids have been changed by grist) and wraps the content as expected"""
    if returned_columns:
        df = df.rename(returned_columns, axis=1)
    records = json.loads(df.to_json(orient="records"))
    return {"records": [{"fields": r} for r in records]}


def get_columns_mapping(doc_id: str, table_id: str, id_to_label: bool):
    # some column ids are not accepted by grist (e.g 'id'), so we get the new ids
    # to potentially replace them so that the upload doesn't crash
    r = RequestRetry.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns",
        headers=headers,
    )
    handle_grist_error(r)
    if id_to_label:
        return {
            c["id"]: c["fields"]["label"] for c in r.json()["columns"]
        }
    else:
        return {
            c["fields"]["label"]: c["id"] for c in r.json()["columns"]
        }


def handle_and_return_columns(doc_id: str, table_id: str, df: pd.DataFrame, append: Union[bool, str]):
    """
    Handles cases where the df has more/less columns than the table:
        - more: add missing columns (empty) for the records to be uploaded
        - less: grist handles it (but adds default values to rows)
    """
    returned_columns = get_columns_mapping(doc_id, table_id, id_to_label=False)
    if append == 'exact' and sorted(list(returned_columns.keys())) != sorted(df.columns.to_list()):
        raise ValueError(
            "Columns of the existing table don't match with sent data:\n"
            f"- existing: {sorted(list(returned_columns.keys()))}\n"
            f"- sent: {sorted(df.columns.to_list())}"
        )
    elif append == 'lazy':
        columns_to_add = [c for c in df.columns if c not in returned_columns.keys()]
        if columns_to_add:
            print(f"Adding missing columns: {columns_to_add}")
            columns_to_add = {"columns": [{
                "id": col,
                "fields": {
                    "label": col
                },
            } for col in columns_to_add]}
            r = RequestRetry.post(
                GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/columns",
                headers=headers,
                json=columns_to_add,
            )
            handle_grist_error(r)
        # re-getting the columns post potential update
        returned_columns = get_columns_mapping(doc_id, table_id, id_to_label=False)
    return returned_columns


def df_to_grist(df: pd.DataFrame, doc_id: str, table_id: str, append: Union[bool, str] = False):
    """
    Uploads a pd.DataFrame to a grist table (in chunks to avoid 413 errors)
    If the table(_id) already exists:
        - if append:
            > append=='lazy': append records at the end of the table, add new columns if needed
            > append=='exact': append only if columns match
        - else: replace it entirely
    Otherwise create it
    """
    assert append in [False, "lazy", "exact"]
    tables = RequestRetry.get(
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
        r = RequestRetry.post(
            GRIST_API_URL + "docs/" + doc_id + "/tables",
            headers=headers,
            json=table
        )
        handle_grist_error(r)
        returned_columns = handle_and_return_columns(doc_id, table_id, df, append)
    else:
        if append:
            print(f"Appending records to '{doc_id}/tables/{table_id}' in grist")
            returned_columns = handle_and_return_columns(doc_id, table_id, df, append)
        else:
            print(f"Erasing and refilling '{doc_id}/tables/{table_id}' in grist")
            erase_table_content(doc_id, table_id)
            # some column ids are not accepted by grist (e.g 'id'), so we get the new ids
            # to potentially replace them so that the upload doesn't crash
            returned_columns = rename_table_columns(doc_id, table_id, df.columns.to_list())
    # fill it up
    res = []
    for chunk in chunkify(df):
        r = RequestRetry.post(
            GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/records",
            headers=headers,
            json=recordify(chunk, returned_columns)
        )
        handle_grist_error(r)
        res += r.json()["records"]
    return res


def get_table_as_df(
    doc_id: str,
    table_id: str,
    columns_labels: bool = True,
    usecols: Optional[list[str]] = None,
):
    """
    Gets a grist table as a pd.Dataframe. You may choose if you want the columns' labels or ids.
    Fill in usecols with the list of the columns you want to keep (using their ids).
    """
    r = RequestRetry.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/records",
        headers=headers
    )
    handle_grist_error(r)
    df = pd.DataFrame([k["fields"] for k in r.json()["records"]])
    if usecols:
        df = df[usecols]
    if not columns_labels:
        return df
    column_mapping = get_columns_mapping(doc_id, table_id, id_to_label=True)
    return df.rename(column_mapping, axis=1)


def update_records(
    doc_id: str,
    table_id: str,
    conditions: dict,
    new_values: dict,
    query_params: dict = {"onmany": "all", "noadd": False},
):
    # conditions should look like {"col1": "val1", "col2": "val2", ...}, values will be updated where
    # col1==val1 & col2==val2 & ...
    # new_values should look like {"col": "new_value", ...}, we update the values of the specified columns
    # see https://support.getgrist.com/api/#tag/records/operation/replaceRecords for query parameters
    url_params = "&".join(f"{k}={v}" for k, v in query_params.items())
    r = RequestRetry.put(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/records?{url_params}",
        headers=headers,
        json={
            "records": [
                {
                    "require": conditions,
                    "fields": new_values,
                },
            ],
        },
    )
    handle_grist_error(r)


def get_unique_values_from_multiple_choice_column(column: pd.Series) -> set:
    # multiple choice columns look like ["L", "val1", "val2", ...]
    # this returns all unique single possible values
    # NB: make sure the column is properly cast upstream (no NaN for instance)
    return set([value for cell in column if cell for value in cell if value != "L"])
