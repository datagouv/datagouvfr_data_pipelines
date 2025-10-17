import re
from datetime import datetime
from datagouvfr_data_pipelines.verticales.simplifions.grist_v2_manager import (
    GristV2Manager,
)

IGNORED_KEYS = ["Modifie_le", "Modifie_par", "anchor_link", "technical_title"]


class DiffManager:
    def __init__(self):
        pass

    @staticmethod
    def _create_simplifions_backup():
        new_name = "Simplifions Copy - " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_doc_id = GristV2Manager._copy_document(document_name=new_name)
        return {"id": new_doc_id, "name": new_name}

    @staticmethod
    def _list_simplifions_backups() -> dict:
        documents = GristV2Manager._list_workspace_documents()
        name_pattern = re.compile(
            r"Simplifions Copy - \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        )
        backups = [doc for doc in documents if name_pattern.match(doc["name"])]
        return backups

    @staticmethod
    def _get_last_simplifions_backup() -> dict:
        backups = DiffManager._list_simplifions_backups()
        sorted_backups = sorted(backups, key=lambda x: x["name"], reverse=True)
        return sorted_backups[0]

    @staticmethod
    def get_diff(new_fields: dict, backup_fields: dict):
        if new_fields == backup_fields:
            return None

        diff = {}
        all_keys = (set(new_fields.keys()) | set(backup_fields.keys())) - set(
            IGNORED_KEYS
        )

        for key in all_keys:
            row_value = new_fields.get(key)
            backup_value = backup_fields.get(key)

            if row_value != backup_value:
                diff[key] = {"old": backup_value, "new": row_value}

        return diff

    @staticmethod
    def _boldify_last_section(description: str) -> str:
        if " > " not in description:
            return description
        split_description = description.split(" > ")
        first_section = " > ".join(split_description[:-1])
        last_section = split_description[-1]

        return f"{first_section} > **{last_section}**"

    @staticmethod
    def format_row_link(row: dict) -> str:
        """Format a row as a markdown link with boldified last section."""
        title = DiffManager._boldify_last_section(row["fields"]["technical_title"])
        link = row["fields"]["anchor_link"]
        return f"[{title}]({link})"

    @staticmethod
    def format_diff_value(value) -> str:
        """Format a diff value for display: wrap in backticks if exists, otherwise return '(vide)'."""
        if not value:
            return "(vide)"
        formatted_value = f"`{value}`"

        if isinstance(value, list) and value and value[0] == "L":
            formatted_value = f"`{value[1:]}`"
        elif isinstance(value, str) and len(value) > 100:
            formatted_value = f"`{value[:100].replace('`', "'")}...` _(tronqué)_"

        return formatted_value
