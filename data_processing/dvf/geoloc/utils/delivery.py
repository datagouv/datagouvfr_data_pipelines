from dataclasses import dataclass
from datetime import date


@dataclass
class DeliveryInfo:
    delivery: str  # month of the DVF delivery: "april" or "october"
    month: int  # month of the DVF delivery: 4 or 10
    file_count: int  # number of data files: 5 or 6
    cadastre_millesime: str  # month of the cadastre millesime: "03", "06"


DELIVERIES = {
    "april": DeliveryInfo(
        delivery="april", month=4, file_count=5, cadastre_millesime="03"
    ),
    "october": DeliveryInfo(
        delivery="october", month=10, file_count=6, cadastre_millesime="06"
    ),
}


def get_delivery_from_today(today: date) -> str:
    return "april" if 4 <= today.month < 10 else "october"


def get_delivery_info(delivery: str | None, today: date) -> DeliveryInfo:
    if delivery is None:
        delivery = get_delivery_from_today(today)
    delivery_info = DELIVERIES.get(delivery)
    if delivery_info is None:
        raise ValueError(f"Unknown delivery: {delivery!r}")
    return delivery_info
