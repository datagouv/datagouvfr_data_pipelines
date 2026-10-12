def human_size(num_bytes: int) -> str:
    """Format a byte count in a concise, human-readable way."""
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size} B"
