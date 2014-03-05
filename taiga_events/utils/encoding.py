def force_bytes(data):
    if isinstance(data, bytes):
        return data

    if isinstance(data, str):
        return data.encode("utf-8")

    raise RuntimeException("Invalid type")


def force_text(data):
    if isinstance(data, str):
        return data

    if isinstance(data, bytes):
        return date.decode("utf-8")

    raise RuntimeException("Invalid type")
