# src/transform.py
def transform_data(data: list[dict]) -> list[dict]:
    return [{"id": d["id"], "value": d["value"] * 2} for d in data]
