import json
from datetime import datetime
from pathlib import Path

import segyio
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

N = 4
DATA_DIR = Path.home() / "airflow" / "airflow_home" / "data"
print("DATA_DIR:", DATA_DIR)

# в этом файле только первые 4 блока
INPUT_FILE = DATA_DIR / "in.sgy"
OUTPUT_FILE = DATA_DIR / "tmp/block"
MEM_FILE = DATA_DIR / "mem.json"


def read_offsets(segy_file: Path) -> list[int]:
    with segyio.open(segy_file, "r", ignore_geometry=True) as f:
        return [int(i) for i in f.attributes(segyio.TraceField.offset)]


@dag(
    dag_id="tkm2d",
    description="Миграция",
    schedule=None,
    catchup=False,
    tags=["tkm2d", "seismic", "migration"],
)
def tkm2d_dag():
    @task
    def read_file_offsets():
        offsets = read_offsets(INPUT_FILE)
        count = len(offsets)

        block_sizes = [count // N + (1 if count % N > i else 0) for i in range(N)]
        gaps = [sum(block_sizes[:i]) for i in range(N)]

        return {
            "offsets": offsets,
            "block_sizes": block_sizes,
            "gaps": gaps,
        }

    @task
    def process_block(data: dict, row_idx: int):
        offsets = data["offsets"]
        block_sizes = data["block_sizes"]
        gaps = data["gaps"]

        begin = gaps[row_idx]
        end = begin + block_sizes[row_idx]

        return [offsets[i] // 50 for i in range(begin, end)]

    @task
    def collect_results(results: list[list[int]]):
        rows = []
        for block in results:
            rows.extend(block)

        mem = sorted(
            [(i, e) for i, e in enumerate(rows)],
            key=lambda x: x[1],
        )

        MEM_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(MEM_FILE, "w") as f:
            json.dump(mem, f)

        return str(MEM_FILE)

    data = read_file_offsets()

    processed = [process_block(data, i) for i in range(N)]

    mem_path = collect_results(processed)

    write_segy = BashOperator(
        task_id="write_segy_external",
        bash_command=(
            f"python3 {DATA_DIR}/write_bin_blocks.py {INPUT_FILE} {OUTPUT_FILE} {mem_path}"
        ),
    )

    mem_path >> write_segy


tkm2d = tkm2d_dag()
