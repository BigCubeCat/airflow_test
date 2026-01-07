import json
import struct
from pathlib import Path

import segyio
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

N = 4
DATA_DIR = Path.home() / "airflow" / "airflow_home" / "data"
TMP_DIR = DATA_DIR / "tmp"
print("DATA_DIR:", DATA_DIR)

# в этом файле только первые 4 блока
INPUT_FILE = DATA_DIR / "in.sgy"
OUTPUT_FILE = DATA_DIR / "tmp/block"
MEM_FILE = DATA_DIR / "mem.json"

VEL_FILE = DATA_DIR / "vel.bin"
ETA_FILE = DATA_DIR / "eta.bin"
IMAG_FILE = DATA_DIR / "imag64.bin"

TKM2D_EXECUTABLE = Path.home() / "airflow" / "airflow_home" / "scripts" / "tkm2d_test"


def write_float32_array(path: str, data: list[float]) -> None:
    import struct

    with open(path, "wb") as f:
        f.write(struct.pack(f"{len(data)}f", *data))


def write_int64_array(path: str, data: list[int]) -> None:
    with open(path, "wb") as f:
        f.write(struct.pack(f"{len(data)}q", *data))


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

    @task
    def create_blocks(mem_path: str):
        with open(mem_path, "r") as f:
            mem = json.load(f)

        block_ids = set(blockno for _, blockno in mem)
        count_blocks = len(block_ids)

        trace_files = [f"{OUTPUT_FILE}_{i}.bin" for i in range(count_blocks)]
        offset_files = [f"{OUTPUT_FILE}_{i}_offset.bin" for i in range(count_blocks)]
        ensemble_files = [
            f"{OUTPUT_FILE}_{i}_ensemble.bin" for i in range(count_blocks)
        ]

        traces = [[] for _ in range(count_blocks)]
        offsets = [[] for _ in range(count_blocks)]
        ensembles = [[] for _ in range(count_blocks)]

        with segyio.open(INPUT_FILE, "r", ignore_geometry=True) as fin:
            for in_idx, blockno in mem:
                traces[blockno].extend(fin.trace[in_idx])

                hdr = fin.header[in_idx]
                offsets[blockno].append(float(hdr[segyio.TraceField.offset]))
                ensembles[blockno].append(int(hdr[segyio.TraceField.CDP]))

        for f_tr, f_off, f_en, tr, off, en in zip(
            trace_files, offset_files, ensemble_files, traces, offsets, ensembles
        ):
            write_float32_array(f_tr, tr)
            write_float32_array(f_off, off)
            write_int64_array(f_en, en)
        params = []
        for i, (t, o, e) in enumerate(zip(trace_files, offset_files, ensemble_files)):
            params.append(
                f"{TKM2D_EXECUTABLE} {t} {o} {e} {VEL_FILE} {ETA_FILE} {IMAG_FILE} {TMP_DIR}/output_{i}.txt"
            )

        return params

    data = read_file_offsets()
    processed = [process_block(data, i) for i in range(N)]
    mem_path = collect_results(processed)
    commands = create_blocks(mem_path)
    print(commands)
    run_tkm2d = BashOperator.partial(
        task_id="run_tkm2d",
        pool="tkm2d_pool",
    ).expand(bash_command=commands)


tkm2d = tkm2d_dag()
