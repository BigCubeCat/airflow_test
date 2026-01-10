import json
import struct
from pathlib import Path

import segyio
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# === Пути и файлы ===

DATA_DIR = Path.home() / "airflow" / "airflow_home" / "data"
TMP_DIR = DATA_DIR / "tmp"

INPUT_FILE = DATA_DIR / "input.sgy"
MEM_FILE = TMP_DIR / "mem.json"

OUTPUT_FILE = DATA_DIR / "tmp" / "block"

VEL_FILE = DATA_DIR / "vel.bin"
ETA_FILE = DATA_DIR / "eta.bin"
IMAG_FILE = DATA_DIR / "imag64.bin"

TKM2D_EXECUTABLE = Path.home() / "airflow" / "airflow_home" / "scripts" / "tkm2d_test"


# === Вспомогательные функции ===


def write_float32_array(path: str, data: list[float]) -> None:
    with open(path, "wb") as f:
        f.write(struct.pack(f"{len(data)}f", *data))


def write_int64_array(path: str, data: list[int]) -> None:
    with open(path, "wb") as f:
        f.write(struct.pack(f"{len(data)}q", *data))


def read_offsets(segy_file: Path) -> list[int]:
    with segyio.open(segy_file, "r", ignore_geometry=True) as f:
        return [int(v) for v in f.attributes(segyio.TraceField.offset)]


# === DAG ===


@dag(
    dag_id="tkm2d",
    description="TKM2D migration (pool-controlled parallelism)",
    schedule=None,
    catchup=False,
    tags=["tkm2d", "seismic", "migration"],
)
def tkm2d_dag():
    # Формируем mem.json
    @task
    def build_mem() -> str:
        offsets = read_offsets(INPUT_FILE)

        mem = sorted(
            [(i, offsets[i] // 50) for i in range(len(offsets))],
            key=lambda x: x[1],
        )

        MEM_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(MEM_FILE, "w") as f:
            json.dump(mem, f)

        return str(MEM_FILE)

    # Создаём бинарные блоки и команды запуска
    @task
    def create_blocks(mem_path: str) -> list[str]:
        with open(mem_path, "r") as f:
            mem = json.load(f)

        block_ids = sorted({blockno for _, blockno in mem})

        traces = {b: [] for b in block_ids}
        offsets = {b: [] for b in block_ids}
        ensembles = {b: [] for b in block_ids}

        with segyio.open(INPUT_FILE, "r", ignore_geometry=True) as fin:
            for in_idx, blockno in mem:
                traces[blockno].extend(fin.trace[in_idx])

                hdr = fin.header[in_idx]
                offsets[blockno].append(float(hdr[segyio.TraceField.offset]))
                ensembles[blockno].append(int(hdr[segyio.TraceField.CDP]))

        TMP_DIR.mkdir(parents=True, exist_ok=True)

        commands: list[str] = []

        for b in block_ids:
            trace_file = f"{OUTPUT_FILE}_{b}.bin"
            offset_file = f"{OUTPUT_FILE}_{b}_offset.bin"
            ensemble_file = f"{OUTPUT_FILE}_{b}_ensemble.bin"

            write_float32_array(trace_file, traces[b])
            write_float32_array(offset_file, offsets[b])
            write_int64_array(ensemble_file, ensembles[b])

            commands.append(
                f"{TKM2D_EXECUTABLE} "
                f"{trace_file} "
                f"{offset_file} "
                f"{ensemble_file} "
                f"{VEL_FILE} "
                f"{ETA_FILE} "
                f"{IMAG_FILE} "
                f"{TMP_DIR}/output_{b}.txt"
            )

        return commands

    @task
    def gather_result(mem_path: str):
        with open(mem_path, "r") as f:
            mem = json.load(f)
        block_ids = sorted({blockno for _, blockno in mem})
        with open(f"{TMP_DIR}/result.txt", "w") as f:
            for b in block_ids:
                with open(f"{TMP_DIR}/output_{b}.txt", "r") as g:
                    f.write(g.read())

    # === Граф ===

    mem_path = build_mem()
    commands = create_blocks(mem_path)

    run_tkm2d = BashOperator.partial(
        task_id="run_tkm2d",
        pool="tkm2d_pool",
    ).expand(bash_command=commands)

    run_gather = gather_result(mem_path)
    run_tkm2d >> run_gather


tkm2d = tkm2d_dag()
