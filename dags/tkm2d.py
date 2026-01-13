import json
import os
import struct
from pathlib import Path
from typing import Optional

import numpy as np
import segyio
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# === Пути и файлы ===

DATA_DIR = Path.home() / "airflow" / "airflow_home" / "data"
TMP_DIR = DATA_DIR / "tmp"

INPUT_FILE = DATA_DIR / "in.sgy"
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
        """
        Собирает данные из блоков в SEG-Y файл.
        Записывает в TR_OFFSET - номер блока, в TR_ENSEMBLE - номер трассы в блоке.
        """
        # Читаем информацию о блоках из JSON
        with open(mem_path, "r") as f:
            mem = json.load(f)

        # Получаем отсортированный список уникальных номеров блоков
        block_ids = sorted({blockno for _, blockno in mem})

        # Константы
        SAMPLES_PER_TRACE = 2500
        SAMPLE_INTERVAL = 1000  # микросекунды
        FORMAT_CODE = 5  # IEEE float32

        # Определяем общее количество трасс и собираем все данные
        all_traces = []
        all_headers = []

        for block_id in block_ids:
            # Читаем данные блока как float32 массив
            data = np.fromfile(f"{TMP_DIR}/output_{block_id}.txt", dtype=np.float32)

            # Проверяем целостность данных
            if len(data) % SAMPLES_PER_TRACE != 0:
                raise ValueError(
                    f"Неверный размер данных в блоке {block_id}: "
                    f"{len(data)} не кратно {SAMPLES_PER_TRACE}"
                )

            num_traces_in_block = len(data) // SAMPLES_PER_TRACE
            traces_data = data.reshape(num_traces_in_block, SAMPLES_PER_TRACE)

            # Создаем заголовки для каждой трассы в блоке
            for local_trace_idx in range(num_traces_in_block):
                header = {
                    segyio.TraceField.offset: int(block_id),  # TR_OFFSET = номер блока
                    segyio.TraceField.CDP: int(
                        local_trace_idx
                    ),  # TR_ENSEMBLE = номер трассы в блоке
                    segyio.TraceField.TRACE_SAMPLE_COUNT: SAMPLES_PER_TRACE,
                    segyio.TraceField.TRACE_SAMPLE_INTERVAL: SAMPLE_INTERVAL,
                    segyio.TraceField.TRACE_SEQUENCE_FILE: len(all_traces) + 1,
                }

                all_traces.append(traces_data[local_trace_idx])
                all_headers.append(header)

        # Создаем SEG-Y файл
        output_path = f"{TMP_DIR}/result.sgy"

        spec = segyio.spec()
        spec.samples = list(range(SAMPLES_PER_TRACE))
        spec.format = FORMAT_CODE
        spec.tracecount = len(all_traces)

        with segyio.create(output_path, spec) as segyfile:
            # Устанавливаем поля бинарного заголовка
            segyfile.bin[segyio.BinField.Samples] = SAMPLES_PER_TRACE
            segyfile.bin[segyio.BinField.Interval] = SAMPLE_INTERVAL
            segyfile.bin[segyio.BinField.Format] = FORMAT_CODE
            segyfile.bin[segyio.BinField.Traces] = len(all_traces)

            # Записываем трассы и заголовки
            for i in range(len(all_traces)):
                segyfile.trace[i] = all_traces[i]
                segyfile.header[i] = all_headers[i]

            # Текстовый заголовок
            text = (
                "SEG-Y файл создан Apache Airflow. "
                + f"Трасс: {len(all_traces)}, "
                + f"Отсчетов на трассу: {SAMPLES_PER_TRACE}"
            )
            segyfile.text[0] = text.ljust(3200)[:3200]

        print(f"SEG-Y файл успешно создан: {output_path}")
        print(f"Всего трасс: {len(all_traces)}")
        print(f"Обработано блоков: {len(block_ids)}")

        return output_path

    mem_path = build_mem()
    commands = create_blocks(mem_path)

    run_tkm2d = BashOperator.partial(
        task_id="run_tkm2d",
        pool="tkm2d_pool",
    ).expand(bash_command=commands)

    run_gather = gather_result(mem_path)
    run_tkm2d >> run_gather


tkm2d = tkm2d_dag()
