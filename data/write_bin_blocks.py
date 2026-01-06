#!/usr/bin/env python3
import json
import struct
import sys


def write_float32_array(path: str, data: list[float]) -> None:
    import struct

    with open(path, "wb") as f:
        f.write(struct.pack(f"{len(data)}f", *data))


def write_int64_array(path: str, data: list[int]) -> None:
    with open(path, "wb") as f:
        f.write(struct.pack(f"{len(data)}q", *data))


def main():
    import segyio

    in_file = sys.argv[1]
    out_file_prefix = sys.argv[2]
    mem_file = sys.argv[3]

    with open(mem_file, "r") as f:
        mem = json.load(f)

    block_ids = set(blockno for _, blockno in mem)
    count_blocks = len(block_ids)

    trace_files = [f"{out_file_prefix}_{i}.bin" for i in range(count_blocks)]
    offset_files = [f"{out_file_prefix}_{i}_offset.bin" for i in range(count_blocks)]
    ensemble_files = [
        f"{out_file_prefix}_{i}_ensemble.bin" for i in range(count_blocks)
    ]

    traces = [[] for _ in range(count_blocks)]
    offsets = [[] for _ in range(count_blocks)]
    ensembles = [[] for _ in range(count_blocks)]

    with segyio.open(in_file, "r", ignore_geometry=True) as fin:
        for in_idx, blockno in mem:
            traces[blockno].extend(fin.trace[in_idx])

            hdr = fin.header[in_idx]
            offsets[blockno].append(float(hdr[segyio.TraceField.Offset]))
            ensembles[blockno].append(int(hdr[segyio.TraceField.Ensemble]))

    for f_tr, f_off, f_en, tr, off, en in zip(
        trace_files, offset_files, ensemble_files, traces, offsets, ensembles
    ):
        write_float32_array(f_tr, tr)
        write_float32_array(f_off, off)
        write_int64_array(f_en, en)


if __name__ == "__main__":
    main()
