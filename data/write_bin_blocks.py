#!/usr/bin/env python3
import json
import sys


def write_float32_array(path: str, data: list[float]) -> None:
    import struct

    with open(path, "wb") as f:
        f.write(struct.pack(f"{len(data)}f", *data))


def main():
    import segyio

    in_file = sys.argv[1]
    out_file_prefix = sys.argv[2]
    mem_file = sys.argv[3]

    with open(mem_file, "r") as f:
        mem = json.load(f)

    dic = set()
    for out_idx, (in_idx, blockno) in enumerate(mem):
        dic.add(blockno)
    count_blocks = len(dic)
    block_files = [f"{out_file_prefix}_{i}.bin" for i in range(count_blocks)]
    blocks = [[] for _ in range(count_blocks)]

    with segyio.open(in_file, "r", ignore_geometry=True) as fin:
        for out_idx, (in_idx, blockno) in enumerate(mem):
            blocks[blockno].extend(fin.trace[in_idx])
    for file, block in zip(block_files, blocks):
        write_float32_array(file, block)


if __name__ == "__main__":
    main()
