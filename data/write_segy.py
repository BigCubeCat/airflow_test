#!/usr/bin/env python3
import json
import sys


def main():
    import segyio

    in_file = sys.argv[1]
    out_file = sys.argv[2]
    mem_file = sys.argv[3]

    with open(mem_file, "r") as f:
        mem = json.load(f)

    with segyio.open(in_file, "r", ignore_geometry=True) as fin:
        spec = segyio.spec()
        spec.sorting = fin.sorting
        spec.format = fin.format
        spec.samples = fin.samples
        spec.tracecount = len(mem)

        with segyio.create(out_file, spec) as fout:
            fout.bin = fin.bin

            for out_idx, (in_idx, blockno) in enumerate(mem):
                fout.trace[out_idx] = fin.trace[in_idx]
                fout.header[out_idx] = fin.header[in_idx]


if __name__ == "__main__":
    main()
