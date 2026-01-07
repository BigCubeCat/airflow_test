import segyio


def main(in_path, out_path, offset_limit):
    with segyio.open(in_path, "r", ignore_geometry=True) as src:
        selected = [
            i
            for i in range(src.tracecount)
            if src.header[i][segyio.TraceField.offset] < offset_limit
        ]

        spec = segyio.spec()
        spec.sorting = src.sorting
        spec.format = src.format
        spec.samples = src.samples
        spec.tracecount = len(selected)

        with segyio.create(out_path, spec) as dst:
            dst.text[0] = src.text[0]
            dst.bin = src.bin

            for new_i, old_i in enumerate(selected):
                dst.trace[new_i] = src.trace[old_i]
                dst.header[new_i] = src.header[old_i]


if __name__ == "__main__":
    import sys

    main(sys.argv[1], sys.argv[2], 400)
