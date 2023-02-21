import sys

from benchmarks import BENCHMARKS


def run_benchmark(func):
    args = func.setup()
    func(*args)
    func.teardown()


if __name__ == "__main__":
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print("Specify benchmark name as only argument")
        print("Available benchmarks:")
        print("\n".join(BENCHMARKS.keys()))
        sys.exit(0)

    if sys.argv[1] in BENCHMARKS.keys():
        func = BENCHMARKS[sys.argv[1]]
        run_benchmark(func)
        sys.exit(0)

    print("Unknown benchmark.")
    sys.exit(1)
