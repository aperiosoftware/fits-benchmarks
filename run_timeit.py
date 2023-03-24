import sys
import timeit

from benchmarks import BENCHMARKS


def run_benchmark(func, number):
    args = func.setup()
    func(*args)
    time = timeit.timeit("func(*args)", globals=locals(), number=number)
    func.teardown()
    return time


if __name__ == "__main__":
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print("Specify benchmark name as only argument")
        print("Available benchmarks:")
        print("\n".join(BENCHMARKS.keys()))
        sys.exit(0)

    if sys.argv[1] in BENCHMARKS.keys():
        func = BENCHMARKS[sys.argv[1]]
        number = 5
        time = run_benchmark(func, number)
        print(f"Ran {func.__name__} {number} times, average time {time / number} s")
        sys.exit(0)

    print("Unknown benchmark.")
    sys.exit(1)
