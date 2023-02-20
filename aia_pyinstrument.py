import sys

import common
import pyinstrument


__all__ = ['access_data_aia', 'access_single_tile_aia', 'open_file_aia']


def run_pyinstrument(setup=lambda: [], teardown=lambda: None, profiler_kwargs=None):
    def run_benchmark(func):
        def wrapped_benchmark():
            args = setup()
            pkwargs = profiler_kwargs or {}
            profiler = pyinstrument.Profiler(**pkwargs)
            profiler.start()

            func(*args)

            profiler.stop()

            profiler.print()

            teardown()
        return wrapped_benchmark
    return run_benchmark


@run_pyinstrument(profiler_kwargs=dict(interval=0.00001))
def open_file_aia():
    common.open_file_aia()


@run_pyinstrument(setup=common.open_file_aia, profiler_kwargs=dict(interval=0.0001))
def access_single_tile_aia(hdul):
    return common.access_single_tile_aia(hdul)


@run_pyinstrument(setup=common.open_file_aia, profiler_kwargs=dict(interval=0.0001))
def access_data_aia(hdul):
    return common.access_data_aia(hdul)


if __name__ == "__main__":
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print("Specify benchmark name as only argument")
        print("Available benchmarks:")
        for func in __all__:
            print(func)
        sys.exit(0)

    if sys.argv[1] in __all__:
        func = locals()[sys.argv[1]]
        func()
        sys.exit(0)

    print("Unknown benchmark.")
    sys.exit(1)
