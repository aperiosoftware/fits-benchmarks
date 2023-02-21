from pathlib import Path
from astropy.io import fits
import dask
dask.config.set(scheduler='synchronous')  # overwrite default with single-threaded scheduler

BENCHMARKS = {}


def benchmark(setup=lambda: [], teardown=lambda: None, pyinstrument=None):
    global BENCHMARKS

    def _benchmark(func):
        func.setup = setup
        func.teardown = teardown
        func.pyinstrument = pyinstrument or {}
        BENCHMARKS[func.__name__] = func
        return func
    return _benchmark


@benchmark(pyinstrument=dict(interval=0.00001))
def open_file_aia():
    testfile = Path(".") / "data" / "aia_lev1_193a_2013_03_15t12_01_06_84z_image_lev1.fits"
    return fits.open(testfile)[1],


@benchmark(pyinstrument=dict(interval=0.00001))
def open_close_file_aia():
    # TODO: Check this actually tests the enter and exit properly
    testfile = Path(".") / "data" / "aia_lev1_193a_2013_03_15t12_01_06_84z_image_lev1.fits"
    with fits.open(testfile) as hdul:
        pass


@benchmark(setup=open_file_aia, pyinstrument=dict(interval=0.0001))
def access_single_tile_aia(hdu):
    return hdu.section[0, :]


@benchmark(setup=open_file_aia, pyinstrument=dict(interval=0.0001))
def access_data_aia(hdu):
    return hdu.data


def load_dkist_dask_array():
    import dkist
    ds = dkist.Dataset.from_directory("/storfa/dkist/globus/pid_1_118/AKNPB")
    print(ds.data)
    return ds.data,


@benchmark(setup=load_dkist_dask_array)
def dkist_access_single_frame(data):
    data[0, 0, 0].compute()


@benchmark(setup=load_dkist_dask_array)
def dkist_access_single_chunk(data):
    arr = data[0, 0, 0, 0].compute()
    print(arr.shape)
