from pathlib import Path
from functools import partial

import tempfile
import numpy as np
from astropy.io.fits._tiled_compression.utils import _n_tiles
from astropy.io import fits
import dask
dask.config.set(scheduler='synchronous')  # overwrite default with single-threaded scheduler
# dask.config.set(scheduler='threads')

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


def write_and_open(data, tile_size=[256, 256]):
    hdul = fits.HDUList([
        fits.PrimaryHDU(),
        fits.CompImageHDU(
            data=data,
            compression_type="RICE_1",
            tile_size=tile_size,
        )
    ])
    file_path = tempfile.mkstemp(".fits")[1]
    hdul.writeto(file_path)

    return fits.open(file_path)[1]


def make_and_open_float64():
    return write_and_open(np.ones((4096, 4096), dtype=np.float64)),


def make_and_open_float32():
    return write_and_open(np.ones((4096, 4096), dtype=np.float32)),


@benchmark(setup=make_and_open_float64, pyinstrument=dict(interval=0.0001))
def access_data_float64(hdu):
    return hdu.data


@benchmark(setup=make_and_open_float32, pyinstrument=dict(interval=0.0001))
def access_data_float32(hdu):
    return hdu.data


def load_dkist_dask_array(path):
    import dkist
    ds = dkist.Dataset.from_directory(path)
    print(f"Using DKIST dataset ({ds.meta['inventory']['datasetId']}) with shape: {ds.data.shape}")
    with fits.open(ds.files.basepath / ds.files.filenames[0], disable_image_compression=True) as hdul:
        h = hdul[1].header
        tile_shape = tuple(h[f"ZTILE{z}"] for z in range(1, h["ZNAXIS"] + 1))[::-1]
        print(f"FITS tile shape is {tile_shape} (numpy order).")
        print(f"File has {np.product(_n_tiles(hdul[1].data.shape, tile_shape))} tiles.")
    return ds.data,


def load_AKNPB():
    return load_dkist_dask_array("/storfa/dkist/globus/pid_1_118/AKNPB")


@benchmark(setup=load_AKNPB)
def dkist_access_single_frame(data):
    data[0, 0, 0].compute()


@benchmark(setup=load_AKNPB)
def dkist_access_mosaic(data):
    data[0, :, 0, :].compute()


@benchmark(setup=load_AKNPB)
def dkist_access_single_chunk(data):
    arr = data[0, 0, 0, 0].compute()
    print(arr.shape)


def load_simulated_dkist(dataset):
    base = Path("/storfa/dkist/tiled_performance")
    return load_dkist_dask_array(base / dataset)


@benchmark(partial(load_simulated_dkist, "row_tiles"))
def dkist_load_mosaic_row(data):
    return data[0, :, 0, :].compute()


@benchmark(partial(load_simulated_dkist, "square_tiles"))
def dkist_load_mosaic_square(data):
    return data[0, :, 0, :].compute()
