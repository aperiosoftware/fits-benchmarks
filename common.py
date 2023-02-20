from pathlib import Path
from astropy.io import fits


testfile = Path(".") / "data" / "aia_lev1_193a_2013_03_15t12_01_06_84z_image_lev1.fits"


def open_file_aia():
    return fits.open(testfile),


def access_single_tile_aia(hdul):
    return hdul[1].section[0, :]


def access_data_aia(hdul):
    return hdul[1].data
