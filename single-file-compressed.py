"""
Do some opertations on a single RICE compressed, integer FITS file.
"""
from pathlib import Path
from astropy.io import fits
from astropy.io.fits._tiled_compression.utils import _tile_shape as tile_shape


class SingleFITS:
    testfile = Path(".") / "data" / "aia_lev1_193a_2013_03_15t12_01_06_84z_image_lev1.fits"

    def setup(self):
        hdul = fits.open(self.testfile, disable_image_compression=True)
        self.hdu = hdul[1]
        self.header = self.hdu.header
        self.tile_shape = tile_shape(self.header)

    def time_open(self):
        fits.open(self.testfile)

    def time_access_single_tile(self):
        self.hdu.section[10:20, 10:20]
