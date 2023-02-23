import os
from astropy.io import fits

AIA_DATA = os.path.join(os.path.dirname(__file__), 'data', 'aia_lev1_193a_2013_03_15t12_01_06_84z_image_lev1.fits')


class TimeDataAccess:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    def setup(self):
        self.hdu = fits.open(AIA_DATA)[1]

    def time_access_data_aia(self):
        self.hdu.data
