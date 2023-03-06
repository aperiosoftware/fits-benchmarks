from astropy.io import fits
import tempfile
from pathlib import Path
import numpy as np
import timeit

import pyinstrument

array_size = (4096, 4096)
dtype = np.float64
# array_size = (2556, 977)

tile_sizes = [[n]*2 for n in range(4096, 1, -100)]


def extract_function_time(profiler, function_name):
    function_timings = {}
    def parse_node(node):
        for child in node.children:
            class_name = f"{child.class_name}." if child.class_name is not None else ""
            function_timings[f"{class_name}{child.function}"] = child.time
            parse_node(child)
    parse_node(profiler.last_session.root_frame())
    return function_timings[function_name]


# temp_dir = Path(tempfile.mkdtemp())
temp_dir = Path("/storfa/tmp/")

test_files = {}


for tile_size in tile_sizes:
    data = np.random.random(array_size).view(dtype)
    filename = f"tile_test_{array_size}_{tile_size}.fits"
    file_path = temp_dir / filename
    if not file_path.exists():
        hdul = fits.HDUList([
            fits.PrimaryHDU(),
            fits.CompImageHDU(
                data=data,
                compression_type="RICE_1",
                tile_size=tile_size[::-1],
            )
        ])
        hdul.writeto(file_path)
    test_files[tuple(tile_size)] = file_path


print("Generated all data")

def read_data(filename):
    with fits.open(filename) as hdul:
        np.asarray(hdul[1].data)

results = {}
for tile_size, filename in test_files.items():
    results[tuple(tile_size)] = timeit.timeit(f"read_data('{filename}')", globals=locals(), number=1000)

print(results)
