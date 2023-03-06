from astropy.io import fits
import tempfile
from pathlib import Path
import numpy as np
import timeit
import astropy.table # because eugh
import matplotlib.pyplot as plt

import pyinstrument

array_size = (4096, 4096)
dtype = np.float64
# array_size = (2556, 977)

tile_sizes = [[n]*2 for n in range(4096, 1, -50)]


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
    filename = f"tile_test_{array_size}_{tile_size}_{dtype}.fits"
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


def read_all_data(filename):
    with fits.open(filename) as hdul:
        np.asarray(hdul[1].data)


def read_single_tile(filename):
    with fits.open(filename) as hdul:
        np.asarray(hdul[1].section[0, 0])


all_data = {}
single_tile = {}
for tile_size, filename in test_files.items():
    print(tile_size)
    all_data[tuple(tile_size)] = timeit.timeit(f"read_all_data('{filename}')", globals=locals(), number=10)
    single_tile[tuple(tile_size)] = timeit.timeit(f"read_single_tile('{filename}')", globals=locals(), number=10)

print(all_data)
print(single_tile)


n_elements = [np.product(s) / 1000 for s in all_data.keys()]
fig, ax = plt.subplots()
l1, = ax.plot(n_elements, all_data.values(), label="Whole Array", marker='o-')
ax.set_ylabel("Time for 10 reads [s]")
ax.set_xlabel("Number of elements in a tile [thousands]")
ax2 = ax.twinx()
l2, = ax2.plot(n_elements, single_tile.values(), label="Single Tile", color='orange', marker='o-')
ax2.set_ylabel("Time for 10 reads [s]")
plt.legend([l1, l2], ["Whole Array", "Single Tile"])
plt.show()
