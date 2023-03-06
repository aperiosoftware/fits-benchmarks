from astropy.io import fits
import tempfile
from pathlib import Path
import numpy as np
import timeit
import astropy.table # because eugh
import matplotlib.pyplot as plt
from astropy.table import QTable
import astropy.units as u
from matplotlib.ticker import ScalarFormatter
from astropy.visualization import quantity_support
quantity_support()

import pyinstrument

array_size = (4096, 4096)
dtype = np.float64
# array_size = (2556, 977)

tile_increment = -16
tile_sizes = [[n]*2 for n in range(array_size[0], 1, tile_increment)]


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
    filename = f"tile_test_{array_size}_{tile_size}_{dtype.__name__}.fits"
    file_path = temp_dir / filename
    if not file_path.exists():
        print(f"Generating {filename=}")
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


data_cache_filepath = temp_dir / f"test_data_{array_size}_square_{abs(tile_increment)}_{dtype}.ecsv"

if not data_cache_filepath.exists():
    n_repeats = 10

    tile_sizes = []
    all_data = []
    single_tile = []
    sizes = []
    for tile_size, filename in test_files.items():
        print(tile_size)
        tile_sizes.append(tile_size)
        all_data.append((timeit.timeit(f"read_all_data('{filename}')", globals=locals(), number=n_repeats) / n_repeats))
        single_tile.append((timeit.timeit(f"read_single_tile('{filename}')", globals=locals(), number=n_repeats) / n_repeats))
        sizes.append(filename.stat().st_size)

    data = QTable([np.arange(len(tile_sizes)-1, -1, -1), tile_sizes, all_data * u.s, single_tile * u.s, sizes * u.byte],
                names=["index", "tile_size", "all_data", "single_tile", "file_size"])
    data.add_index("index")

    data.write(data_cache_filepath)
else:
    print("Loading cached data")
    data = QTable.read(data_cache_filepath)


def get_tick_label(x, pos):
    try:
        return str(tuple(data.iloc[int(x)]["tile_size"]))
    except Exception:
        return str(int(x))

fig, [ax, ax3] = plt.subplots(nrows=2, sharex=True)
l1, = ax.plot(data["index"], data["all_data"].to(u.ms), label="Whole Array", marker='o', color="blue")
ax.set_ylabel("Average time to read whole array [ms]")
ax.set_xlabel("Tile shape")
ax.set_xscale('log')
ax.yaxis.label.set_color("blue")

ax2 = ax.twinx()
ax2.yaxis.label.set_color("orange")
l2, = ax2.plot(data["index"], data["single_tile"].to(u.ms), label="Single Tile", color='orange', marker='o')
ax2.set_ylabel("Average time to single tile array [ms]")
plt.legend([l1, l2], ["Whole Array", "Single Tile"])

ax3.loglog(data["index"], data["file_size"].to(u.Mibyte), marker="o")
ax.xaxis.set_major_formatter(get_tick_label)
ax.set_xticks(data["index"][np.isin(data["tile_size"][:, 0], np.logspace(6, 12, 7, base=2, endpoint=True))], rotation='vertical')

plt.show()
