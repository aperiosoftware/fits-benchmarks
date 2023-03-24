from astropy.io import fits
import tempfile
from pathlib import Path
import numpy as np
import timeit
import astropy.table # because eugh
import matplotlib.pyplot as plt
from astropy.table import QTable
import astropy.units as u
from matplotlib import ticker
from astropy.visualization import quantity_support
from itertools import chain
quantity_support()

import pyinstrument

test_data = fits.getdata("/storfa/dkist/globus/VBI_2022_06_02T17_22_50_173_00486136_I_BLKGA_L1.fits", hdu=1)

array_size = (4096, 4096)
array_size = (2556, 977)
dtype = np.float32
data_type = "random"
data_type = "vbi"

data_type_name = f"_{data_type}" if data_type == "random" else ""

tile_increment = -32
tile_sizes = [[n]*2 for n in range(array_size[0], 1, tile_increment)]
slit_factors = [
    36,
    71,
    142,
    213,
    284,
    426,
    639,
    852,
    1278,
    2556,
]
tile_sizes = [[n]*2 for n in slit_factors[::-1]]


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
    # data = test_data.astype(dtype)
    data = np.random.random(array_size).astype(dtype)
    filename = f"tile_test_{array_size}_{tile_size}_{dtype.__name__}{data_type_name}.fits"
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
        tile_size = [hdul[1]._header[f"ZTILE{n}"] for n in range(hdul[1]._header["ZNAXIS"], 0, -1)]
        tile_slice = [slice(0, si) for si in tile_size]
        np.asarray(hdul[1].section[tile_slice])


data_cache_filepath = temp_dir / f"test_data_{array_size}_square_{abs(tile_increment)}_{dtype.__name__}{data_type_name}.ecsv"

if not data_cache_filepath.exists():
    n_repeats = 20

    tile_sizes = []
    all_data = []
    single_tile = []
    sizes = []
    n_elements = []
    for tile_size, filename in test_files.items():
        print(tile_size)
        n_elements.append(np.product(tile_size))
        tile_sizes.append(tile_size)
        all_data.append((timeit.timeit(f"read_all_data('{filename}')", globals=locals(), number=n_repeats) / n_repeats))
        single_tile.append((timeit.timeit(f"read_single_tile('{filename}')", globals=locals(), number=n_repeats) / n_repeats))
        sizes.append(filename.stat().st_size)

    data = QTable([np.arange(len(tile_sizes)-1, -1, -1),
                   tile_sizes,
                   (all_data * u.s).to(u.ms),
                   (single_tile * u.s).to(u.ms),
                   (sizes * u.byte).to(u.Mibyte)],
                names=["index", "tile_size", "all_data", "single_tile", "file_size"])
    data.add_index("index")

    data.write(data_cache_filepath)
else:
    print("Loading cached data")
    data = QTable.read(data_cache_filepath)
    data.add_index("index")

    data["all_data"] = data["all_data"].to(u.ms)
    data["single_tile"] = data["single_tile"].to(u.ms)
    data["file_size"] = data["file_size"].to(u.Mibyte)

# Print a summary
print("#"*80)
print("Summary of timings:")
print("#"*80)
print(f"Array Size: {array_size} | dtype: {dtype.__name__} | array source: {data_type}")
for tile_size in tile_sizes:
    sub_data = data[(data["tile_size"] == tile_size)[:, 0]]
    print(f"{sub_data['tile_size'].tolist()[0]}, Whole Data: {sub_data['all_data']}, Single Tile: {sub_data['single_tile']}")
print("#"*80)

def get_tick_label(x, pos):
    try:
        return str(tuple(data.iloc[int(x)]["tile_size"]))
    except Exception as e:
        return str(int(x))

fig, [ax, ax3] = plt.subplots(nrows=2, sharex=False)
ax.set_title(f"Timings for a {data_type} array.")
l1, = ax.plot(data["index"], data["all_data"].to(u.ms), label="Whole Array", marker='o', color="blue")
ax.set_ylabel("Average time to read whole array [ms]")
ax.set_xscale('log')
ax.set_yscale('log')
ax.yaxis.label.set_color("blue")
ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
ax.xaxis.set_major_formatter(get_tick_label)
ax.xaxis.set_major_locator(ticker.FixedLocator(data["index"][np.isin(data["tile_size"][:, 0], np.logspace(6, 12, 7, base=2, endpoint=True))]))

ax2 = ax.twinx()
ax2.yaxis.label.set_color("orange")
l2 = ax2.plot(data["index"], data["single_tile"].to(u.ms), label="Single Tile", color='orange', marker='o')
ax2.set_ylabel("Average time to single tile array [ms]")
ax2.set_yscale('log')
ax2.yaxis.set_major_formatter(ticker.ScalarFormatter())
# plt.legend([l1, l2], ["Whole Array", "Single Tile"])

ax3.plot(data["index"], data["file_size"].to(u.Mibyte), marker="o")
ax3.set_xscale('log')
ax3.set_yscale('log')
ax3.set_xlabel("Tile shape")
ax3.xaxis.set_major_formatter(get_tick_label)
ax3.yaxis.set_major_formatter(ticker.ScalarFormatter())
ax3.xaxis.set_major_locator(ticker.FixedLocator(data["index"][np.isin(data["tile_size"][:, 0], np.logspace(6, 12, 7, base=2, endpoint=True))]))
# ax3.set_xticks(data["index"][np.isin(data["tile_size"][:, 0], np.logspace(6, 12, 7, base=2, endpoint=True))], rotation='vertical')

plt.show()
