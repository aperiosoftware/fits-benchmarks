#!/bin/bash -xe

# Last commit before FITS changes
taskset -c 0 asv run -e 9409d02ff9e101d4215f83aa1f9a23d805230fd0^!

# All commits since
taskset -c 0 asv run -e NEW
