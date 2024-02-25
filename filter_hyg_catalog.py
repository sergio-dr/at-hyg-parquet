# %%
import pandas as pd

# __/ Stars \__________
in_fname = "https://github.com/astronexus/ATHYG-Database/raw/main/data/subsets/hyglike_from_athyg_v31.csv.gz"
usecols = ['ra', 'dec', 'hip', 'proper', 'bayer', 'spect', 'mag']

out_fname_f = lambda v: f"hyglike_from_athyg_v31_for_{v}.parquet"

versions = ['Planisphere', 'SkyChart']
mag_limits = [4.2, 6.7]

df = pd.read_csv(in_fname, compression='gzip', usecols=usecols)

# Remove stars without Hipparcos ID, then set int dtype
df.dropna(subset=['hip'], inplace=True)
df.hip = df.hip.astype(np.int32)

for v, mag in zip(versions, mag_limits):
    df[df.mag <= mag].to_parquet(out_fname_f(v))


# %%
# __/ DSOs \__________
in_fname = "https://raw.githubusercontent.com/astronexus/HYG-Database/main/misc/dso.csv"
usecols = ['ra', 'dec', 'cat1', 'id1', 'r1', 'r2', 'angle', 'mag']

out_fname_f = lambda v: f"dso_for_{v}.parquet"

versions = ['Planisphere', 'SkyChart']
mag_limits = [4.0, 10.0]
sz_limits = [60.0, 10.0]  # arcmin

df = pd.read_csv(in_fname, low_memory=False, usecols=usecols)

# Mag and size filters
# Note: r1 and r2 are not "semimajor/minor" axes (or radius),
# as described in the github page, they are major/minor axes
# *diameter*

df.dropna(subset=['mag'], inplace=True)
df.dropna(subset=['r1'], inplace=True)

for v, mag, sz in zip(versions, mag_limits, sz_limits):
    mag_filter = df['mag'] <= mag
    sz_filter = df['r1'] >= sz
    df[mag_filter & sz_filter].to_parquet(out_fname_f(v))


# %%
