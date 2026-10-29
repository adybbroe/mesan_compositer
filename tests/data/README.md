# Reduced integration-test data

The NOAA-21 files in this directory are 32 x 128 pixel subsets of two
consecutive operational-style NWCSAF/PPS granules from 2026-03-15.

For each granule, the reduced fixtures contain the real PPS NetCDF structure,
geolocation, timing and metadata needed by the Satpy `nwcsaf-pps_nc` reader.
The CTTH fixtures keep only `ctth_alti`; the CMA fixtures keep only `cma`.
Palette and ancillary variables are deliberately omitted because they are
unrelated to the resampling regression.

The two granules are used together to exercise Satpy's `MultiScene` resampling
and weighted blending path. This protects against regressions where the CTTH
data are resampled to the target area but the satellite-zenith-angle weights
remain on their original swath geometry.

The bundled TLE file makes Pyorbital execution deterministic and offline. The
regression tests check array geometry/resampling, not orbital accuracy.

The remapped Meteosat CTTH fixture is a 32 x 32 crop of the real `meps4km`
product. It retains the fixed-grid projection coordinates and mapping metadata
needed to exercise the `nwcsaf-pps_nc-remapped` single-scene path without
requiring the full operational file.
