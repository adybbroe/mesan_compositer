# Reduced integration-test data

`S_NWC_CTTH_noaa21_test_20260315T1045371Z_20260315T1047018Z.nc` is a
32 x 128 pixel subset of an operational-style NWCSAF/PPS NOAA-21 CTTH product.
It keeps only the real PPS NetCDF structure, geolocation, `ctth_alti`, and
metadata needed by the Satpy `nwcsaf-pps_nc` reader while making the integration
test small enough for CI. Palette and ancillary CTTH variables are deliberately
omitted because they are unrelated to the resampling regression.

The test TLE file is bundled only to make Pyorbital execution deterministic and
offline. The regression test checks array geometry/resampling, not orbital
accuracy.
