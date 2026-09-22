"""
Due to OSTIA data being rectilinear and model data being curvilinear we must use  a regridder function to enable comparisons. This takes the input from monthly_and_annual_average.py.
"""

import xarray as xr
import xesmf as xe

# Import datasets

ostia = xr.open_dataset('ostia_2018.nc')
ostia_average_temp = ostia.mean(dim = 'time')

annual_average_temp_og = xr.open_dataset('INPUT/DIRECTORY/annual_surface_temp_aowb/aowb_monthly_and_annual_surface_temp.nc')
annual_average_temp = annual_average_temp_og.set_coords(
    ["nav_lat", "nav_lon"]
).rename({"nav_lon": "lon", "nav_lat": "lat"})

# Regridding function
regridder = xe.Regridder(
    annual_average_temp,  # The source grid (curvilinear)
    ostia_average_temp,    # The target grid (rectilinear)
    method="bilinear",
    ignore_degenerate=True
)

ds_regrid = regridder(annual_average_temp)

# Save to NetCDF file
output_path = "INPUT/DIRECTORY/annual_surface_temp_aowb/aowb_monthly_and_annual_surface_temp_REGRID.nc"
ds_regrid.to_netcdf(output_path)

print(f"Saved successfully to {output_path}")
