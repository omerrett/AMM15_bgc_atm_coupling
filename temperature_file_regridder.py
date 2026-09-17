"""
Due to OSTIA data being rectilinear and model data being curvilinear we must use  a regridder function to enable comparisons
"""

import xarray as xr
import xesmf as xe

# Import datasets

ostia = xr.open_dataset('ostia_2018.nc')
ostia_average_temp = ostia.mean(dim = 'time')

annual_average_temp_og = xr.open_dataset('INPUT/DIRECTORY/annual_surface_temp/aowb_monthly_and_annual_surface_temp.nc')
annual_average_temp_og.set_coords(['nav_lat','nav_lon'])
annual_average_temp = annual_average_temp_og.rename({'nav_lon': 'lon', 'nav_lat': 'lat'})
annual_sst = annual_average_temp['votemper']

# Regridding function
regridder = xe.Regridder(
    annual_average_temp,  # The source grid (curvilinear)
    ostia_average_temp,    # The target grid (rectilinear)
    method="bilinear",
    ignore_degenerate=True
)

sst_regrid = regridder(annual_sst)


# Ensure the array has a name (this becomes the variable name in the .nc file)
sst_regrid.name = 'sst'

# Convert to Dataset
ds_output = sst_regrid.to_dataset()

# Save to NetCDF file
output_path = "OUTPUT/DIRECTORY/annual_surface_temp/average_temp_REGRID.nc"
ds_output.to_netcdf(output_path)

print(f"Saved successfully to {output_path}")
