import xarray as xr
import glob

file_pattern = "/gws/pw/j25/rep/omerrett/OWB/PHY/*.nc"
files = sorted(glob.glob(file_pattern))

ds = xr.open_mfdataset(files, combine='by_coords')
chl = ds['votemper'] 

# 3. Extract the 5 versions
# Individual cells
c375_725 = chl.isel(y=375, x=725).drop_vars(['nav_lat', 'nav_lon'])
c374_725 = chl.isel(y=374, x=725).drop_vars(['nav_lat', 'nav_lon'])
c375_726 = chl.isel(y=375, x=726).drop_vars(['nav_lat', 'nav_lon'])
c374_726 = chl.isel(y=374, x=726).drop_vars(['nav_lat', 'nav_lon'])

# 2x2 Spatial Mean
mean_2x2 = chl.isel(y=slice(374, 376), x=slice(725, 727)).mean(dim=['x', 'y'])

# 4. Merge into a single Dataset
l4_options = xr.Dataset({
    'cell_375_725': c375_725,
    'cell_374_725': c374_725,
    'cell_375_726': c375_726,
    'cell_374_726': c374_726,
    '2x2': mean_2x2
})

# 5. Add some helpful metadata
l4_options.attrs['description'] = "Potential Temperature"
l4_options.attrs['units'] = "celsius"

# 6. Save to one file
l4_options.to_netcdf("/gws/pw/j25/rep/omerrett/OWB/Analysis/L4_analysis/L4_temperature_OWB.nc")

print("File saved: L4_temperature_Options_2018.nc")