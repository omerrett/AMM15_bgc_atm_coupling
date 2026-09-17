"""
This script takes shelftmb files, calcualtes a daily average value for net shortwave radiation (qsr) and saves that to a file for each day.
These are then averaged monthly and then annualy and saved.
"""

import xarray as xr
import glob
import os
import tqdm

idir = 'INPUT/DIRECTORY/shelftmb'
odir = 'OUTPUT/DIRECTORY/qsr'

files = sorted(glob.glob(idir + '/*.nc'))

print("Processing daily files...")
for f in tqdm.tqdm(files): 
    with xr.open_dataset(f, chunks={}) as ds:
        # Get datetime object and string representation
        current_date = ds.time_counter.values[0]
        date_str = str(ds.time_counter.dt.strftime('%Y-%m-%d').values[0])
        output_filename = f"{odir}/daily_qsr/daily_qsr_{date_str}.nc"
        
        # Skips preexisting files 
        if os.path.exists(output_filename):
            continue

        # Compute the daily mean
        mean_qsr = ds['qsr'].mean(dim='time_counter', keep_attrs=True)
        
        # Expand dimensions to keep 'time_counter' with its actual date
        ds_out = mean_qsr.expand_dims(time_counter=[current_date]).to_dataset(name='qsr')
        
        # Saves daily file
        ds_out.to_netcdf(output_filename)

print("\nProcessing monthly and annual aggregations...")

# Opens all newly created daily files together
ds_daily_all = xr.open_mfdataset(f"{odir}/daily_qsr/daily_qsr_*.nc", combine='by_coords')

# Averages monthly 
# Issues can arrise here. For older xarray versions, use '1M' or 'M'
ds_monthly = ds_daily_all.resample(time_counter='1ME').mean(dim='time_counter', keep_attrs=True)
ds_monthly.to_netcdf(f"{odir}/qsr_monthly_means_2018.nc")
print(f"Saved monthly file to: {odir}/qsr_monthly_means_2018.nc")

# Averages annualy
ds_yearly = ds_daily_all.mean(dim='time_counter', keep_attrs=True)
ds_yearly.to_netcdf(f"{odir}/qsr_annual_means_2018.nc")
print(f"Saved annual file to: {odir}/qsr_annual_means_2018.nc")
