"""
Script calculating monthly and annual averages in a useful format for plotting 
"""

import xarray as xr
import glob
import os
import calendar
from dask.distributed import Client
from dask.diagnostics import ProgressBar

# Directories
idir = '/gws/pw/j25/rep/omerrett/AOWB_RALP3P3_fixes/BGC'
odir = '/gws/pw/j25/rep/omerrett/AOWB_RALP3P3_fixes/Analysis/annual_surface_chloro'

def run_calc(idir, odir):
    # Ensure output directory exists
    os.makedirs(odir, exist_ok=True)
    output_filename = os.path.join(odir, "aowb_monthly_and_annual_surface_chloro.nc")

    # 1. Load the dataset
    files = sorted(glob.glob(f'{idir}/*.nc'))
    
    ds = xr.open_mfdataset(
        files, 
        combine='by_coords', 
        parallel=True,
        engine='h5netcdf',
        chunks={'time_counter': -1, 'x': 'auto', 'y': 'auto'}
    )

    # 2. Select the surface chlorophyll
    target_var = 'total_chlorophyll_calculator_result'
    ds_chloro = ds[target_var].isel(deptht=0)

    # 3. Create the Final Dataset
    final_ds = xr.Dataset()

    # --- ANNUAL CALCULATION ---
    print("Calculating annual mean...")
    final_ds['annual_chloro'] = ds_chloro.mean(dim='time_counter').compute()

    # --- MONTHLY CALCULATION ---
    print("Calculating monthly means...")
    # Group by month (1=Jan, 2=Feb, etc.)
    monthly_means = ds_chloro.resample(time_counter='MS').mean().compute()

    # Map month integers to names and add to dataset
    month_names = {i: calendar.month_name[i][:3].lower() for i in range(1, 13)}

    for i in range(len(monthly_means.time_counter)):
        m_idx = monthly_means.time_counter.dt.month.values[i]
        var_name = f"{month_names[m_idx]}_chloro"
        final_ds[var_name] = monthly_means.isel(time_counter=i).drop_vars('time_counter')

    # 4. Save to NetCDF
    print(f"Saving to: {output_filename}")
    with ProgressBar():
        final_ds.to_netcdf(output_filename)

    print("Done")

if __name__ == '__main__':
    from dask import config as cfg
    cfg.set({'distributed.scheduler.worker-ttl': None})
    client = Client(n_workers=7)
    
    print(f"--- DASK DASHBOARD ---")
    print(f"URL: {client.dashboard_link}")
    print(f"----------------------")
    
    run_calc(idir, odir)
    client.close()