"""
Code that averages a chosen variable over a 100x100 grid cell region and saves a daily file for each of these.
These then need to be combined into one file e.g. 'OUTPUT/DIRECTORY/temp_hovmuller/regional/off_shelf/annual.nc' for plotting.
"""

import xarray as xr
from dask.distributed import Client, progress
import dask
import glob
from pathlib import Path
import os

# Create a directory for each region's outputs 
def setup_directories(odir):

    Path(odir + "/regional/off_shelf/").mkdir(parents=True, exist_ok=True)
    Path(odir + "/regional/shelf_break_n/").mkdir(parents=True, exist_ok=True)
    Path(odir + "/regional/shelf_break_s/").mkdir(parents=True, exist_ok=True)
    Path(odir + "/regional/northsea/").mkdir(parents=True, exist_ok=True)

def run_calc(infile, odir, i):
    # Assign variables of choice (e.g. temperature and salinity)
    vars = ['votemper','vosaline']
    try:
        ds = xr.open_dataset(infile)[vars]
      
        # Averages over a 100x100 grid cell region and saves a daily file for each of these.      
      
        # Off-shelf
        dat_off_shelf = ds.isel(y=slice(1050, 1150), x=slice(200, 300)).mean(['x', 'y'])
        dat_off_shelf.to_netcdf(f'{odir}/regional/off_shelf/temp_{i:03d}.nc')
        
        # Shelf-break
        dat_shelf_break_n = ds.isel(y=slice(800, 900), x=slice(480, 580)).mean(['x', 'y'])
        dat_shelf_break_n.to_netcdf(f'{odir}/regional/shelf_break_n/temp_{i:03d}.nc')

        dat_shelf_break_s = ds.isel(y=slice(175, 270), x=slice(450, 550)).mean(['x', 'y'])
        dat_shelf_break_s.to_netcdf(f'{odir}/regional/shelf_break_s/temp_{i:03d}.nc')
        
        # North Sea
        dat_north_sea = ds.isel(y=slice(800, 900), x=slice(900, 1000)).mean(['x', 'y'])
        dat_north_sea.to_netcdf(f'{odir}/regional/northsea/temp_{i:03d}.nc')

        return f"Success: {Path(infile).name}"
    except Exception as e:
        return f"Failed: {Path(infile).name} with error {e}"

# This bit carries out the claculations in parallel
if __name__ == '__main__':

    client = Client(n_workers=5)
    print(f"Dask dashboard link: {client.dashboard_link}")
    
    # Define input and output directories
    idir = 'INPUT/DIRECTORY'
    odir = 'OUTPUT/DIRECTORY/temp_hovmuller'
    
    # Create output directories before starting to avoid conflicts
    setup_directories(odir)
    
    # Find all the files to process
    files = sorted(glob.glob(f'{idir}/*.nc'))
    
    # Build a list of tasks for Dask
    tasks = []
    for i, infile in enumerate(files):
        check_file = f'{odir}/regional/northsea/bgc_{i:03d}.nc'
        
        if os.path.exists(check_file):
            # Skip this file and move to the next
            continue
        # Wraps run_calc function with dask.delayed
        task = dask.delayed(run_calc)(infile, odir, i)
        tasks.append(task)
        
    # Execute all the tasks
    if tasks:
        print(f"Starting parallel processing of {len(tasks)} files...")
        results = dask.compute(*tasks, progress=progress)
        print("Processing complete.")
    else:
        print(f"Warning: No files found in {idir}")
            
    client.close()
