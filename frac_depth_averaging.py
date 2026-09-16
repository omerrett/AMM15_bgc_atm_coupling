"""
Calculates depth-weighted vertical averages for 3D ocean/biogeochemical model outputs.

Handles partial cell thickness weighting across vertical cell layers (deptht) to integrate to user specified depth (avg_depth)

"""



from dask.distributed import Client
import dask
import xarray as xr
import glob
import tqdm
import os

# Choose depth to average to here.
avg_depth = 50.0

def run_calc(idir,odir):
    phy_files = sorted(glob.glob(idir+'/Analysis/daily_e3t/outputs/*.nc')) 
    bgc_files = sorted(glob.glob(idir+'/BGC/*.nc'))
    # Assumes files are split into /BGC for biogeochemical files and that daily e3t value files exist
    
    for i,(p,b) in tqdm.tqdm(enumerate(zip(phy_files,bgc_files))): 

        # Names the files with dates
        with xr.open_dataset(b, chunks={}) as ds_temp:
            date_str = str(ds_temp.time_counter.dt.strftime('%Y-%m-%d').values[0])
        output_filename = f"{odir}/top{avg_depth}_{date_str}.nc"

        
        # Skips preexisting files    
        if os.path.exists(output_filename):
            continue

        # Processing
        #Chunks to enable faster processing
        ds_phy = xr.open_dataset(p).chunk({'x': 300, 'y': 300}) 
        ds_bgc = xr.open_dataset(b).chunk({'x': 300, 'y': 300}) 

        ds_top = ds_phy[['nav_lon','nav_lat']].copy()
        
        # Asigns cell thickness
        thickness = ds_phy.e3t 

        # Computes the depth that the top and bottom of cell are at
        cell_bottom_depth = thickness.cumsum('deptht')
        cell_top_depth = cell_bottom_depth - thickness

        # Assigns binary mask to all full cells above the chosen depth
        top_mask = xr.where(cell_bottom_depth <= avg_depth, 1, 0)

        straddle = (cell_top_depth < avg_depth) & (cell_bottom_depth > avg_depth)
        frac_val = (avg_depth - cell_top_depth) / thickness


        frac_mask = xr.where(straddle, frac_val, top_mask)

        frac_depth = (thickness * frac_mask).sum('deptht')

        # This is for the variable N3_n, change to appropriate variable as required

        weighted_sum = (ds_bgc.N3_n * thickness * frac_mask).sum('deptht')

        ds_top['N3_n_top'] = weighted_sum / frac_depth.where(frac_depth > 0)

        ds_top.to_netcdf(output_filename, unlimited_dims='time_counter') 

if __name__ == '__main__':
    # Enables to be run in parallel
    from dask import config as cfg
    cfg.set({'distributed.scheduler.worker-ttl': None})
    client = Client(n_workers=8)

    #Change these as required
    idir = 'INPUT/DIRECTORY'  
    odir = f"OUTPUT/DIRECTORY/depth_avg_bgc/outputs_frac_nutrients_{avg_depth}m"

    if not os.path.exists(odir):
        os.makedirs(odir)

    run_calc(idir,odir)