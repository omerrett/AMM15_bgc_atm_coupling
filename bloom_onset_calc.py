import xarray as xr
import numpy as np
from scipy.signal import savgol_filter
from dask.distributed import Client
from dask import config as cfg


def analyse_peaks(data, times):
    """Identify bloom onset and extract the explicit calendar date directly."""

    # Smooth time series
    data = savgol_filter(data, 7, 2, axis=0)

    try:
        grad = np.diff(data)

        #This requires 0.15 mg m^-3 of growth increase for 5 continuous days
        N = 5      # Sustained growth days
        T = 0.15   # Growth increase threshold


        bloom_onset_idx = np.flatnonzero(
            np.convolve(grad > T, np.ones(N, dtype=int), 'valid') >= N
        )

        if bloom_onset_idx.size > 0:
            idx_val = float(bloom_onset_idx[0])
            
            # Extract the actual timestamp object for this specific file entry
            actual_time = times[bloom_onset_idx[0]]
            
            # Convert the datetime64 object cleanly into an encoded float format: YYYYMMDD.0
            date_str = np.datetime_as_string(actual_time, unit='D').replace('-', '')
            date_verify_val = float(date_str)
        else:
            idx_val = np.nan
            date_verify_val = np.nan

        return np.array([idx_val, date_verify_val], dtype='float64')

    except Exception:
        return np.array([np.nan, np.nan], dtype='float64')

# Main phenology function
def run_phenology(chl_fname, output_fname='phenology.nc'):

    # Open dataset with Dask chunking
    ds = xr.open_dataset(chl_fname).chunk({
        'y': 100,
        'x': 100,
        'time_counter': -1
    })

    varname = 'chloro_top'

    # Apply bloom detection
    peaks = xr.apply_ufunc(
        analyse_peaks,
        ds[varname],
        ds['time_counter'],                       
        input_core_dims=[['time_counter'], ['time_counter']],  
        output_core_dims=[['metrics']],
        dask_gufunc_kwargs={'output_sizes': {'metrics': 2}},
        dask='parallelized',
        output_dtypes=['float64'],
        vectorize=True
    )

    # Build output dataset
    # This feautres two values (one as an index and one as a value of the date) to check debugging in case of missing dates in the dataset
    ds_out = xr.Dataset(
        {
            f'{varname}_bloom_onset_index': peaks.isel(metrics=0),
            f'{varname}_date_verify_raw': peaks.isel(metrics=1),
        },
        coords={
            'nav_lat': ds['nav_lat'],
            'nav_lon': ds['nav_lon'],
        }
    )

    # Save
    ds_out.to_netcdf(output_fname)
    print(f'Phenology analysis saved to {output_fname}')

# Runs in parallel
if __name__ == '__main__':

    cfg.set({'distributed.scheduler.worker-ttl': None})
    client = Client(n_workers=5)

    try:

        infile = (
        'ANNUAL_DEPTH_AVERAGED_FILE.nc'
        )

        outfile = (
        'OUTPUT/DIRECTORY/'
        'bloom_onset_date.nc'
        )

        run_phenology(infile, output_fname=outfile)

    finally:
        client.close()
