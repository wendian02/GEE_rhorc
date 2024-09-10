import os, time, datetime
import numpy as np
import scipy.ndimage
import acolite as ac
import skimage.measure


def gee_launch(sensor, SUN_ELEVATION, vza = 0):
    reflectance = {}
    time_start = datetime.datetime.now()
    sensor = sensor
    sza = 90 - float(SUN_ELEVATION)
    ## read rsrd and get band wavelengths
    hyper = False
    ## hyperspectral
    # print("gem.gatts", gem.gatts)
    rsrd = ac.shared.rsr_dict(sensor)

    if sensor in rsrd:
        rsrd = rsrd[sensor]
    else:
        print('Could not find {} RSR'.format(sensor))
        # return ()

    # ## set wind to wind range
    # if gem.gatts['wind'] is None: gem.gatts['wind'] = setu['wind_default']
    # if par == 'romix+rsurf':
    #     gem.gatts['wind'] = max(2, gem.gatts['wind'])
    #     gem.gatts['wind'] = min(20, gem.gatts['wind'])
    # else:
    #     gem.gatts['wind'] = max(0.1, gem.gatts['wind'])
    #     gem.gatts['wind'] = min(20, gem.gatts['wind'])

    # ## get mean average geometry
    # geom_ds = ['sza', 'vza', 'raa', 'pressure', 'wind']
    # for ds in gem.datasets:
    #     if ('raa_' in ds) or ('vza_' in ds):
    #         gem.data(ds, store=True, return_data=False)
    #         geom_ds.append(ds)
    # for ds in geom_ds: gem.data(ds, store=True, return_data=False)
    # geom_mean = {k: np.nanmean(gem.data(k)) if k in gem.datasets else gem.gatts[k] for k in geom_ds}

    ## get gas transmittance
    # print("rsrd['rsr']", rsrd['rsr'])
    tg_dict = ac.ac.gas_transmittance(sza, vza, rsr=rsrd['rsr'])
    # print("tg_dict", tg_dict)

    ## make bands dataset
    for bi, b in enumerate(rsrd['rsr_bands']):
        if b not in reflectance:
            reflectance[b] = {k:rsrd[k][b] for k in ['wave_mu', 'wave_nm', 'wave_name'] if b in rsrd[k]}
            reflectance[b]['rhot_ds'] = 'rhot_{}'.format(reflectance[b]['wave_name'])
            reflectance[b]['rhos_ds'] = 'rhos_{}'.format(reflectance[b]['wave_name'])
            for k in tg_dict:
                if k not in ['wave']:
                    reflectance[b][k] = 1.0 - tg_dict[k][b]
            reflectance[b]['wavelength']=reflectance[b]['wave_nm']
    ## end bands dataset

    ## compute surface reflectances
    band_reflectance = {}
    for bi, b in enumerate(reflectance):
        # print(reflectance[b]['wave_name'])
        name = 'rhos_{}'.format(reflectance[b]['wave_name'])
        # print(name)
        band_reflectance[name] = reflectance[b]['tt_gas']
        print('Computing surface reflectance', b, name, reflectance[b]['tt_gas'])
    return band_reflectance

# L5_TM
# L8_OLI
# a = gee_launch(sensor= "L8_OLI", SUN_ELEVATION= 26.10933006)
# # sza = 90-float(meta[ik]['SUN_ELEVATION'])
# print(a)
