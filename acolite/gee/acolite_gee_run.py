import acolite as ac
import os
import datetime
import pandas as pd
import shutil


def acolite_gee_run(input_image_dir,  # raw input path
                    output_nc_dir,  # acolite output path
                    output_csv_dir,  # matchup point csv output path
                    sources,  # ['Landsat 5', 'Landsat 7','Landsat 8','Landsat 9', 'Sentinel-2']
                    df_point, # point info
                    sdate, # start date
                    edate, # end date
                    is_clear=True):  # clear the output folder
    # save data as nc

    longitude = df_point["lon"]
    latitude = df_point["lat"]
    check_dates = df_point["date"]
    point_name = df_point["name"]

    # download image
    gem_files = ac.gem.extract(st_lon=longitude, st_lat=latitude, sdate=sdate, edate=edate,
                               check_dates=check_dates, max_diff_h=6,  # match points
                               width = 5,
                               st_name=point_name,  # save image name
                               output=input_image_dir, sources=sources)


    if len(gem_files) == 0:
        print('No images found for point {}'.format(point_name))
        return False

    else:
        time_start = datetime.datetime.now()
        time_start = time_start.strftime('%Y%m%d_%H%M%S')
        settings = {'inputfile': input_image_dir,
                    'output': output_nc_dir,
                    'polygon': '',
                    'l2w_parameters': ['rhorc_*'],
                    'output_rhorc': True,  # output rhorc
                    'rgb_rhot': False,
                    'rgb_rhos': False,
                    'map_l2w': False,
                    'runid': time_start}

        l1r_files = []
        for im_fn in os.listdir(input_image_dir): # all images of the matched point
            if '.DS_Store' in im_fn:
                continue
            temp = f"{input_image_dir}/{im_fn}"
            files = os.listdir(temp) # all match image of the matched point
            for fn in files:
                dir = "{}/{}/{}".format(settings["inputfile"], im_fn, fn)
                l1r_files.append(dir)

        ## do atmospheric correction
        l2r_files, l2t_files = [], []
        l2w_files = []
        for l1r in l1r_files:

            gatts = ac.shared.nc_gatts(l1r)
            l1r_setu = ac.acolite.settings.parse(gatts['sensor'], settings=settings)

            if 'acolite_file_type' not in gatts: gatts['acolite_file_type'] = 'L1R'
            ## do VIS-SWIR atmospheric correction
            if l1r_setu['atmospheric_correction']:
                if gatts['acolite_file_type'] == 'L1R':
                    ## run ACOLITE
                    ret = ac.acolite.acolite_l2r(l1r, settings=l1r_setu, verbosity=ac.config['verbosity'])
                    if len(ret) != 2: continue
                    l2r, l2r_setu = ret
                else:
                    l2r = '{}'.format(l1r)
                    l2r_setu = ac.acolite.settings.parse(gatts['sensor'], settings=l1r_setu)

                if (l2r_setu['adjacency_correction']):
                    ret = None
                    ## acstar3 adjacency correction
                    if (l2r_setu['adjacency_method'] == 'acstar3'):
                        ret = ac.adjacency.acstar3.acstar3(l2r, setu=l2r_setu, verbosity=ac.config['verbosity'])
                    ## GLAD
                    if (l2r_setu['adjacency_method'] == 'glad'):
                        ret = ac.adjacency.glad.glad_l2r(l2r, verbosity=ac.config['verbosity'], settings=l2r_setu)
                    l2r = [] if ret is None else ret

                ## if we have multiple l2r files
                if type(l2r) is not list: l2r = [l2r]
                l2r_files += l2r

                if l2r_setu['l2r_export_geotiff']:
                    for ncf in l2r:
                        ac.output.nc_to_geotiff(ncf, match_file=l2r_setu['export_geotiff_match_file'],
                                                cloud_optimized_geotiff=l1r_setu['export_cloud_optimized_geotiff'],
                                                skip_geo=l2r_setu['export_geotiff_coordinates'] is False)

                        if l2r_setu['l2r_export_geotiff_rgb']: ac.output.nc_to_geotiff_rgb(ncf, settings=l2r_setu)

                ## make rgb rhos maps
                if l2r_setu['rgb_rhos']:
                    l2r_setu_ = {k: l1r_setu[k] for k in l2r_setu}
                    l2r_setu_['rgb_rhot'] = False
                    for ncf in l2r:
                        ac.acolite.acolite_map(ncf, settings=l2r_setu_, plot_all=False)

                ## compute l2w parameters
                if l2r_setu['l2w_parameters'] is not None:
                    if type(l2r_setu['l2w_parameters']) is not list:
                        l2r_setu['l2w_parameters'] = [l2r_setu['l2w_parameters']]
                    for ncf in l2r:
                        ret = ac.acolite.acolite_l2w(ncf, settings=l2r_setu)
                        if ret is not None:
                            if l2r_setu['l2w_export_geotiff']: ac.output.nc_to_geotiff(ret,
                                                                                       match_file=l2r_setu['export_geotiff_match_file'],
                                                                                       cloud_optimized_geotiff=l1r_setu['export_cloud_optimized_geotiff'],
                                                                                       skip_geo=l2r_setu['export_geotiff_coordinates'] is False)
                            l2w_files.append(ret)

                            ## make l2w maps
                            if l2r_setu['map_l2w']:
                                ac.acolite.acolite_map(ret, settings=l2r_setu)
                            ## make l2w rgb
                            if l2r_setu['rgb_rhow']:
                                l2r_setu_ = {k: l1r_setu[k] for k in l2r_setu}
                                l2r_setu_['rgb_rhot'] = False
                                l2r_setu_['rgb_rhos'] = False
                                ac.acolite.acolite_map(ret, settings=l2r_setu_, plot_all=False)
        #   end save
        name_output = f"{output_csv_dir}/{point_name}.csv"
        match_rs = ac.gee.gee_nc2csv(input=output_nc_dir, output=name_output,
                                     longitude=longitude, latitude=latitude, point_info=df_point,
                                     save_points=True)


        # remove images and acolite output files
        if is_clear:
            shutil.rmtree(input_image_dir)
            shutil.rmtree(output_nc_dir)

        return match_rs


