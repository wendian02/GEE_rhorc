import acolite as ac
import os
import pandas as pd
import numpy as np


def gee_nc2csv(input, output, longitude, latitude, point_info, save_points=True):
    folders = os.listdir(input)

    files_all = []

    for i in folders:
        if ('L2W' in i) and ('.nc' in i):
            dir = "{}/{}".format(input, i)
            files_all.append(dir)

    df_rs = pd.DataFrame()
    for full_fn in files_all:
        extract_point = ac.shared.nc_extract_point(full_fn, longitude, latitude)
        extract_point_value = extract_point["data"]

        df_value = pd.DataFrame.from_dict(extract_point_value, orient='index').T

        df_value["image_date"] = extract_point["gatts"]["isodate"]
        df_value["satellite"] = extract_point["gatts"]["sensor"]
        if "MSI" in extract_point["gatts"]["sensor"]:
            df_value["CLOUD_COVER"] = extract_point["gatts"]["CLOUDY_PIXEL_PERCENTAGE"]
        elif "OLI" in extract_point["gatts"]["sensor"]:
            df_value["CLOUD_COVER"] = extract_point["gatts"]["CLOUD_COVER"]
        else:
            pass

        df_value["insitu_lon"] = point_info['lon']
        df_value["insitu_lat"] = point_info['lat']
        df_value["insitu_date"] = point_info['date']
        df_value["Id"] = point_info["Id"]  #

        df_rs = pd.concat([df_rs, df_value], axis=0)

    if save_points:
        df_rs.to_csv(output, encoding='utf-8', index=False, header=True)  # one point match one images

    return df_rs
