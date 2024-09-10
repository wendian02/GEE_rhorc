import os.path

import acolite as ac
import pandas as pd

import ee

ee.Authenticate()
ee.Initialize(project='ee-sniper')  # GEE project

# import warnings
# warnings.filterwarnings('ignore')

input_dir_all = "./input_nc"
output_nc_dir_all = "./output_acolite"
output_csv_dir_all = "./output_csv"
sources = ['Sentinel-2', 'Landsat 8', 'Landsat 9']
points_csv_fullfn = "points90.csv"

"""
一个Points匹配一个时间段的image，利用check_dates来过滤这个时间段的所有Image
check_dates是一个list，例如 [2024.1234, 2024.1235] 等等
修改之后，输入可以是一个时间的str，"2021/01/01 15:30:00"

"""
sdate = "2021-01"  # gee image search date range
edate = "2021-03"

df_match_all = pd.DataFrame()

point_list = pd.read_csv(points_csv_fullfn)
for index, df_point in point_list.iterrows():  # 返回所有的行索引
    point_name = df_point['name']
    longitude = df_point["lon"]
    latitude = df_point["lat"]
    input_dir = f"{input_dir_all}/{point_name}"
    output_nc_dir = f"{output_nc_dir_all}/{point_name}"

    rs = ac.gee.acolite_gee_run(input_dir, output_nc_dir, output_csv_dir_all,
                                sources, df_point,
                                sdate, edate, is_clear=False)  # is_clear=False, 不清空output_nc_dir
    if rs is not False:
        df_match_all = pd.concat([df_match_all, rs], axis=0)

# fill the sentinel unmatch band [sources only contain 'Sentinel-2']
if sources == ['Sentinel-2']:
    S2A_unmatch_band = ["rhorc_442", "rhorc_559", "rhorc_739", "rhorc_780", "rhorc_864", "rhorc_1610", "rhorc_2186"]
    S2B_unmatch_band = ["rhorc_443", "rhorc_560", "rhorc_740", "rhorc_783", "rhorc_865", "rhorc_1614", "rhorc_2202"]
    for (a, b) in zip(S2A_unmatch_band, S2B_unmatch_band):
        df_match_all[a] = df_match_all[a].fillna(df_match_all[b])
    df_match_all = df_match_all.drop(S2B_unmatch_band, axis=1)

df_match_all.to_csv(f"{os.path.splitext(points_csv_fullfn)[0]}_match_all.csv", index=False)
