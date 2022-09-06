# !/usr/bin/env python

"""
Copyright 2021 Robert McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Import modules
import warnings
from datetime import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
import os

warnings.filterwarnings("ignore")


def string_clean_upper_fn(dirty_string):
    """ Remove whitespaces and clean strings.

            :param dirty_string: string object.
            :return clean_string: processed string object. """

    str1 = dirty_string.replace('_', ' ')
    str2 = str1.replace('-', ' ')
    str3 = str2.replace('  ', ' ')
    str4 = str3.upper()
    clean_str = str4.strip()

    if clean_str == 'End selection':
        clean_string = 'nan'
    else:
        clean_string = clean_str

    return clean_string


def string_clean_capital_fn(dirty_string):
    """ Remove whitespaces and clean strings.

            :param dirty_string: string object.
            :return clean_string: processed string object."""

    str1 = dirty_string.replace('_', ' ')
    str2 = str1.replace('-', ' ')
    str3 = str2.replace('  ', ' ')
    str4 = str3.capitalize()
    clean_str = str4.strip()

    if clean_str == 'End selection':
        clean_string = 'nan'
    else:
        clean_string = clean_str

    return clean_string


def string_clean_title_fn(dirty_string):
    """ Remove whitespaces and clean strings.

            :param dirty_string: string object.
            :return clean_string: processed string object. """

    str1 = dirty_string.replace('_', ' ')
    str2 = str1.replace('-', ' ')
    str3 = str2.replace('  ', ' ')
    str4 = str3.title()
    clean_str = str4.strip()

    if clean_str == 'End selection':
        clean_string = 'nan'
    else:
        clean_string = clean_str

    return clean_string


def single_variable(feature, row):
    variable = string_clean_capital_fn(str(row[feature]))
    if variable == 'Yes':
        var = 1
    elif variable == 'Nan':
        var = 0
    elif variable == 'No':
        var = 0

    else:
        var = variable
    return var


def single_variable_replace(feature, extract, replace, row):
    var = string_clean_capital_fn(str(row[feature]))
    # print('var: ', var)
    # print(type(var))
    variable = var.replace(extract, replace)

    return variable


def multiple_variable_unclean(feature, rng, row):
    var = str(row[feature])
    # print('Var...', var)
    var_ = var.split(' ')
    # print('var_', var_)

    var_list = []
    if var_[0] != 'Nan':
        # print('var length: ', len(var_))
        extra = rng - len(var_)
        for i in var_:
            var_list.append(i)
        for n in range(extra):
            var_list.append('No')
    else:

        for i in range(rng):
            var_list.append('No')

    return var_list


def other_variable(feature, row):
    variable = string_clean_capital_fn(str(row[feature]))

    if variable == 'Nan':
        var = 0
        var_ = 'None'
    else:
        var = 1
        var_ = variable
    return [var_, var]


def odk_choices(odk, n):
    choice_df = odk[odk['list_name'] == n]
    choice_list = sorted(choice_df.name.unique().tolist())
    print('choice_list: ', choice_list)
    length_ = len(choice_list)
    return choice_list, length_


def sort_required_list_fn(input_list, required_list, x):
    """ Sort the feral animals recorded into the required order for the ras sheet.

    :param feral_list: list object storing feral animal type variables created in feral_extraction_fn function.
    :param required_list: ordered list object created in main_routine: seven feral animal categories as sting
    variables.
    :param final_list: list object created in main_routine: seven list items of np.nan variables.
    :return: final_list ordered list object where matched variables replace (np.nan -> str(variable))
     no match (np.nan -> str(nan)).
     """

    final_list = [0] * x
    n = 0
    # loop through required list
    for i in required_list:
        print('required_list: ', i)
        print('input_list: ', input_list)
        # search for a required_list variable in the input_list
        if i in input_list:
            # append value to final_list in the ordered position
            final_list[n] = 1

        else:
            # append str(nan) to final_list in the ordered position
            final_list[n] = 0  # string ensures looping capability if no ferals recorded

        n += 1

    return final_list


def date_time_fn(row):
    """ Extract and reformat date and time fields.

            :param row: pandas dataframe row value object.
            :return date_time_list: list object containing two string variables: s_date2, obs_date_time. """

    # print(row['START'])
    s_date, s_time = row['START'].split('T')

    # print('s_date: ', s_date)
    # date clean

    form_date = s_date[-2:] + '/' + s_date[-5:-3] + '/' + s_date[:4]
    # print('form_date: ', form_date)
    s_date2 = s_date[-2:] + '/' + s_date[-5:-3] + '/' + s_date[2:4]
    photo_date = s_date.replace("-", "")

    # time clean
    s_hms, _ = s_time.split('.')
    s_hm = s_hms[:8]
    if s_hm[:1] == '0':
        s_hm2 = s_hm[1:8]
    else:
        s_hm2 = s_hm[:8]

    dirty_obs_time = datetime.strptime(s_hm2, '%H:%M:%S')

    obs_time = dirty_obs_time.strftime("%I:%M:%S %p")
    obs_date_time = form_date + ' ' + obs_time

    date_time_list = [form_date, obs_date_time]
    # print('date_time_list: ', date_time_list)

    return date_time_list, photo_date


def pilot_fn(row):
    """ Extract and reformat pilot and arn number information.

            :param row: pandas dataframe row value object.
            :return obs_pilot: string object containing pilot name. """
    pilot = str(row['PILOTS:FINAL_PILOT'])
    # clean variable, remove white space and possible typos
    pilot = string_clean_title_fn(pilot)
    first, second = pilot.split(' ')
    obs_pilot = second + ', ' + first

    return obs_pilot


def arn_fn(row, pilot, arn_csv):
    """ Extract and reformat arn number information.

            :param row: pandas dataframe row value object.
            :return obs_pilot: string object containing pilot name. """

    arn_df = pd.read_csv(arn_csv)
    # print(list(arn_df.columns))
    # print(pilot)
    arn_pilot = pilot.replace(', ', ' ')

    # convert pilot csv to dictionary (name/arn number.
    arn_dict = dict(zip(arn_df.pilot, arn_df.arn))
    arn = str(row['PILOTS:OTHER_PILOT_ARN'])

    if arn != 'nan':

        arn_final = string_clean_upper_fn(pilot)
        pass
    else:
        arn = 'nan'
        if arn_pilot in arn_dict:
            arn_final = arn_dict[arn_pilot]
        else:
            arn_final = 'Unknown'

    return str(arn_final)


def unit_fn(row):
    """ Extract working unit and clean other unit information.

        :param row: pandas dataframe row value object.
        :return final_unit: string object containing the unit information.
        """
    unit = str(row['PILOTS:FINAL_UNIT'])
    final_unit = string_clean_title_fn(unit)

    return final_unit


def gps_points_fn(row):
    """ Extract the centre point offset latitude, longitude, altitude, accuracy and datum information and export it as
    a list of variables.

            :param row: pandas dataframe row value object.
            :return gps: string object containing the gps device information.
            :return c_lat: float object containing the center point latitude information.
            :return c_lon: float object containing the center point longitude information.
            :return c_acc: float object containing the center point accuracy information (mobile device only).
            :return off_direct: string object containing the offset direction information.
            :return o_lat: float object containing the offset point latitude information.
            :return o_lon: float object containing the offset point longitude information.
            :return o_acc: float object containing the center point accuracy information. """

    gps = str(row['GPS_SELECT'])
    # mobile device - collectd at the beginning of the odk form.
    if gps == 'device':
        datum = 'wgs84'
        lat = float(row['GPS1:Latitude'])
        lon = float(row['GPS1:Longitude'])
        acc = float(row['GPS1:Accuracy'])

    # External device - collectd at the beginning of the odk form.
    elif gps == 'gps':
        # datum = 'wgs84'

        datum = str(row['EXT_GPS_COORD_CENTRE2:EXT_GPS_COORD_CENTRE_LAT2'])
        lat = float(row['EXT_GPS_COORD_CENTRE2:EXT_GPS_COORD_CENTRE_LAT2'])
        lon = float(row['EXT_GPS_COORD_CENTRE2:EXT_GPS_COORD_CENTRE_LONG2'])
        acc = np.nan


    lat_lon_list = [datum, gps, lat, lon, acc]

    return lat_lon_list


def drone_equipment_fn(row, string_clean_capital_fn):
    """ Extract the drone information.

        :param row: pandas dataframe row value object.
        :param string_clean_capital_fn: function to remove whitespaces and clean strings.
        :return: desc_list: list object containing n variables:
        """

    drone = single_variable('DRONES:FINAL_DRONE', row)
    micasense = single_variable('DRONES:FINAL_MICASENSE', row)
    prop = single_variable_replace('EQUIPMENT:PROPELLERS', 'Nan', 'No', row)

    prop = single_variable_replace('EQUIPMENT:PROPELLERS', 'Nan', 'No', row)
    bat_p4 = single_variable_replace('EQUIPMENT:BATTERY_P4', 'Nan', 'No', row)
    bat_mica = single_variable_replace('EQUIPMENT:BATTERY_MICA', 'Nan', 'No', row)
    bat_rtk = single_variable_replace('EQUIPMENT:BATTERY_RTK', 'Nan', 'No', row)

    drone_list = [drone, micasense, prop, bat_p4, bat_mica, bat_rtk]
    return drone_list


def pre_flight(row, string_clean_capital_fn):
    casa_rule = single_variable('PRE_FLIGHT:CASA_RULES', row)
    pre_check = single_variable('PRE_FLIGHT:PRE_FLIGHT_CHECK', row)
    pre_brief = single_variable('PRE_FLIGHT:PRE_FLIGHT_BRIEFING', row)

    return [casa_rule, pre_brief, pre_check]


def risk(row, string_clean_capital_fn, odk):
    haz_required, length_ = odk_choices(odk, 'HAZZARDS')

    risk_ = single_variable('RISK:RISK_ASSESSMENT', row)
    haz_list = multiple_variable_unclean('RISK:HAZZARDS', length_, row)
    final_haz_list = sort_required_list_fn(haz_list, haz_required, length_)
    other_haz_list = other_variable('RISK:HAZZARD_OTHER', row)
    # final_haz_list.append(other_haz)
    final_haz_list.extend(other_haz_list)
    final_haz_list.insert(0, risk_)

    return final_haz_list, haz_required


def post_flight(row, string_clean_capital_fn, odk):
    time = single_variable('POST_FLIGHT:FLIGHT_TIME', row)
    post_check = single_variable('POST_FLIGHT:POST_FLIGHT_CHECK', row)
    nm = single_variable('INC_NM:INCIDENT_NM', row)

    nm_required, length_ = odk_choices(odk, 'INCIDENT')
    nm_list = multiple_variable_unclean('INC_NM:INCIDENT', length_, row)
    final_nm_list = sort_required_list_fn(nm_list, nm_required, length_)
    other_nm_list = other_variable('INC_NM:INCIDENT_OTHER', row)
    # other_nm = single_variable_replace('INC_NM:INCIDENT_OTHER', 'Nan', 'No', row)

    list_a = [int(time), post_check, nm]
    # final_nm_list.append(other_nm)
    final_nm_list.extend(other_nm_list)
    list_a.extend(final_nm_list)

    return list_a, nm_required


def disposal(row, string_clean_capital_fn, odk):
    maintain_required, length_ = odk_choices(odk, 'MAINTENANCE')
    maintain_list = multiple_variable_unclean('MAINTAIN:MAINTENANCE', length_, row)
    final_maintain_list = sort_required_list_fn(maintain_list, maintain_required, length_)
    other_main_list = other_variable('MAINTAIN:MAIN_OTHER', row)
    # other_main = single_variable_replace('MAINTAIN:MAIN_OTHER', 'Nan', 'No', row)

    # list_a =[int(time), post_check, nm]
    # final_maintain_list.append(other_main)
    final_maintain_list.extend(other_main_list)
    # list_a.extend(nm_list)

    dispose_required, length_ = odk_choices(odk, 'DISPOSAL')
    dispose_list = multiple_variable_unclean('MAINTAIN:DISPOSAL', length_, row)
    final_dispose_list = sort_required_list_fn(dispose_list, dispose_required, length_)
    other_disp_list = other_variable('MAINTAIN:DISPOSAL_OTHER', row)
    # other_disp = single_variable_replace('MAINTAIN:DISPOSAL_OTHER', 'Nan', 'No', row)

    # list_a =[int(time), post_check, nm]
    # final_dispose_list.append(other_disp)
    final_dispose_list.extend(other_disp_list)

    comment = string_clean_capital_fn(str(row['MAINTAIN:COMMENT']))

    return final_maintain_list, maintain_required, final_dispose_list, dispose_required, comment


def meta_data(row):
    meta_id = (str(row['meta:instanceID']))
    meta_insta = (str(row['meta:instanceName']))
    return [meta_id, meta_insta]


def column_add(column, list_, var1, var2):
    for i in list_:
        column = column + ', ' + str(i)

    column + ', ' + var1 + ', ' + var2
    return column


def convert(list):
    tpl = tuple(i for i in list)
    return tpl


def main_routine(df, arn_csv, odk_form):

    odk = pd.read_excel(odk_form, sheet_name='choices')
    final_clean_list = []

    for index, row in df.iterrows():
        clean_list = []
        # print(row)
        date_time_list, photo_date = date_time_fn(row)
        pilot = pilot_fn(row)
        arn = arn_fn(row, pilot, arn_csv)
        gps_list = gps_points_fn(row)
        drone_equipment_list = drone_equipment_fn(row, string_clean_capital_fn)
        unit = unit_fn(row)
        pre_list = pre_flight(row, string_clean_capital_fn)
        risk_list, haz_required = risk(row, string_clean_capital_fn, odk)
        nm_list, nm_required = post_flight(row, string_clean_capital_fn, odk)
        maintain_list, maintain_required, dispose_list, dispose_required, comment = disposal(
            row, string_clean_capital_fn, odk)
        meta_list = meta_data(row)

        # ---------------------------------------- extend/append to list -----------------------------------------------
        clean_list.extend(date_time_list)
        clean_list.extend([pilot, arn, unit])
        clean_list.extend(gps_list)
        clean_list.extend(drone_equipment_list)
        clean_list.extend(pre_list)
        clean_list.extend(risk_list)
        clean_list.extend(nm_list)
        clean_list.extend(dispose_list)
        clean_list.extend(maintain_list)
        clean_list.append(comment)
        clean_list.extend(meta_list)
        final_clean_list.append(clean_list)


    # ---------------------------------------------- feature_names -----------------------------------------------------
    # print('date_time_list: ', date_time_list)
    # print('drone equip: ', drone_equipment_list)
    # print('pre_list: ', pre_list)

    column_name_list = ['date', 'date_time', 'pilot', 'arn', 'unit', 'datum', 'gps', 'lat', 'lon', 'acc', 'drone',
                        'micasense', 'prop', 'bat_p4', 'bat_mica', 'bat_rtk', 'casa_rule', 'pre_brief', 'pre_check',
                        'risk']

    column_name_list.extend(haz_required)
    column_name_list.extend(['haz_o_typ', 'haz_othr'])
    column_name_list.extend(['flt_time', 'post_check', 'near_miss'])
    column_name_list.extend(nm_required)
    column_name_list.extend(['nm_o_typ', 'nm_othr'])
    column_name_list.extend(maintain_required)
    column_name_list.extend(['mtain_o_typ', 'mtain_othr'])
    column_name_list.extend(dispose_required)
    column_name_list.extend(['disp_o_typ', 'disp_othr'])
    column_name_list.extend(['comment', 'met_key', 'meta_form'])
    print(column_name_list)
    column_tpl = convert(column_name_list)

    drone_df = pd.DataFrame(final_clean_list)
    drone_df.columns = column_tpl

    drone_df.to_csv(r"Z:\Scratch\Rob\odk\odk_drone\outputs\drone.csv", index=False)
    print('df shape: ', drone_df.shape)


if __name__ == "__main__":
    main_routine()
