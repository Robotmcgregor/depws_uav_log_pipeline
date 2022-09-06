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
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import pandas as pd
import warnings
from glob import glob

warnings.filterwarnings("ignore")


def cmd_args_fn():
    p = argparse.ArgumentParser(
        description="""Process raw RMB odk outputs -> csv, shapefiles observational sheets, and Ras sheets.""")

    p.add_argument("-d", "--directory_odk", help="The directory containing ODK csv files.", default="raw_odk")

    p.add_argument("-x", "--export_dir", help="Directory path for outputs.",
                   default=r'Z:\Scratch\Rob\Drone')

    p.add_argument("-o", "--odk_drone_form", help="Path to the ODK Drone Xlsx form.",
                   default=r"Z:\Scratch\Rob\odk\odk_drone\odk_form\odk_drone.xlsx")

    p.add_argument("-c", "--chrome_driver", help="File path for the chrome extension driver.",
                   default=r"E:\DENR\code\rangeland_monitoring\rmb_aggregate_processing\assets\chrome_driver\chrome_driver_v89_0_4389_23\chromedriver.exe")

    p.add_argument("-r", "--remote_desktop", help="Working on the remote_desktop? - Enter remote_auto, remote, "
                                                  "local or offline.", default="remote_auto")

    p.add_argument("-t", "--time_sleep", help="Time between odk aggregate actions -if lagging increase integer",
                   default=20)

    p.add_argument("-ver", "--version", help="ODK version being processed (eg. v1, v2 etc.)",
                   default="v1")

    '''p.add_argument("-p", "--property_enquire",
                   help="Enter the name of of a single property you wish to process. (eg. Property Name)",
                   default=None)

    p.add_argument("-od", "--output_directory",
                   help="Enter path to output directory in the Spatial/Working drive)",
                   default=r'Z:\Scratch\Rob\Drone')'''

    cmd_args = p.parse_args()

    if cmd_args.directory_odk is None:
        p.print_help()

        sys.exit()

    return cmd_args


def temporary_dir_fn(export_dir):
    """ Create a temporary directory 'user_date_time'.
    :param export_dir: string object path to an existing directory where an output folder will be created.
    """
    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    final_user = user[3:]

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    temp_dir = export_dir + '\\' + final_user + '_' + str(date_time_list[0]) + '_' \
               + str(date_time_list_split[0]) + str(date_time_list_split[1])
    # check if the folder already exists - if False = create directory, if True = return error message zzzz.

    try:
        shutil.rmtree(temp_dir)

    except:
        print('The following directory did not exist line 105: ', temp_dir)

    # create folder a temporary folder titled (titled 'tempFolder)'
    os.makedirs(temp_dir)
    # print('temp dir created: ', temp_dir)

    prop_output_dir = '{}\\{}'.format(temp_dir, 'prop_output')
    if not os.path.exists(prop_output_dir):
        os.mkdir(prop_output_dir)

    return temp_dir


def assets_search_fn(search_criteria, folder):
    """ Using the search_criteria object search the asses sub-directory for a specific file.

    :param search_criteria: string object containing the name of a file including the file extension.
    :param folder: string object containing the name of a sub-directory located within the asset directory.
    :return: string object containing the located file name.
    """

    path_parent = os.path.dirname(os.getcwd())
    assets_dir = (path_parent + '\\' + folder)

    files = ""
    file_path = (assets_dir + '\\' + search_criteria)
    for files in glob(file_path):
        print(search_criteria, 'located.')

    return files


def main_routine():
    # todo update pipeline purpose
    """ This script processes the ODK Drone log form.
        Step 1: Initiate_odk_processing:
         - calls the command arguments
         - creates a temporary directory.

         Step 2: Aggregate collect raw data remote desktop
         - Loop through odk form list
         - Open and log into ODK Aggregate
         - Loop through the odk_forms list
         - select and export results csv
         - download csv and move to user directory.
         
         Step 3:
         - Convert csv to Pandas DataFrame
         - Iterate through the rows
         - Clean data, convert values to 0 = No 1 Yes for counts.

    """

    # read in the command arguments
    cmd_args = cmd_args_fn()
    directory_odk = cmd_args.directory_odk
    export_dir = cmd_args.export_dir
    chrome_driver_path = cmd_args.chrome_driver
    remote_desktop = cmd_args.remote_desktop
    time_sleep = int(cmd_args.time_sleep)
    version = cmd_args.version
    odk_form = cmd_args.odk_drone_form

    arn_csv = assets_search_fn("rangelands_uav_pilot_list.csv", "assets\\csv")
    print(arn_csv)

    # list of the odk files required for the observation and RAS sheets to complete.
    odk_form_list = ["DRONE_LOG_" + version]

    if remote_desktop == "remote_auto":
        # extract user name
        home_dir = os.path.expanduser("~")
        _, user = home_dir.rsplit('\\', 1)

        # remove all old result files from the Downloads directory from the PGB-BAS server.
        download_folder_path = "C:\\Users" + "\\" + user + "\\Downloads"
        files = glob(download_folder_path + '\\*results*.csv')
        # remove existing results files
        for f in files:
            os.remove(f)
            print('Located and removed: ', files, 'from', download_folder_path)

        # extract odk results csv files from aggregate.
        import step1_2_aggregate_collect_raw_data_remote_desktop
        odk_results_list = step1_2_aggregate_collect_raw_data_remote_desktop.main_routine(
            chrome_driver_path, odk_form_list, time_sleep)

        # purge result csv with 0 observations
        path_parent = os.path.dirname(os.getcwd())
        raw_odk_dir = (path_parent + '\\raw_odk')
        files = glob(raw_odk_dir + '\\*')
        for f in files:
            df = pd.read_csv(f)
            total_rows = len(df.index)
            if total_rows < 1:
                os.remove(f)
                print('Located and removed: ', files, 'from', raw_odk_dir)

        files = glob(download_folder_path + '\\*results.csv')
        # remove existing results files
        odk_results_list = []
        for f in files:
            df = pd.read_csv(f)
            total_rows = len(df.index)
            if total_rows > 1:
                _, file_ = f.rsplit('\\', 1)
                file_output = raw_odk_dir + '\\' + file_
                shutil.move(f, file_output)
                print('-' * 50)
                print('file_output: ', file_output)
                odk_results_list.append(file_output)
                print(file_, 'have been moved to ', raw_odk_dir)

    else:
        # All other workflows that are not remote_auto.
        odk_results_list = []

        for i in odk_form_list:
            file = i + '_results.csv'
            odk_results_list.append(file)
    # create an empty list for located result csv files for processing.
    located_list = []

    # call the temporary_dir_fn function to create a temporary directory located in cmd argument (-x) 'user_date_time'.
    temp_dir = temporary_dir_fn(export_dir)

    # create a directory to store csv files required for fractional cover analysis
    odk_complete_dir = temp_dir + "\\odk_complete"
    os.mkdir(odk_complete_dir)
    # print('='*50)
    # print('odk results list: ', odk_results_list)
    for results_csv in odk_results_list:
        print('- located: ', results_csv)
        print('- in directory: ', directory_odk)
        df = pd.read_csv("{0}\\{1}".format(directory_odk, results_csv))

        import step2_1_process_data
        step2_1_process_data.main_routine(df, arn_csv, odk_form)


if __name__ == '__main__':
    main_routine()
