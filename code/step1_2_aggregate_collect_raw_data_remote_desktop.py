#!/usr/bin/env python

"""
Copyright 2021 Robert McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Import modules
import selenium
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time
import os
import shutil
import sys


def odk_aggregate_log_in_fn(driver, time_sleep):
    """Log in to ODK Aggregate using Selenium."""

    # open and log in to ODK Aggregate
    driver.get("https://odktest:odktest@pgb-bas14.nt.gov.au:8443/ODKAggregate")

    # allow a five second time interval before the next function
    # time.sleep(time_sleep)

    # print('Selenium is in control of: ', driver.title)

    return driver


def close_diver_fn(driver):
    # close the chrome driver.
    # print('driver is about to close')
    time.sleep(5)
    driver.close()
    driver.quit()


def odk_form_extraction_fn(driver, odk_form_list, chrome_driver, time_sleep):
    for page in odk_form_list:

        odk_aggregate_log_in_fn(driver)

        # Find and select the the form dropdown menu.
        s1 = Select(driver.find_element_by_xpath("//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select"))
        time.sleep(20)
        # print('selected the form dropdown')
        # from the form dropdown select by text the required page (loop iteration)
        s1.select_by_visible_text(page)
        time.sleep(20)
        # print('selected the appropriate form')

        # todo if table is larger than 1

        try:
            # find and click on the export data button
            driver.find_element_by_xpath(
                "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button").click()
            # print(page, ' located')
            time.sleep(20)

            try:
                # find and click on the export csv button, no filers have been selected.
                driver.find_element_by_xpath("/html/body/div[5]/div/table/tbody/tr/td[7]/button").click()
                # print(page, ' html file being prepared for download')
                time.sleep(20)

                driver.find_element_by_xpath(
                    "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[2]/td/div/div[2]/div/table/tbody/tr[3]/td[4]/div").click()
                # print('download ', page, 'results csv')

                time.sleep(20)

                # print('Navigate back to the Filter submissions page.')

            except NoSuchElementException:
                # print(page, 'does not contain data')
                pass

        except NoSuchElementException:
            # print(page, 'does not contain data')
            pass

    close_diver_fn(driver)


def odk_export_csv_checker_fn(search_criteria):
    """ Search for a specific odk csv output.

        :param located_list:
        :param dir_path: string object containing the raw odk output csv files.
        :param search_criteria: string object containing the raw odk file name and type.
        :param temp_dir: string object path to the created output directory (date_time).
        :param veg_list: string object path to the odk veglist excel file (containing botanical and common names).
        :param shrub_list_excel: string object path to the odk veglist excel file (containing a 3P grass list).
        :param pastoral_estate: string object containing the file path to the pastoral estate shapefile. """

    # print(os.getcwd())

    path_parent = os.path.dirname(os.getcwd())
    # print('path_parent: ', path_parent)
    raw_odk_dir = (path_parent + '\\raw_odk')
    # raw_odk_dir = directory_odk
    # print('raw_odk_dir: ', raw_odk_dir)

    user_downloads_dir = os.path.join(os.path.expanduser('~'), 'downloads')
    # print('user_downloads_dir: ', user_downloads_dir)
    file_path = (user_downloads_dir + '\\' + search_criteria)
    # print(file_path)

    # print('=================================')
    # print('Searching for: ', search_criteria)

    if not os.path.exists(file_path):
        # print(search_criteria, ' not located.')
        pass

    else:
        # print(search_criteria, ' located, exporting file..........')
        # call the import_script_fn function.
        # print(file_path, raw_odk_dir + '\\' + search_criteria)
        # print('______________________________________________________')
        # print('file_path: ', file_path)
        # print('raw_odk_dir:', raw_odk_dir)
        shutil.move(file_path, raw_odk_dir + '\\' + search_criteria)


def main_routine(chrome_driver, odk_form_list, time_sleep):
    """
    Extract the URL's contained within the Star Transect ODK Aggregate .csv for the star transect repeats (transects).
    Open and log into ODK Aggregate using the testodk username and password,
    navigate and open the transect tables, and download the table data as a .html so that it can be imported as a Pandas
    data frame in the following script.
    :param time_sleep: integer object utilised to put the script to sleep between tasks (n = 1 second)
    :param odk_form_list: list object containing the names of the relevant odk forms for access and downloading results.
    :param chrome_driver: string object containing the path to the appropriate chrome diriver executable.
    """


    # define the Chrome Web driver path
    driver = webdriver.Chrome(chrome_driver)
    time.sleep(5)

    # log in to aggregate
    odk_aggregate_log_in_fn(driver, time_sleep)

    for page in odk_form_list:

        # Find and select the the form dropdown menu.
        time.sleep(time_sleep * 1)
        dropdown_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
            (By.XPATH, "//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select")))

        if EC.element_selection_state_to_be(dropdown_state, is_selected=True):

            s1 = Select(WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select"))))
            time.sleep(2)
            if EC.visibility_of(s1):
                s1.select_by_visible_text(page)
            else:
                close_diver_fn(driver)
                import sys
                sys.exit()

            # ------------------------------------------- export button ------------------------------------------------

            time.sleep(time_sleep * 1)
            export_button_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
                (By.XPATH, "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button")))
            time.sleep(2)

            if export_button_state:
                # print('able to click the export button')
                # Define an element that you can start scraping when it appears
                # If the element appears after 5 seconds, break the loop and continue
                export_button_element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                    (By.XPATH, "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button")))
                # print('export_button_element has been located')
                time.sleep(2)
                if EC.element_to_be_clickable(
                        (By.XPATH, "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button")):
                    export_button_element.click()
                else:
                    close_diver_fn(driver)
                    import sys
                    sys.exit()

                # ---------------------------------------- export csv button -------------------------------------------

                time.sleep(time_sleep * 1)
                csv_export_button_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
                    (By.XPATH, "/html/body/div[5]/div/table/tbody/tr/td[7]/button")))
                time.sleep(2)

                if csv_export_button_state:
                    # print('able to click the export button')
                    # Define an element that you can start scraping when it appears
                    # If the element appears after 5 seconds, break the loop and continue
                    csv_export_button_element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                        (By.XPATH, "/html/body/div[5]/div/table/tbody/tr/td[7]/button")))
                    time.sleep(2)

                    if EC.element_to_be_clickable(
                            (By.XPATH, "/html/body/div[5]/div/table/tbody/tr/td[7]/button")):
                        csv_export_button_element.click()
                    else:
                        close_diver_fn(driver)
                        import sys
                        sys.exit()

                    # -------------------------------------- hyperlink download ----------------------------------------

                    time.sleep(time_sleep * 1)
                    # attempt to locate the first hyperlink the the table, when the element becomes visible.
                    hyperlink_state = WebDriverWait(driver, 10).until(EC.visibility_of_element_located(
                        (By.XPATH,
                         "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[2]/td/div/div[2]/div/table/tbody/tr[3]/td[4]/div")))
                    time.sleep(2)
                    if hyperlink_state:
                        # print('able to click the hyperlink')
                        # Define an element that you can start scraping when it appears
                        # If the element appears after 5 seconds, break the loop and continue
                        hyperlink_element = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(
                            (By.XPATH,
                             "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[2]/td/div/div[2]/div/table/tbody/tr[3]/td[4]/div")))
                        time.sleep(2)
                        hyperlink_element.click()

                    # -------------------------------------- return to home page ---------------------------------------

                    time.sleep(time_sleep * 1)
                    home_page_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
                        (By.XPATH,
                         "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td[2]/div/div")))
                    time.sleep(2)

                    if home_page_state:
                        # print('able to click the export button')
                        # Define an element that you can start scraping when it appears
                        # If the element appears after 5 seconds, break the loop and continue
                        home_page_element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                            (By.XPATH,
                             "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td[2]/div/div")))
                        time.sleep(2)

                        if EC.element_to_be_clickable(
                                (By.XPATH,
                                 "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td[2]/div/div")):
                            home_page_element.click()
                        else:
                            close_diver_fn(driver)
                            import sys
                            sys.exit()

                    else:
                        close_diver_fn(driver)
                        import sys
                        sys.exit()

                else:
                    close_diver_fn(driver)
                    import sys
                    sys.exit()
            else:

                close_diver_fn(driver)
                import sys
                sys.exit()

        else:

            close_diver_fn(driver)
            import sys
            sys.exit()

    driver.quit()


if __name__ == "__main__":
    main_routine()
