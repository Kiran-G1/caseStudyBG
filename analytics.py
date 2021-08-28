import os
import re

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from lib.logger import Logger

# to make driver & executor run with same version of python
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

"""
Helper function which strips the integers out of a string using regex
"""


def strip_ints_from_str(string):
    list_of_ints = re.findall(r'\d+', str(string))
    if len(list_of_ints) > 0:
        return list_of_ints[0]
    else:
        return 0


class Analytics:
    """
    constructor to initialize data members and start logger utility
    """

    def __init__(self, yaml_config):
        self.load_yaml_config(yaml_config)
        self.logger = Logger(self.job_name).logger

        self.spark = SparkSession.builder.appName(self.job_name).getOrCreate()

    def load_yaml_config(self, job_config_file):
        with open(job_config_file, 'r') as f:
            job_yaml_data = yaml.load(f.read())

        self.csv_files = job_yaml_data["data_resources"]
        self.job_name = job_yaml_data["job_name"]

    """
    Helper method which just logs to file and prints to console.
    """

    def print_and_log(self, msg):
        self.logger.info(msg)
        print(msg)

    """
        method to read the csv with spark and return a data-frame
    """

    def read_csv(self, path):
        df = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)
        return df

    """
    Driver method to perform analytics
    """

    def show_analytics(self):
        """
        Find the number of crashes (accidents) in which number of persons killed are male?
        """
        primary_person_df = self.read_csv(self.csv_files["primary_person"])

        males_killed_df = primary_person_df.filter(primary_person_df.PRSN_GNDR_ID == 'MALE').filter(
            primary_person_df.PRSN_INJRY_SEV_ID == 'KILLED')
        # selecting crash_id and dropping duplicates to get the exact count of accidents which involves male casualties.
        count_of_men_killed = males_killed_df.select("CRASH_ID").drop_duplicates().count()
        self.print_and_log("count of men killed:" + str(count_of_men_killed))

        """
        How many two wheelers are booked for crashes?
        """

        units_use_df = self.read_csv(self.csv_files["units_use"])
        two_wheelers = units_use_df.filter(units_use_df.VEH_BODY_STYL_ID.isin(['MOTORCYCLE', 'POLICE MOTORCYCLE']))
        # selecting vin which is unique and dropping duplicates records based on that to get exact count of motor cycles
        total_two_wheeler_crashes = two_wheelers.select("VIN").drop_duplicates().count()
        self.print_and_log("two wheelers involved in accidents:" + str(total_two_wheeler_crashes))

        """
        Which state has highest number of accidents in which females are involved?

        """
        female_casualties = primary_person_df.filter(primary_person_df.PRSN_GNDR_ID == 'FEMALE') \
            .filter(primary_person_df.DRVR_LIC_STATE_ID != 'NA')

        max_accident_state = female_casualties.drop_duplicates(["CRASH_ID"]) \
            .groupBy("DRVR_LIC_STATE_ID") \
            .count().orderBy("count", ascending=False) \
            .select("DRVR_LIC_STATE_ID", "count").first()

        self.print_and_log("most accidents occurred in the state with female casualties:" +
                           str(max_accident_state.DRVR_LIC_STATE_ID))

        """
        Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        """
        top15_vehicles_in_crash = units_use_df.filter((units_use_df.DEATH_CNT > 0) | (units_use_df.TOT_INJRY_CNT > 0)) \
            .drop_duplicates(["CRASH_ID"]).groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False).take(15)

        for top in range(4, 14):
            self.print_and_log("top-" + str(top) + " vehicle  make id is: " + top15_vehicles_in_crash[top].VEH_MAKE_ID)

        """
         For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        """
        primary_person_and_units = primary_person_df.join(units_use_df, on=["CRASH_ID"], how="inner") \
            .select("CRASH_ID", "PRSN_ETHNICITY_ID", "VEH_BODY_STYL_ID")

        list_of_body_types = primary_person_and_units.select("VEH_BODY_STYL_ID").drop_duplicates().collect()

        for body_type in list_of_body_types:
            body_type_str = str(body_type.VEH_BODY_STYL_ID)
            self.print_and_log("top ethnic type for body type " + body_type_str + " is: ")

            top_ethnic_for_body = primary_person_and_units.filter(
                primary_person_and_units.VEH_BODY_STYL_ID == body_type_str).groupBy(
                "PRSN_ETHNICITY_ID").count().orderBy("count", ascending=False).take(1)
            self.print_and_log(top_ethnic_for_body)

        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the
        contributing factor to a crash (Use Driver Zip Code)
        """
        units_use_df.filter(units_use_df.VEH_BODY_STYL_ID.isin(
            ['SPORT UTILITY VEHICLE', 'PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR'])
        ).filter(units_use_df.CONTRIB_FACTR_1_ID == 'UNDER INFLUENCE - ALCOHOL') \
            .drop_duplicates(["CRASH_ID"])

        top5_zip_codes = primary_person_df.join(units_use_df, "CRASH_ID", "inner").select("DRVR_ZIP") \
            .filter(primary_person_df.DRVR_ZIP > 0).groupBy("DRVR_ZIP") \
            .count().orderBy("count", ascending=False).take(4)
        self.print_and_log("top 5 zip codes which has highest car crashes under alcohol influence\n")
        for zip_code in top5_zip_codes:
            self.print_and_log(zip_code)

        """
         Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~)
          is above 4 and car avails Insurance

        """
        damages_df = self.read_csv(self.csv_files["damages"])

        strip_integers = udf(lambda x: strip_ints_from_str(x),
                             StringType()
                             )

        # performing left anti join to get ids which don't exist in damaged report.

        units_use_df.join(damages_df, "CRASH_ID", "left_anti").filter(
            (units_use_df.VEH_BODY_STYL_ID.isin(
                ['SPORT UTILITY VEHICLE', 'PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR']))
            &
            (units_use_df.FIN_RESP_TYPE_ID != 'NA')
            &
            (units_use_df.VEH_DMAG_SCL_1_ID != 'NA')).select("CRASH_ID", "VEH_DMAG_SCL_1_ID") \
            .withColumn("VEH_DMAG_SCL_INT", strip_integers(units_use_df.VEH_DMAG_SCL_1_ID)) \
            .filter(col("VEH_DMAG_SCL_INT") > 4).show()

        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed 
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of 
        offences (to be deduced from the data)
        """

        charges = self.read_csv(self.csv_files["charges"])

        speed_charges_unlicensed = charges.filter(charges.CHARGE.isin(['UNSAFE SPEED/ DISPLAY EX REG',
                                                                       'FAILURE TO CONTROL SPEED, '
                                                                       'NO DRIVER LICENSE',
                                                                       'FAILURE TO CONTROL SPEED ACCIDENT'])) \
            .join(units_use_df, "CRASH_ID", "inner").join(primary_person_df, "CRASH_ID", "inner") \
            .filter(primary_person_df.DRVR_LIC_CLS_ID != 'UNLICENSED')

        top_25_states = primary_person_df.select(["CRASH_ID", "DRVR_LIC_STATE_ID"]).drop_duplicates(["CRASH_ID"]) \
            .groupBy("DRVR_LIC_STATE_ID").count().orderBy("count", ascending=False).take(25)

        top_25_states = [element.DRVR_LIC_STATE_ID for element in top_25_states]

        self.print_and_log(top_25_states)

        top_10_colors = units_use_df.select(["CRASH_ID", "VEH_COLOR_ID"]).groupBy("VEH_COLOR_ID").count() \
            .orderBy("count", ascending=False).take(10)

        top_10_colors = [element.VEH_COLOR_ID for element in top_10_colors]

        self.print_and_log(top_10_colors)

        top5_vehicle_make_ids = speed_charges_unlicensed.filter(
            (speed_charges_unlicensed.DRVR_LIC_STATE_ID.isin(top_25_states))
            &
            (speed_charges_unlicensed.VEH_COLOR_ID.isin(top_10_colors))
        ) \
            .groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False)\

        top5_vehicle_make_ids.show()

        self.print_and_log("top 5 vehile make ids where drivers are charged with speeding and license state falls "
                           "in top  25 highest ticket states")

        self.print_and_log(top5_vehicle_make_ids.take(5))


if __name__ == "__main__":
    analytics_job = Analytics("./conf/job_conf.yaml")
    analytics_job.show_analytics()
