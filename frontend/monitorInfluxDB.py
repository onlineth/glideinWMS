#!/usr/bin/env python
#
# Project:
#   glideinWMS
#
# File Version:
#
# Description:
#   This module implements the basic functions needed
#   to interface to InfluxDB for the Frontend
#
# Author:
#   Thomas Hein
#
import socket
from influxdb import InfluxDBClient
from glideinwms.frontend.glideinFrontendMonitoring import Monitoring_Output
from glideinwms.lib import logSupport


# Default Configuration
DEFAULT_CONFIG = {
    "name": "monitorInfluxDB",
    "db_credentials": [],
    "databases": [],
    "hostname": str(socket.gethostname()) # Host Info
}

DEFAULT_CONFIG_AGGR = {}


# noinspection PyRedeclaration
class Monitoring_Output(Monitoring_Output):
    def __init__(self, config, configAgg):
        # Get Default Config from Parent
        super(Monitoring_Output, self).__init__()

        # Set Default Config for this Child
        for key in DEFAULT_CONFIG:
            self.config[key] = DEFAULT_CONFIG[key]

        for key in DEFAULT_CONFIG_AGGR:
            self.configAggr[key] = DEFAULT_CONFIG_AGGR[key]

        # Set Config from Pass Parameters (from the Frontend XML Config File)
        for key in config:
            self.config[key] = config[key]

        for key in configAgg:
            self.configAggr[key] = configAgg[key]

        # Init for InfluxDB
        '''
        This is a hard coded example of how to connect muitiple databases

        self.db_credentials = [
            ['localhost', 8086, 'root', 'root', 'frontend_stats'],
            ['fermicloud532.fnal.gov', 8086, 'frontend93824', 'd43h7487y4328', 'gwms_stats']]
        '''
        self.db_credentials = [['fermicloud532.fnal.gov', 8086, 'frontend93824', 'd43h7487y4328', 'gwms_stats']]

        self.databases = []

        # Connect to databases
        for currDB in self.db_credentials:
            try:
                # Connect to the database with the credentials
                self.databases.append(InfluxDBClient(currDB[0], currDB[1], currDB[2], currDB[3], currDB[4]))

                # Create the database if it has not yet been created
                self.databases[-1].create_database(currDB[4])
            except Exception, e:
                pass
                # Errror connecting to currDB
                # logSupport.log.warning("InfluxDB: Cannot connect to '%s' database. Error: %s" % (currDB[4], str(e)))

    def write_groupStats(self, total, factories_data, states_data, updated):
        json_body = []

        # States Data for Frontend
        # data['states']
        name = "Frontend.states."

        for currState, value in states_data.iteritems():
            for currAttribute, value2 in value.iteritems():
                if (isinstance(value2, dict)):
                    for subElemKey, subElemValue in value2.iteritems():
                        if (isinstance(subElemValue, dict)):
                            print(subElemKey)
                            value2[subElemKey] = str(subElemValue)
                    if value2: # Check for empty dictionaries
                        json_body.append({"measurement": name+currAttribute, "tags": {"State": currState, "Type": "Frontend", "Host": self.config["hostname"]}, "fields": value2})
                else:
                    if value2: # Check for empty dictionaries
                        json_body.append({"measurement": name+"Status", "tags": {"State": currState, "Type": "Frontend", "Host": self.config["hostname"]}, "fields": {currAttribute: value2}})


        # States Data for Factories
        # data['factories']
        name = "Frontend.states."

        for currHost, value in factories_data.iteritems():
            for currAttribute, value2 in value.iteritems():
                if (isinstance(value2, dict)):
                    for subElemKey, subElemValue in value2.iteritems():
                        if (isinstance(subElemValue, dict)):
                            print(subElemKey)
                            value2[subElemKey] = str(subElemValue)
                    if value2: # Check for empty dictionaries
                        json_body.append({"measurement": name+currAttribute, "tags": {"Type": "Factory", "FactoryHost": currHost, "Host": self.config["hostname"]}, "fields": value2})
                else:
                    if value2: # Check for empty dictionaries
                        json_body.append({"measurement": name+"Status", "tags": {"Type": "Factory", "FactoryHost": currHost, "Host": self.config["hostname"]}, "fields": {currAttribute: value2}})

        # Totals Data for Frontend
        # data['totals']
        name = "Frontend.Totals"

        for type, value in total.iteritems():
            if (isinstance(value, dict)):
                if value:
                    json_body.append({"measurement": name, "tags": {"Type": type, "Host": self.config["hostname"]}, "fields": value})

        # Done collecting data, now submit
        self.SubmitData("Frontend_groupStats", json_body)

    def write_factoryStats(self, data, total_el, updated):
        with open("/test.txt", "a") as myfile:
            myfile.write("\nwrite_factoryStats: " + str(data) + " " + str(total_el) + " " + str(updated))

    def write_aggregation(self, global_fact_totals, updated, global_total, status):
        with open("/test.txt", "a") as myfile:
            myfile.write("\nwrite_aggregation: "+str(global_fact_totals)+" "+str(updated)+" "+ str(global_total)+" "+str(status))

    def verify(self, fix):
        pass
        # if not (self.verifyRRD(fix["fix_rrd"])):
        #     self.verifyError = "Run with -fix_rrd option to update errors\n" \
        #                        "WARNING: back up your existing rrds before auto-fixing rrds"
        #     return True
        # return False

    # Internal Functions
    def SubmitData(self, name, json_data):
        # Loop over each database
        for currDBClient in self.databases:
            # Takes the prepared data and submits to the databases
            try:
                currDBClient.write_points(json_data)
            except Exception, e:
                # Error when submitting data
                logSupport.log.warning("InfluxDB: Cannot submit dataset for %s. Error: %s" % (name, str(e)))

