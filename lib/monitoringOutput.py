#
# Description:
#   Monitoring_Output allows a module monitoring system
#   for both the frontend and the factory
#
#
# Author:
#   Thomas Hein
#


from __future__ import print_function

import copy, os, os.path
import string

from glideinwms.lib import logSupport


class Monitoring_Output(object):
    out_list = []

    DEFAULT_CONFIG = {"monitor_dir": "monitor/",
                      "name": "Monitor_Name"}

    DEFAULT_CONFIG_AGGR = {"monitor_dir": "monitor/",
                           "groups": [],
                           "entries": [],
                           "status_relname": "frontend_status.xml"}

    global_config = copy.deepcopy(DEFAULT_CONFIG)
    global_config_aggr = copy.deepcopy(DEFAULT_CONFIG_AGGR)

    def __init__(self):
        self.config = copy.deepcopy(Monitoring_Output.DEFAULT_CONFIG)
        self.configAggr = copy.deepcopy(Monitoring_Output.DEFAULT_CONFIG_AGGR)

    # Override methods

    def write_groupStats(self, total, factories_data, states_data, updated):
        pass

    def write_factoryStats(self, data, total_el, updated):
        pass

    def write_aggregation(self, global_fact_totals, updated, global_total, status):
        pass

    def verify(self, fix):
        # fix is a dictionary that may contain parameters passed from reconfig
        # (ie. fix["fix_rrd"] = True # Then fix the RRD Files)
        return False

    # Common Methods
    def _updateConfig(self, key, value):
        if key in self.config:
            self.config[key] = value
        else:
            raise ValueError("Attempted to Update a Key that did not exsist")

    def _updateConfigAggr(self, key, value):
        if key in self.configAggr:
            self.configAggr[key] = value
        else:
            raise ValueError("Attempted to Update a Key that did not exsist")

    # Static Functions
    @staticmethod
    def createOutList():
        if not (Monitoring_Output.out_list):
            from glideinwms.lib import monitorRRD, monitorXML
            monitorRRD_config = {}
            out = monitorRRD.Monitoring_Output({}, {})
            Monitoring_Output.out_list.append(out)
            out = monitorXML.Monitoring_Output({}, {})
            Monitoring_Output.out_list.append(out)

    @staticmethod
    def updateConfig(key, val, element=None):
        if element:
            element._updateConfig(key, val)
        else:
            if key in Monitoring_Output.global_config:
                Monitoring_Output.global_config[key] = val
            for out in Monitoring_Output.out_list:
                out._updateConfig(key, val)

    @staticmethod
    def updateConfigAggr(key, val, element=None):
        if element:
            element._updateConfigAggr(key, val)
        else:
            if key in Monitoring_Output.global_config_aggr:
                Monitoring_Output.global_config_aggr[key] = val
            for out in Monitoring_Output.out_list:
                out._updateConfigAggr(key, val)

    @staticmethod
    def write_file(relative_fname, output_str):
        fname = os.path.join(Monitoring_Output.global_config["monitor_dir"], relative_fname)
        if not os.path.isdir(os.path.dirname(fname)):
            os.makedirs(os.path.dirname(fname))
        # print "Writing "+fname
        fd = open(fname + ".tmp", "w")
        try:
            fd.write(output_str + "\n")
        finally:
            fd.close()

        tmp2final(fname)
        return

    @staticmethod
    def establish_dir(relative_dname):
        dname = os.path.join(Monitoring_Output.global_config["monitor_dir"], relative_dname)
        if not os.path.isdir(dname):
            os.mkdir(dname)
        return

##################################################

##################################################
def tmp2final(fname):
    """
    This exact method is also in glideFactoryMonitoring.py
    """
    try:
        os.remove(fname+"~")
    except:
        pass

    try:
        os.rename(fname, fname+"~")
    except:
        pass

    try:
        os.rename(fname+".tmp", fname)
    except:
        print("Failed renaming %s.tmp into %s"%(fname, fname))
        logSupport.log.error("Failed renaming %s.tmp into %s" % (fname, fname))
    return


##################################################
def sanitize(name):
    good_chars=string.ascii_letters+string.digits+".-"
    outarr=[]
    for i in range(len(name)):
        if name[i] in good_chars:
            outarr.append(name[i])
        else:
            outarr.append("_")
    return string.join(outarr, "")

##################################################

##################################################

Monitoring_Output.createOutList()