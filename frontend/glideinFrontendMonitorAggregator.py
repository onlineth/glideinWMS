from __future__ import print_function
#
# Project:
#   glideinWMS
#
# File Version: 
#
# Description:
#   This module implements the functions needed
#   to aggregate the monitoring fo the frontend
#
# Author:
#   Igor Sfiligoi (Mar 19th 2009)
#

import time
import os.path
import os
import tempfile
import shutil

from glideinwms.lib import logSupport
from glideinwms.lib import xmlParse, xmlFormat
from glideinwms.lib import rrdSupport

from glideinwms.frontend import glideinFrontendMonitoring

def config_frontend(monitor_dir, groups):
    glideinFrontendMonitoring.Monitoring_Output.updateConfigAggr("monitor_dir", monitor_dir)
    glideinFrontendMonitoring.Monitoring_Output.updateConfigAggr("groups", groups)
    glideinFrontendMonitoring.Monitoring_Output.updateConfig("monitor_dir", monitor_dir)


###########################################################
#
# Functions
#
###########################################################

# PM: Nov 26, 2014
# There is a limit on rrd field names. Max allowed is 20 chars long.
# RRD enforces this limit while creating fields, but will not enforce the limits
# when trying to read from a field with name longer than 20 chars.
# Truncate the names for following to be in limits to avoid above issue.
frontend_status_attributes = {
    'Jobs':("Idle", "OldIdle", "Running", "Total", "Idle_3600"),
    'Glideins':("Idle", "Running", "Total"),
    'MatchedJobs':("Idle", "EffIdle", "OldIdle", "Running", "RunningHere"),
    'MatchedGlideins':("Total", "Idle", "Running", "Failed"),
    #'MatchedGlideins':("Total","Idle","Running","Failed","TCores","ICores","RCores"),
    'MatchedCores':("Total", "Idle", "Running"),
    'Requested':("Idle", "MaxRun")
}

frontend_total_type_strings = {
    'Jobs':'Jobs',
    'Glideins':'Glidein',
    'MatchedJobs':'MatchJob',
    'MatchedGlideins':'MatchGlidein',
    'MatchedCores':'MatchCore',
    'Requested':'Req'
}

frontend_job_type_strings = {
    'MatchedJobs':'MatchJob',
    'MatchedGlideins':'MatchGlidein',
    'MatchedCores':'MatchCore',
    'Requested':'Req'
}


####################################
rrd_problems_found=False
def verifyHelper(filename,dict, fix_rrd=False):
    """
    Helper function for verifyRRD.  Checks one file,
    prints out errors.  if fix_rrd, will attempt to 
    dump out rrd to xml, add the missing attributes,
    then restore.  Original file is obliterated.

    @param filename: filename of rrd to check
    @param dict: expected dictionary
    @param fix_rrd: if true, will attempt to add missing attrs
    """
    global rrd_problems_found
    if not os.path.exists(filename):
        print("WARNING: %s missing, will be created on restart" % (filename))
        return
    rrd_obj=rrdSupport.rrdSupport()
    (missing, extra)=rrd_obj.verify_rrd(filename, dict)
    for attr in extra:
        print("ERROR: %s has extra attribute %s" % (filename, attr))
        if fix_rrd:
            print("ERROR: fix_rrd cannot fix extra attributes")
    if not fix_rrd:
        for attr in missing:
            print("ERROR: %s missing attribute %s" % (filename, attr))
        if len(missing) > 0:
            rrd_problems_found=True
    if fix_rrd and (len(missing) > 0):
        (f, tempfilename)=tempfile.mkstemp()
        (out, tempfilename2)=tempfile.mkstemp()
        (restored, restoredfilename)=tempfile.mkstemp()
        backup_str=str(int(time.time()))+".backup"
        print("Fixing %s... (backed up to %s)" % (filename, filename+backup_str))
        os.close(out)
        os.close(restored)
        os.unlink(restoredfilename)
        #Use exe version since dump, restore not available in rrdtool
        dump_obj=rrdSupport.rrdtool_exe()
        outstr=dump_obj.dump(filename)
        for line in outstr:
            os.write(f, "%s\n"%line)
        os.close(f)
        rrdSupport.addDataStore(tempfilename, tempfilename2, missing)
        os.unlink(filename)
        outstr=dump_obj.restore(tempfilename2, restoredfilename)
        os.unlink(tempfilename)
        os.unlink(tempfilename2)
        shutil.move(restoredfilename, filename)
    if len(extra) > 0:
        rrd_problems_found=True

def verifyRRD(fix_rrd=False):
    """
    Go through all known monitoring rrds and verify that they
    match existing schema (could be different if an upgrade happened)
    If fix_rrd is true, then also attempt to add any missing attributes.
    """
    global rrd_problems_found
    # FROM: migration_3_1
    # dir=monitorAggregatorConfig.monitor_dir
    # total_dir=os.path.join(dir, "total")
    mon_dir = glideinFrontendMonitoring.Monitoring_Output.global_config_aggr["monitor_dir"]

    status_dict = {}
    status_total_dict = {}
    for tp in frontend_status_attributes.keys():
        if tp in frontend_total_type_strings.keys():
            tp_str = frontend_total_type_strings[tp]
            attributes_tp = frontend_status_attributes[tp]
            for a in attributes_tp:
                status_total_dict["%s%s" % (tp_str, a)] = None
        if tp in frontend_job_type_strings.keys():
            tp_str = frontend_job_type_strings[tp]
            attributes_tp = frontend_status_attributes[tp]
            for a in attributes_tp:
                status_dict["%s%s" % (tp_str, a)] = None

    if not os.path.isdir(mon_dir):
        print("WARNING: monitor/ directory does not exist, skipping rrd verification.")
        return True
    # FROM: migration_3_1
    # for filename in os.listdir(dir):
    #     if (filename[:6]=="group_") or (filename=="total"):
    #         current_dir=os.path.join(dir, filename)
    #         if filename=="total":
    #             verifyHelper(os.path.join(current_dir,
    #                 "Status_Attributes.rrd"), status_total_dict, fix_rrd)
    #         for dirname in os.listdir(current_dir):
    #             current_subdir=os.path.join(current_dir, dirname)
    #             if dirname[:6]=="state_":
    #                 verifyHelper(os.path.join(current_subdir,
    #                     "Status_Attributes.rrd"), status_dict, fix_rrd)
    #             if dirname[:8]=="factory_":
    #                 verifyHelper(os.path.join(current_subdir,
    #                     "Status_Attributes.rrd"), status_dict, fix_rrd)
    #             if dirname=="total":
    #                 verifyHelper(os.path.join(current_subdir,
    #                     "Status_Attributes.rrd"), status_total_dict, fix_rrd)
    for dir_name, sdir_name, f_list in os.walk(mon_dir):
        for file_name in f_list:
            if file_name == 'Status_Attributes.rrd':
                if os.path.basename(dir_name) == 'total':
                    verifyHelper(os.path.join(dir_name, file_name),
                                 status_total_dict, fix_rrd)
                else:
                    verifyHelper(os.path.join(dir_name, file_name),
                                 status_dict, fix_rrd)

    return not rrd_problems_found


##############################################################################
# create an aggregate of status files, write it in an aggregate status file
# end return the values
def aggregateStatus():

    type_strings = {
        'Jobs': 'Jobs',
        'Glideins': 'Glidein',
        'MatchedJobs': 'MatchJob',
        'MatchedGlideins': 'MatchGlidein',
        'MatchedCores': 'MatchCore',
        'Requested': 'Req'
    }
    global_total = {
        'Jobs': None,
        'Glideins': None,
        'MatchedJobs': None,
        'Requested': None,
        'MatchedGlideins': None,
        'MatchedCores': None,
    }
    status={'groups': {}, 'total': global_total}
    global_fact_totals = {}

    for fos in ('factories', 'states'):
        global_fact_totals[fos] = {}
    
    nr_groups = 0
    for group in glideinFrontendMonitoring.Monitoring_Output.global_config_aggr["groups"]:
        # load group status file
        status_fname = os.path.join(os.path.join(glideinFrontendMonitoring.Monitoring_Output.global_config_aggr["monitor_dir"], 'group_'+group),
                                    glideinFrontendMonitoring.Monitoring_Output.global_config_aggr["status_relname"])
        try:
            group_data=xmlParse.xmlfile2dict(status_fname)
        except xmlParse.CorruptXML as e:
            logSupport.log.error("Corrupt XML in %s; deleting (it will be recreated)." % (status_fname))
            os.unlink(status_fname)
            continue
        except IOError:
            continue  # file not found, ignore

        # update group
        status['groups'][group] = {}
        for fos in ('factories', 'states'):
            try:
                status['groups'][group][fos] = group_data[fos]
            except KeyError as e:
                # first time after upgrade factories may not be defined
                status['groups'][group][fos] = {}

        this_group=status['groups'][group]
        for fos in ('factories', 'states'):
          for fact in this_group[fos].keys():
            this_fact = this_group[fos][fact]
            if not fact in global_fact_totals[fos].keys():
                # first iteration through, set fact totals equal to the first group's fact totals
                global_fact_totals[fos][fact]={}
                for attribute in type_strings.keys():
                    global_fact_totals[fos][fact][attribute] = {}
                    if attribute in this_fact.keys():
                        for type_attribute in this_fact[attribute].keys():
                            this_type_attribute=this_fact[attribute][type_attribute]
                            try:
                                global_fact_totals[fos][fact][attribute][type_attribute] = int(this_type_attribute)
                            except:
                                pass
            else:
                # next iterations, factory already present in global fact totals, add the new factory values to the previous ones
                for attribute in type_strings.keys():
                    if attribute in this_fact.keys():
                        for type_attribute in this_fact[attribute].keys():
                            this_type_attribute = this_fact[attribute][type_attribute]
                            if isinstance(this_type_attribute, type(global_fact_totals[fos])):
                                # dict, do nothing
                                pass
                            else:
                                if attribute in global_fact_totals[fos][fact].keys() and type_attribute in global_fact_totals[fos][fact][attribute].keys():
                                   global_fact_totals[fos][fact][attribute][type_attribute] += int(this_type_attribute)
                                else:
                                   global_fact_totals[fos][fact][attribute][type_attribute] = int(this_type_attribute)
        #nr_groups+=1
        #status['groups'][group]={}

        if 'total' in group_data:
            nr_groups += 1
            status['groups'][group]['total'] = group_data['total']

            for w in global_total.keys():
                tel = global_total[w]
                if w not in group_data['total']:
                    continue
                #status['groups'][group][w]=group_data[w]
                el = group_data['total'][w]
                if tel is None:
                    # new one, just copy over
                    global_total[w] = {}
                    tel = global_total[w]
                    for a in el.keys():
                        tel[a] = int(el[a]) #coming from XML, everything is a string
                else:                
                    # successive, sum 
                    for a in el.keys():
                        if a in tel:
                            tel[a] += int(el[a])
                              
                    # if any attribute from prev. factories are not in the current one, remove from total
                    for a in tel.keys():
                        if a not in el:
                            del tel[a]


        
    for w in global_total.keys():
        if global_total[w] is None:
            del global_total[w] # remove group if not defined

    # Write xml files


    updated=time.time()
    xml_str=('<?xml version="1.0" encoding="ISO-8859-1"?>\n\n'+
             '<VOFrontendStats>\n'+
             xmlFormat.time2xml(updated, "updated", indent_tab=xmlFormat.DEFAULT_TAB, leading_tab=xmlFormat.DEFAULT_TAB)+"\n"+
             xmlFormat.dict2string(status["groups"], dict_name="groups", el_name="group",
                                   subtypes_params={"class":{"dicts_params":{"factories":{"el_name":"factory",
                                                                                          "subtypes_params":{"class":{"subclass_params":{"Requested":{"dicts_params":{"Parameters":{"el_name":"Parameter",
                                                                                                                                                                                    "subtypes_params":{"class":{}}}}}}}}},
                                                                             "states":{"el_name":"state",
                                                                                          "subtypes_params":{"class":{"subclass_params":{"Requested":{"dicts_params":{"Parameters":{"el_name":"Parameter",
                                                                                                                                                                                    "subtypes_params":{"class":{}}}}}}}}}
                                                                             }}},
                                   leading_tab=xmlFormat.DEFAULT_TAB)+"\n"+
             xmlFormat.class2string(status["total"], inst_name="total", leading_tab=xmlFormat.DEFAULT_TAB)+"\n"+

             xmlFormat.dict2string(global_fact_totals['factories'], dict_name="factories", el_name="factory",
                                   subtypes_params={"class":{"subclass_params":{"Requested":{"dicts_params":{"Parameters":{"el_name":"Parameter",

       "subtypes_params":{"class":{}}}}}}}},
                                   leading_tab=xmlFormat.DEFAULT_TAB)+"\n"+
             xmlFormat.dict2string(global_fact_totals['states'], dict_name="states", el_name="state",
                                   subtypes_params={"class":{"subclass_params":{"Requested":{"dicts_params":{"Parameters":{"el_name":"Parameter",

       "subtypes_params":{"class":{}}}}}}}},
                                   leading_tab=xmlFormat.DEFAULT_TAB)+"\n"+
             "</VOFrontendStats>\n")

    glideinFrontendMonitoring.Monitoring_Output.write_file(glideinFrontendMonitoring.Monitoring_Output.global_config_aggr["status_relname"], xml_str)
                
    # Write rrds

    for out in glideinFrontendMonitoring.Monitoring_Output.out_list:
        out.write_aggregation(global_fact_totals, updated, global_total)

    return status
