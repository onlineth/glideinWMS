import time, os
from glideinwms.frontend.glideinFrontendMonitoring import Monitoring_Output, sanitize
from glideinwms.lib import rrdSupport
from glideinwms.lib import logSupport


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

# Default Configuration
DEFAULT_CONFIG = {"attributes": {
            'Jobs':("Idle", "OldIdle", "Running", "Total", "Idle_3600"),
            'Glideins':("Idle", "Running", "Total"),
            'MatchedJobs':("Idle", "EffIdle", "OldIdle", "Running", "RunningHere"),
            #'MatchedGlideins':("Total","Idle","Running","Failed","TotalCores","IdleCores","RunningCores"),
            'MatchedGlideins':("Total", "Idle", "Running", "Failed"),
            'MatchedCores':("Total", "Idle", "Running"),
            'Requested':("Idle", "MaxRun")
        },
    "rrd_step": 300,       #default to 5 minutes
    "rrd_heartbeat": 1800, #default to 30 minutes, should be at least twice the loop time
    "rrd_archives": [('AVERAGE', 0.8, 1, 740),      # max precision, keep 2.5 days
                           ('AVERAGE', 0.92, 12, 740),       # 1 h precision, keep for a month (30 days)
                           ('AVERAGE', 0.98, 144, 740)        # 12 hour precision, keep for a year
                           ],
    "states_names": ('Unmatched', 'MatchedUp', 'MatchedDown')
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

        self.rrd_obj = rrdSupport.rrdSupport()

        self.updated = time.time()

    def write_groupStats(self, total, factories_data, states_data, updated):
        self.updated = updated

        self.write_one_rrd("total", total)

        for fact in factories_data.keys():
            self.write_one_rrd("factory_%s" % sanitize(fact), factories_data[fact], 1)

        for fact in states_data.keys():
            self.write_one_rrd("state_%s" % sanitize(fact), states_data[fact], 1)

    def write_aggregation(self, global_fact_totals, updated, global_total):
        Monitoring_Output.establish_dir("total")
        self.write_one_rrd_aggr("total/Status_Attributes", updated, global_total, 0)

        for fact in global_fact_totals['factories'].keys():
            fe_dir = "total/factory_%s" % sanitize(fact)
            Monitoring_Output.establish_dir(fe_dir)
            self.write_one_rrd_aggr("%s/Status_Attributes" % fe_dir, updated, global_fact_totals['factories'][fact], 1)
        for fact in global_fact_totals['states'].keys():
            fe_dir = "total/state_%s" % sanitize(fact)
            Monitoring_Output.establish_dir(fe_dir)
            self.write_one_rrd_aggr("%s/Status_Attributes" % fe_dir, updated, global_fact_totals['states'][fact], 1)



    ###############################
    # PRIVATE - Used by write_file
    # Write one RRD
    def write_one_rrd(self, name, data, fact=0):
        val_dict={}
        if fact==0:
            type_strings = {
                'Jobs':'Jobs',
                'Glideins':'Glidein',
                'MatchedJobs':'MatchJob',
                'MatchedGlideins':'MatchGlidein',
                'MatchedCores':'MatchCore',
                'Requested':'Req'
            }
        else:
            type_strings = {
                'MatchedJobs':'MatchJob',
                'MatchedGlideins':'MatchGlidein',
                'MatchedCores':'MatchCore',
                'Requested':'Req'
            }

        #init, so that all get created properly
        for tp in self.config["attributes"].keys():
            if tp in type_strings.keys():
                tp_str=type_strings[tp]
                attributes_tp=self.config["attributes"][tp]
                for a in attributes_tp:
                    val_dict["%s%s"%(tp_str, a)]=None


        for tp in data:
            # type - Jobs,Slots
            if not (tp in self.config["attributes"].keys()):
                continue
            if not (tp in type_strings.keys()):
                continue

            tp_str=type_strings[tp]

            attributes_tp=self.config["attributes"][tp]

            fe_el_tp=data[tp]
            for a in fe_el_tp.keys():
                if a in attributes_tp:
                    a_el=fe_el_tp[a]
                    if not isinstance(a_el, dict): # ignore subdictionaries
                        val_dict["%s%s"%(tp_str, a)]=a_el

        Monitoring_Output.establish_dir("%s"%name)
        self.write_rrd_multi("%s/Status_Attributes"%name,
                                         "GAUGE", self.updated, val_dict)

    def write_rrd_multi(self, relative_fname, ds_type, time, val_dict, min_val=None, max_val=None):
        """
        Create a RRD file, using rrdtool.
        """
        if self.rrd_obj.isDummy():
            return  # nothing to do, no rrd bin no rrd creation

        for tp in ((".rrd", self.config["rrd_archives"]),):
            rrd_ext, rrd_archives = tp
            fname = os.path.join(self.config["monitor_dir"], relative_fname + rrd_ext)
            # print "Writing RRD "+fname

            if not os.path.isfile(fname):
                # print "Create RRD "+fname
                if min_val is None:
                    min_val = 'U'
                if max_val is None:
                    max_val = 'U'
                ds_names = sorted(val_dict.keys())

                ds_arr = []
                for ds_name in ds_names:
                    ds_arr.append((ds_name, ds_type, self.config["rrd_heartbeat"], min_val, max_val))
                self.rrd_obj.create_rrd_multi(fname,
                                              self.config["rrd_step"], rrd_archives,
                                              ds_arr)

            # print "Updating RRD "+fname
            try:
                self.rrd_obj.update_rrd_multi(fname, time, val_dict)
            except Exception as e:
                logSupport.log.error("Failed to update %s" % fname)
                # logSupport.log.exception(traceback.format_exc())
        return


    ####################################
    # PRIVATE - Used by aggregateStatus
    # Write one RRD
    def write_one_rrd_aggr(self, name, updated, data, fact=0):
        if fact == 0:
            type_strings = frontend_total_type_strings
        else:
            type_strings = frontend_job_type_strings

        # initialize the RRD dictionary, so it gets created properly
        val_dict = {}
        for tp in frontend_status_attributes.keys():
            if tp in type_strings.keys():
                tp_str = type_strings[tp]
                attributes_tp = frontend_status_attributes[tp]
                for a in attributes_tp:
                    val_dict["%s%s" % (tp_str, a)] = None

        for tp in data.keys():
            # type - status or requested
            if not (tp in frontend_status_attributes.keys()):
                continue
            if not (tp in type_strings.keys()):
                continue

            tp_str = type_strings[tp]
            attributes_tp = frontend_status_attributes[tp]

            tp_el = data[tp]

            for a in tp_el.keys():
                if a in attributes_tp:
                    a_el = int(tp_el[a])
                    if not isinstance(a_el, dict):  # ignore subdictionaries
                        val_dict["%s%s" % (tp_str, a)] = a_el

        Monitoring_Output.establish_dir("%s" % name)
        self.write_rrd_multi("%s" % name, "GAUGE", updated, val_dict)
