import time, os
from glideinwms.frontend.glideinFrontendMonitoring import Monitoring_Output, sanitize
from glideinwms.lib import rrdSupport
from glideinwms.lib import logSupport


# noinspection PyRedeclaration
class Monitoring_Output(object, Monitoring_Output):
    def __init__(self):
        self.attributes = {
            'Jobs':("Idle", "OldIdle", "Running", "Total", "Idle_3600"),
            'Glideins':("Idle", "Running", "Total"),
            'MatchedJobs':("Idle", "EffIdle", "OldIdle", "Running", "RunningHere"),
            #'MatchedGlideins':("Total","Idle","Running","Failed","TotalCores","IdleCores","RunningCores"),
            'MatchedGlideins':("Total", "Idle", "Running", "Failed"),
            'MatchedCores':("Total", "Idle", "Running"),
            'Requested':("Idle", "MaxRun")
        }

        # set default values
        # user should modify if needed
        self.rrd_step=300       #default to 5 minutes
        self.rrd_heartbeat=1800 #default to 30 minutes, should be at least twice the loop time
        self.rrd_archives=[('AVERAGE', 0.8, 1, 740),      # max precision, keep 2.5 days
                           ('AVERAGE', 0.92, 12, 740),       # 1 h precision, keep for a month (30 days)
                           ('AVERAGE', 0.98, 144, 740)        # 12 hour precision, keep for a year
                           ]

        self.rrd_obj = rrdSupport.rrdSupport()

        self.updated = time.time()

        # only these will be states, all other names are assumed to be factories
        self.states_names=('Unmatched', 'MatchedUp', 'MatchedDown')

    def write_groupStats(self, total, factories_data, states_data, updated):
        self.updated = updated

        self.write_one_rrd("total", total)

        for fact in factories_data.keys():
            self.write_one_rrd("factory_%s" % sanitize(fact), factories_data[fact], 1)

        for fact in states_data.keys():
            self.write_one_rrd("state_%s" % sanitize(fact), states_data[fact], 1)

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
        for tp in self.attributes.keys():
            if tp in type_strings.keys():
                tp_str=type_strings[tp]
                attributes_tp=self.attributes[tp]
                for a in attributes_tp:
                    val_dict["%s%s"%(tp_str, a)]=None


        for tp in data:
            # type - Jobs,Slots
            if not (tp in self.attributes.keys()):
                continue
            if not (tp in type_strings.keys()):
                continue

            tp_str=type_strings[tp]

            attributes_tp=self.attributes[tp]

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

        for tp in ((".rrd", self.rrd_archives),):
            rrd_ext, rrd_archives = tp
            fname = os.path.join(self.monitor_dir, relative_fname + rrd_ext)
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
                    ds_arr.append((ds_name, ds_type, self.rrd_heartbeat, min_val, max_val))
                self.rrd_obj.create_rrd_multi(fname,
                                              self.rrd_step, rrd_archives,
                                              ds_arr)

            # print "Updating RRD "+fname
            try:
                self.rrd_obj.update_rrd_multi(fname, time, val_dict)
            except Exception as e:
                logSupport.log.error("Failed to update %s" % fname)
                # logSupport.log.exception(traceback.format_exc())
        return
