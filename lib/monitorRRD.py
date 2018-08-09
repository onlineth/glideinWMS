import time
from glideinwms.frontend.glideinFrontendMonitoring import Monitoring_Output, sanitize
from glideinwms.lib import rrdSupport

class Monitoring_Output(object, Monitoring_Output):
    def __init__(self):
        # Grab the config obj
        super(Monitoring_Output, self).__init__()

        self.attributes = {
            'Jobs':("Idle", "OldIdle", "Running", "Total", "Idle_3600"),
            'Glideins':("Idle", "Running", "Total"),
            'MatchedJobs':("Idle", "EffIdle", "OldIdle", "Running", "RunningHere"),
            #'MatchedGlideins':("Total","Idle","Running","Failed","TotalCores","IdleCores","RunningCores"),
            'MatchedGlideins':("Total", "Idle", "Running", "Failed"),
            'MatchedCores':("Total", "Idle", "Running"),
            'Requested':("Idle", "MaxRun")
        }

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


    def write_groupStats(self, total, factories_data, states_data, updated):
        pass

    ###############################
    # PRIVATE - Used by write_file
    # Write one RRD
    def write_one_rrd(self,name,data,fact=0):
        global monitoringConfig

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

        self.monitoringConfig.establish_dir("%s"%name)
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