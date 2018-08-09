from glideinwms.frontend.glideinFrontendMonitoring import Monitoring_Output, sanitize

class Monitoring_Output(Monitoring_Output):
    def __init__(self):
        self.attributes = {
            'Jobs': ("Idle", "OldIdle", "Running", "Total", "Idle_3600"),
            'Glideins': ("Idle", "Running", "Total"),
            'MatchedJobs': ("Idle", "EffIdle", "OldIdle", "Running", "RunningHere"),
            # 'MatchedGlideins':("Total","Idle","Running","Failed","TotalCores","IdleCores","RunningCores"),
            'MatchedGlideins': ("Total", "Idle", "Running", "Failed"),
            'MatchedCores': ("Total", "Idle", "Running"),
            'Requested': ("Idle", "MaxRun")
        }

    def write_groupStats(self, total, factories_data, states_data, updated):
        self.updated = updated

        self.write_one_rrd("total", total)

        for fact in factories_data.keys():
            self.write_one_rrd("factory_%s"%sanitize(fact), factories_data[fact], 1)

        for fact in states_data.keys():
            self.write_one_rrd("state_%s"%sanitize(fact), states_data[fact], 1)

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

        monitoringConfig.establish_dir("%s"%name)
        monitoringConfig.write_rrd_multi("%s/Status_Attributes"%name,
                                         "GAUGE", self.updated, val_dict)
