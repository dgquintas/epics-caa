import pycassa

DATASTORE = {'keyspace':            'caaTest', 
             'servers':             ['localhost:9160', ],
             'replication_factor':  1,
             'consistency':         {'read': pycassa.ConsistencyLevel.QUORUM,
                                     'write':pycassa.ConsistencyLevel.ONE},
             'status_ttl':          2, # in seconds
}

CONTROLLER = {'epics_connection_timeout':   0.1, #in seconds
              'num_workers':                8,
              'num_timers':                 4,
}

ARCHIVER = {'pvfields': 
                 {'data': ['value', 'count', 'type', 'units', 'precision'],
                  'time': ['timestamp', 'archived_at', 'archived_at_ts'],
                  'alarms': ['severity', 'upper_disp_limit', 
                             'lower_disp_limit', 'upper_alarm_limit', 
                             'lower_alarm_limit','upper_warning_limit', 
                             'lower_warning_limit', 'upper_ctrl_limit', 
                             'lower_ctrl_limit'],
                  'epics': ['pvname', 'status', 'host', 'access']}
}

WEBCLIENT = {'default_table_fields': ['timestamp', 'archived_at',  'value', 'status', 'type'], }
