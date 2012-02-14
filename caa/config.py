DATASTORE = {'keyspace':            'caaTest', 
             'servers':             ['localhost:9160', ],
             'replication_factor':  1,
             'status_ttl':          2, # in seconds
}

CONTROLLER = {'epics_connection_timeout':   0.5, #in seconds
              'num_workers':                4,
              'num_timers':                 2,
}
