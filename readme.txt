Generic Application Cluster Module

-- Implemented using Zookeeper


1) Cluster has logical name -- e.g. "PartyDataGrid"
2) Cluster has a leader
3) Cluster application lifecycle

    App starts...
    
    Cluster attempts to join 

    If ok .. call back to app to initialise (Fn)
        .. if init ok .. status => Online, out of {Joining, Initializing, Online, Suspect}
    

4) "Leader" is a seperate object as far as code is concerned
    -- each instance will have "leader" code
    -- but leader calls to local no different to remote (think of how to do that)


    .. list children
        .. if I am leader, watch children
        .. if I am not leader watch my previous in succession list


e.g start app
    
    (... optionally do some stuff ...)

    Start cluster with callbacks
        Cluster -- status to Joining
            .. create cluster path if not exists .. easy peasy    







