Generic Application Cluster Module

-- Implemented using Zookeeper


My TODO:

* [DONE!] Only the leader watches the entire directory (stop herd effect)
    -- already done, but all nodes scan initally on startup to check if they should be leader

* Create some configuration info for a node, so each node knows what its responsibility instance
    -- Need a leader function that provides existing partition data .. then leader needs to
        -- work out what to reation A-G, H-N, O-U, V-Z   7,7,7,5 ...

* Share nodes contact details (e.g URL / host+port) - so they can be contacted
    [DONE!] -- the leader knows
    -- share with others, like a map of responsibility -> contact_info
    -- leader can write data to the cluster node?

* Share the partition info across the cluster members 
    -- if call comes through Load balancer at node A . it calls nodeA, node B and node Cluster
    -- e.g we partition by date and user requests LATEST, 

* A general tidy up, especially of stateless calls.
* Include some error tolerance .. and see below on losing contact with cluster etc

* When lose contact with ZK cluster -- or start without ZK cluster being up
    -- Node becomes "stranded" .. so should give up being leader, and drop any responsibility

* Move test code to "example" .. follow Rust practices, this project becomes a lib


--- Original Notes

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

## Scaling disk access 
https://www.gridgain.com/resources/blog/how-boost-and-scale-postgresql-shared-buffers-in-memory-data-grids


## on my machine I can run Zookeeper by using Podman

# Standard port mapping
podman run -t -i -p 2181:2181 zookeeper

# Through port 8080 .. 
podman run -t -i -p 8080:2181 zookeeper


# Then run with this command
APP_LOG=debug cargo run --rel
ease -- --cluster myapp --host $(hostname) --port 8081



17May2021

To execute: 
$ APP_LOG=debug cargo run -- --host 127.0.0.1 -p 8091 --cluster gcrs_limit 


## Leader watches nodes
-- nodes watch their own "partition" node only (leader writes into it)