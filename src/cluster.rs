///! Module for defining the cluster operations, and detecting
///! if I am leader (or who is leader)


use std::sync::Arc;
use std::time::Duration;
use zookeeper::{ZooKeeper, Watcher, WatchedEvent, CreateMode, Acl, ZkError, WatchedEventType};

/// Refers to the state of an cluster member
pub enum ClusterStatus {

    /// Instance is attempting to join a cluster
    Joining,

    /// Instance is initializing, e.g. has received data from leader about it's partition / area of interest
    Initializing,

    /// Instance has initialized and is actively participating in cluster
    Active,

    /// Instance is suspect and should not be used
    Suspect
}

pub enum ClusterEvent {
    Joined,
    Leader,
    NewLeader
}

pub struct Cluster {

    /// Logical name of logical cluster - multiple app clusters can be supported by same ZK cluster
    logical_name: String,

    /// Zk instance used
    zk: Arc<ZooKeeper>,


}


/// Copied from example code, not sure of use as never seems to log anything!
struct LoggingWatcher;
impl Watcher for LoggingWatcher {

    fn handle(&self, e: WatchedEvent) {

        match e.event_type {
            WatchedEventType::None => info!("Session or client state changed {:?}", e.keeper_state),
            _ => debug!("event is related to a zknode")
        }
        info!("Watcher got an event {:?}", e)
    }
}


struct ChildWatch {
    zk: Arc<ZooKeeper>,
    path: String
}

impl ChildWatch {

    /// How to set self in context?? Use a closure
    fn list_children(children: Vec<String>) {
        info!("Listing children");
        for c in &children {
            info!("Child of {}",c);
        }
        // now need to re-watch
    }

    // Careful of the "move" here
    fn restart_watch_children(zk: Arc<ZooKeeper>, _event: WatchedEvent) {
        debug!("Restarting watch");
        let zk2 = zk.clone();
        let children = zk.get_children_w("/my_cool_cluster" ,  move |e: WatchedEvent| Self::restart_watch_children(zk2.clone(), e) );
        match children {
            Ok(v) => Self::list_children(v),
            Err(_) => error!("Failed to get children"),
        };
    }


    fn init_watch_children(&self) {
        let zk = self.zk.clone();
        let children = self.zk.get_children_w(self.path.as_str() ,  move |e: WatchedEvent| Self::restart_watch_children(zk.clone(), e) );
        match children {
            Ok(v) => Self::list_children(v),
            Err(_) => error!("Failed to get children"),
        };
    }


    fn new(path: &str, zk: Arc<ZooKeeper>) -> ChildWatch {
        ChildWatch{path: String::from(path), zk}
    }

}



impl Cluster {

    // TODO: configurable address, timeout option, application (logical cluster name)
    /// Creates a new instance, returns an Arc to it


    pub fn new(cluster: &str ) -> Cluster {
        debug!("Attemping to join cluster [{}]", &cluster);

        // TODO: we want to manage this as part of "Cluster" code - run asynchronously ?  Use calls
        let zk = ZooKeeper::connect("127.0.0.1:2181", Duration::from_millis(100), LoggingWatcher{} ).unwrap();
        zk.add_listener(|zk_state| info!("New ZkState is {:?}", zk_state));

        // Check for and create if missing the cluster node
        let mut path = String::new();
        path += "/";
        path += cluster;

        let create_result = zk.create(path.as_str(), vec![], Acl::open_unsafe().clone(), CreateMode::Persistent);
        if let Err(x) = create_result {
            match x {
                ZkError::NodeExists => info!("Node {} already exists - no need to create", path),
                _ => panic!("Unhandled error"),   // TODO: we should be safe here and allow for re-try
            }
        } else {
            info!("Node {} did not already exist and created succesfully", path);
        }        
        
        // emit_status(Joining)

        let zk = Arc::new(zk);
        // Self::restart_watch_children(None, zk.clone());

        // Ok, now we set 
        let cw =  ChildWatch::new(&path, zk.clone());
        cw.init_watch_children();


        // Create my node -- then it should be visibale to others
        // NB error should not happen here - but shoud check
        path += "/member_";
        let my_path = zk.create(path.as_str(),
            vec![], // try zero length vec of data .. i.e .. no data!
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential);

        // check if it is okay
        match my_path {
            Ok(s) => info!("This instances zknode is [{}]", s),
            Err(e) => error!("Failed to create my node {:?}", e),
        }

        Cluster {
            logical_name: String::from(cluster),
            zk: zk
        } 
        
    }


        // // we've got basic setup .. we can look at the node and get the children && watch it !!
    // // let children = zk.get_children_w("/roger", |evnt: WatchedEvent| {
    // // });

}