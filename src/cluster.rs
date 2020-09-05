///! Module for defining the cluster operations, and detecting
///! if I am leader (or who is leader)


use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::str::FromStr;
use zookeeper::{ZooKeeper, Watcher, WatchedEvent, CreateMode, Acl, ZkError, WatchedEventType};

// /// Refers to the state of an cluster member
// pub enum ClusterStatus {

//     /// Instance is attempting to join a cluster
//     Joining,

//     /// Instance is initializing, e.g. has received data from leader about it's partition / area of interest
//     Initializing,

//     /// Instance has initialized and is actively participating in cluster
//     Active,

//     /// Instance is suspect and should not be used
//     Suspect
// }

// pub enum ClusterEvent {
//     Joined,
//     Leader,
//     NewLeader
// }

// Callbacks from watches require static scope, we use this singleton to
// hold a singleton
lazy_static! {
    static ref CLUSTER : Arc<Mutex<Option<Cluster>>> = Arc::new(Mutex::new(None));
}

pub struct Cluster {

    /// Zookeeper path for the node for our cluster
    zk_path: String,

    /// Zk instance used
    zk: ZooKeeper,

    pub client_id: u32

}


/// Copied from example code, not sure of use as never seems to log anything!
struct LoggingWatcher;
impl Watcher for LoggingWatcher {

    fn handle(&self, e: WatchedEvent) {

        match e.event_type {
            WatchedEventType::None => info!("Session or client state changed {:?}", e.keeper_state),
            _ => debug!("event is related to a zknode")
        }
        info!("*** General Watcher got an event {:?}", e)
    }
}


impl Cluster {

    // TODO: configurable address, timeout option, application (logical cluster name)
    // TODO: should get a result
    /// Creates a new instance, returns an Arc to it - so it can be used
    pub fn new(cluster: &str ) {
        debug!("{:?} Attemping to join cluster [{}]",std::thread::current().name(), &cluster);

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
        

        // Create my node -- then it should be visibale to others
        // NB error should not happen here - but shoud check
        let mut member_path = path.clone();
        member_path += "/member_";
        let my_path = zk.create(&member_path,
            vec![], // try zero length vec of data .. i.e .. no data!
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential);

        // From the node, get the generated sequence
        if let Ok(s) = my_path {
            let c = Cluster {
                zk_path: path,
                zk: zk,
                client_id : Self::sequence_no(&s).unwrap()
            };

            c.children_changed();
            let arc = Arc::clone(&CLUSTER);
            let mut mg = arc.lock().unwrap();
            mg.replace(c);

            

        } else {
            panic!("Failed to initialize Zookeeper client");
        }
        
    }


    /// How to set self in context?? Use a closure
    fn list_children(&self, children: Vec<String>) {
        info!("Listing children");
        for c in &children {
            let seq = Self::sequence_no(c);
            if let Some(x) = seq {
                if x == self.client_id {
                    info!("Child of {}, sequence is {:?} **** ME !!", c, x);
                } else {
                    info!("Child of {}, sequence is {:?} ", c, x);
                }

            }
        }
        // now need to re-watch
    }


    fn children_changed(&self) {
        let children = self.zk.get_children_w(&self.zk_path , Self::zz_children_changed) ;

        match children {
            Ok(v) => self.list_children(v),
            Err( x ) => error!("Failed to get children of  {:?}", x),
        };

    }



    ///
    fn zz_children_changed(event: WatchedEvent) {

        match &event.event_type {
            WatchedEventType::NodeChildrenChanged => info!("Children has changed"),
            _ => {},
        }


        debug!("zz_children changed");

        // get ref to singleton
        let arc = Arc::clone(&CLUSTER);
        let opt = arc.lock().unwrap();
        let cluster = opt.as_ref().unwrap(); // TODO: check for singleton
        

        // we can get the path from the Cluster now
        cluster.children_changed();
        
    }
    

    ///
    fn sequence_no(s: &str) -> Option<u32> {
        // Example of above code re-written using combinator functions on Option and error!
        // A good learning example I think - check number of lines of code with below
        s.rfind('_')
            .map( |idx| s.split_at(idx+1).1)
            .map( |substr| u32::from_str(substr).ok() )
            .flatten()
        // if let Some(idx) = s.rfind('_') {
        //     match u32::from_str(s.split_at(idx+1).1) {
        //         Ok(i) => Some(i),
        //         Err(x) => { error!("Failed to get sequence {:?}", x); None}
        //     }
        // } else {
        //     None
        // }
    }



}