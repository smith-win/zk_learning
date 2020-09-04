///! Module for defining the cluster operations, and detecting
///! if I am leader (or who is leader)


use std::time::Duration;
use std::sync::Arc;
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

pub struct Cluster {

    /// Zookeeper path for the node for our cluster
    zk_path: String,

    /// Zk instance used
    zk: Arc<ZooKeeper>,

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


/// Just messing around with ZK impl and its need for static lifetime
impl LoggingWatcher {

    fn cluster_member_node(_e: WatchedEvent) {
        println!("All hell breaks loose");
    }


}


impl Cluster {

    // TODO: configurable address, timeout option, application (logical cluster name)
    /// Creates a new instance, returns an Arc to it


    pub fn new(cluster: &str ) -> Cluster {
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
        
        // emit_status(Joining)

        let zk = Arc::new(zk);
        // Self::restart_watch_children(None, zk.clone());
        // Ok, now we set 

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
            c.init_watch_children();
            c
        } else {
            panic!();
        }
        
    }


    /// How to set self in context?? Use a closure
    fn list_children(children: Vec<String>) {
        info!("Listing children");
        for c in &children {
            info!("Child of {}, sequence is {:?} ",c, Self::sequence_no(c));
        }
        // now need to re-watch
    }

    // Careful of the "move" here
    fn restart_watch_children(zk: Arc<ZooKeeper>, event: WatchedEvent) {
        debug!("{:?} Restarting watch", std::thread::current().name());

        match event.event_type {
            WatchedEventType::NodeChildrenChanged => debug!("Children of membership/leader-election node changed"),
            _ => debug!("Other event type"),
        }

        let zk2 = zk.clone();
        let children = zk.get_children_w("/my_cool_cluster" , LoggingWatcher::cluster_member_node) ; // move |e: WatchedEvent| Self::restart_watch_children(zk2.clone(), e) );
        match children {
            Ok(v) => Self::list_children(v),
            Err( x ) => error!("Failed to get children of  {:?}", x),
        };
    }

    // TODO: like the above example .. check 
    fn init_watch_children(&self) {
        let zk = self.zk.clone();
        let children = self.zk.get_children_w(&self.zk_path,  move |e: WatchedEvent| Self::restart_watch_children(zk.clone(), e) );
        // let children = self.zk.get_children(&self.zk_path,  true );
        match children {
            Ok(v) => Self::list_children(v),
            Err( x ) => error!("Failed to get children of {}  {:?}", self.zk_path, x),
        };
    }
    

    ///
    fn sequence_no(s: &str) -> Option<u32> {
        Self::sequence_no_2(s)
        // if let Some(idx) = s.rfind('_') {
        //     match u32::from_str(s.split_at(idx+1).1) {
        //         Ok(i) => Some(i),
        //         Err(x) => { error!("Failed to get sequence {:?}", x); None}
        //     }
        // } else {
        //     None
        // }
    }

    // Example of above code re-written using combinator functions on Option and error!
    // A good learning example I think
    fn sequence_no_2(s: &str) -> Option<u32> {
        s.rfind('_')
            .map( |idx| s.split_at(idx+1).1)
            .map( |substr| u32::from_str(substr).ok() )
            .flatten()
    }

        // // we've got basic setup .. we can look at the node and get the children && watch it !!
    // // let children = zk.get_children_w("/roger", |evnt: WatchedEvent| {
    // // });

}