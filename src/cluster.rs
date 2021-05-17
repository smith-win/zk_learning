///! Module for defining the cluster operations, and detecting
///! if I am leader (or who is leader)

use std::collections::HashMap;
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

    /// id of myself in the cluster
    pub client_id: u32,

    /// indicates if this process is the leader
    leader: bool,

    /// Maps the client id to (node_info, node_responsibility)
    /// Only the leader needs this
    member_config: Option<HashMap<u32, (String, String)>>,

    /// Call back to custom leader operations, boxed as we don't know
    /// type until runtime
    leader_ops : Box<dyn ClusterLocalNode>

}

/// Defines the operations that enable a node to participate in the cluster
/// e.g send responsibilities or instructions to each cluster member.
/// It must be "Send" because we can call it on any thread - hence we can use in the static.
/// Leader operations start iwth prefix "leader_".
/// Member  operations start with prefix "member_"
/// A node can be both leader and member
pub trait ClusterLocalNode: Send {

    /// Each node in a cluster can have a data type associated with it.
    /// This tells the Node what they are responsible for
    // type NodeData;

    // TODO: we could provide added & removed lists ?
    /// Called on the leader -- a chance to setup the cluster and inform
    /// each client what they are responsible for
    /// When the cluster changes, we inform the leader.
    /// We provide the set of members, and their configuration key
    // TODO: add return method here -- Map<String> ?  So we can get the responsibilities
    fn leader_cluster_changed(&mut self, members: &Vec<String>);

    /// Called on members -- they can provide information about themselves
    /// typically how thay can be contacted (e.g base URI of service they expose)
    fn member_contact_info(&self) -> &str;

    /// Called to set the responsibility on the local node
    fn member_responsibility(&self, resp: &str);

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

    // Simple case would be to have a loop which tries every 10s or 
    // so for a ZK cluster to be running

    // TODO: configurable address, timeout option, application (logical cluster name)
    // TODO: should get a result
    /// Creates a new instance, returns an Arc to it - so it can be used
    pub fn new<T: ClusterLocalNode + 'static>(app_cluster_name: &str, zk_cluster: &str, leader_ops: T ) {
        debug!("{:?} Attemping to join cluster [{}]",std::thread::current().name(), &app_cluster_name);

        // TODO: we want to manage this as part of "Cluster" code - run asynchronously ?  Use calls
        let zk = ZooKeeper::connect(zk_cluster, Duration::from_millis(100), LoggingWatcher{} ).unwrap();
        zk.add_listener(|zk_state| info!("New ZkState is {:?}", zk_state));

        // Check for and create if missing the cluster node
        let mut path = String::new();
        path += "/";
        path += app_cluster_name;

        let create_result = zk.create(path.as_str(),  vec![], Acl::open_unsafe().clone(), CreateMode::Persistent);
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

        // Get the contact data for this instance of a node
        let my_contact_info = leader_ops.member_contact_info();
        let vec = Vec::from(my_contact_info.as_bytes());

        let my_path = zk.create(&member_path,
            vec, 
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential);

        let cluster_member_paths = zk.get_children(&path, false).unwrap() ;

        // From the node, get the generated sequence
        if let Ok(s) = my_path {

            let mut c = Cluster {
                zk_path: path,
                zk: zk,
                client_id : Self::sequence_no(&s).unwrap(),
                leader: false,
                leader_ops: Box::new(leader_ops),
                member_config: None
            };
            
            // Leadership check etc
            c.leadership_check(cluster_member_paths);
            let arc = Arc::clone(&CLUSTER);
            let mut mg = arc.lock().unwrap();
            mg.replace(c);

        } else {
            panic!("Failed to initialize Zookeeper client");
        }
        
    }

    /// How to set self in context?? Use a closure
    fn leadership_check(&mut self, cluster_member_paths: Vec<String>) {
        info!("leadership_check: listing members");

        // This is the sequence number of the node we are watching

        // can use enumerate to get index of minimum ?

        // index in array, the sequence number and the path
        // TODO: is this tuple-tripe useful?
        let mut my_watched : Option<(u32, usize, &String)> = None;


        // TODO: can use "fold" here
        for (idx, c) in cluster_member_paths.iter().enumerate() {
            let seq = Self::sequence_no(c);
            if let Some(x) = seq {
                
                if x == self.client_id {
                    info!("Child of {}, sequence is {:?} **** ME !!", c, x);
                } else if x < self.client_id {
                    info!("Child of {}, sequence is {:?} ", c, x);

                    // my_watched.replace(std::cmp::max(my_watched.unwrap_or((x), x));
                    if let Some(pair) = my_watched {
                        if dbg!(x > pair.0) {
                            my_watched.replace( (x, idx, c));
                        }
                    } else {
                        my_watched.replace( (x, idx, c));
                    }

                } else {
                    info!("Child of {}, sequence is {:?} ", c, x);
                }

            }
        }
        
        // now need to re-watch
        if let Some(x) = my_watched {
            self.leader = false;
            let watching_path = format!("{}/{}", self.zk_path, x.2);
            info!("I will be watching {} -- {}", x.0, watching_path);
            let _x = self.zk.exists_w(&watching_path, Self::zz_watched_changed);
        } else {
            // No one found to watch, so we are the leader
            self.leader = true;

            // If we are newly promoted to leader, we need to get the member config
            if self.member_config.is_none() {
                info!("I have just been promoted to cluster leader");
                self.member_config.replace( HashMap::with_capacity(std::cmp::max(20, cluster_member_paths.len())) );
            } else {
                info!("I am already the leader");
            }

            // The cluster has changed, and this instance of the app is the leader.
            // We now build the map of node Id and their corresponding "info" ..
            // which allows us to contact them.
            // TODO: Once this map is built, we need to calculate the reponsibilities.
            &cluster_member_paths.iter()
                .for_each(|p| {

                    let full_node_path = format!("{}/{}", self.zk_path, p);
                    let client_contact_info = self.zk.get_data(&full_node_path, false).map_or( None, |v| String::from_utf8(v.0).ok());
                    
                    let client_id = Self::sequence_no(p);
                    if let  (Some(info), Some(id) )= (client_contact_info, client_id) {
                        
                        // a bit weird, I own it here (member_config)
                        match self.member_config.as_mut().unwrap().entry(id) {
                            std::collections::hash_map::Entry::Vacant(e) => {
                                info!("NEW member joined: {} @ {}", e.key(), info);
                                e.insert((info, String::new()) );
                            },
                            _ => {},
                        }

                    } else {
                        error!("Failed to get data? ");
                    }
                });

            // The leader always watches children, so we need to re-watch
            let _x = self.zk.get_children_w(&self.zk_path , Self::zz_children_changed) ;
        }

        // call our custom call back to ensure its working
        self.leader_ops.leader_cluster_changed(&cluster_member_paths);
        
    }
    

    // TODO: need some result to go on
    /// Writes member config to the node, all members can then pick up
    /// the partition data
    fn write_member_data(&mut self) {

        // build a simple string of the member configs so all can provide it.

        let full_node_path = format!("{}/{}", self.zk_path, "member_data");
        let client_contact_info = self.zk.get_data(&full_node_path, false).map_or( None, |v| String::from_utf8(v.0).ok());
    }



    /// Called when the list of children has changed
    fn children_changed(&mut self) {

        //
        let children = self.zk.get_children_w(&self.zk_path , Self::zz_children_changed) ;

        match children {
            Ok(v) => self.leadership_check(v),
            Err( x ) => error!("Failed to get children of  {:?}", x),
        };

    }


    // "static" func to check leadership / children 
    fn zz_check_leadership() {
        // get ref to singleton
        let arc = Arc::clone(&CLUSTER);
        let mut opt = arc.lock().unwrap();
        
        let cluster = opt.as_mut();
        
        // we can get the path from the Cluster now
        match cluster {
            Some(x) =>  x.children_changed(),
            _ => {},
        }
    }


    /// Called when all the children hav changed
    fn zz_children_changed(event: WatchedEvent) {
        debug!("zz_children changed");

        match &event.event_type {
            WatchedEventType::NodeChildrenChanged => {
                info!("Children have changed");
                Self::zz_check_leadership();
            },
            _ => {},
        }
        
    }
    

    /// Called when the watched child is deleted .. we have to then
    /// check for leader status and watch potential leader
    fn zz_watched_changed(event: WatchedEvent) {
        match &event.event_type {
            WatchedEventType::NodeDeleted => {
                info!("Watched node has been deleted!");
                Self::zz_check_leadership();
            }
            _ => {},
        }
    }


    /// Looks a the name of a node and returns the sequence number from it.
    /// The sequence number is used in leadership election.
    fn sequence_no(node_name: &str) -> Option<u32> {
        // Example of above code re-written using combinator functions on Option and error!
        // A good learning example I think - check number of lines of code with below
        node_name.rfind('_')
            .map( |idx| node_name.split_at(idx+1).1)
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