///! Module for defining the cluster operations, and detecting
///! if I am leader (or who is leader)

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::str::FromStr;
use zookeeper::{ZooKeeper, Watcher, WatchedEvent, CreateMode, Acl, ZkError, WatchedEventType};


// Callbacks from watches require static scope, we use this singleton to
// hold singleton in thread safe manner
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
    /// Leader publishes this, members read it
    member_config: HashMap<u32, (String, String)>,

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
    // NB - lifetime annotations - the returned &str should be one of the references
    // passed in (list of memers).  We help the compiler here by letting it know this.
    // (the responsibily is not passed in)
    /// Called on the leader -- a chance to setup the cluster and inform
    /// each client what they are responsible for
    /// When the cluster changes, we inform the leader.
    /// We provide the set of members, and their configuration key
    /// Returns list of the responsibilties - in order responsibility -> node_id (could be list)
    fn leader_cluster_changed<'a>(&mut self, members: u16) -> Vec<String>;

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

    fn create_peristance_node(zk: &ZooKeeper, path: &str) {
        let create_result = zk.create(path,  vec![], Acl::open_unsafe().clone(), CreateMode::Persistent);
        if let Err(x) = create_result {
            match x {
                ZkError::NodeExists => info!("Node {} already exists - no need to create", path),
                _ => panic!("Unhandled error"),   // TODO: we should be safe here and allow for re-try
            }
        } else {
            info!("Node {} did not already exist and created succesfully", path);
        }        
    }


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
        let path = format!("/{}", app_cluster_name);
        Self::create_peristance_node(&zk, &path);

        Self::create_peristance_node(&zk, &format!("{}/members", path));

        // Create my node -- then it should be visibale to others
        // NB error should not happen here - but shoud check
        let member_path = format!("{}/members/member_", path);
        
        // Get the contact data for this instance of a node
        let my_contact_info = leader_ops.member_contact_info();
        let vec = Vec::from(my_contact_info.as_bytes());

        let my_path = zk.create(&member_path,
            vec, 
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential);

        let cluster_member_paths = zk.get_children(&format!("{}/members", path), false).unwrap() ;

        // From the node, get the generated sequence
        if let Ok(s) = my_path {

            let mut c = Cluster {
                zk_path: path,
                zk: zk,
                client_id : Self::sequence_no(&s).unwrap(),
                leader: false,
                leader_ops: Box::new(leader_ops),
                member_config: HashMap::with_capacity(std::cmp::max(20, cluster_member_paths.len()))
            };
            
            // Leadership check etc
            info!("New-node - initial leadership check");
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
                    info!("Node {}, sequence is {:?} **** ME !!", c, x);
                } else if x < self.client_id {
                    info!("Node {}, sequence is {:?} ", c, x);

                    if let Some(pair) = my_watched {
                        if x > pair.0  {
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
            let watching_path = format!("{}/members/{}", self.zk_path, x.2);
            info!("I will be watching {} -- {}", x.0, watching_path);

            // FIXME: we've ignored error here
            let _x = self.zk.exists_w(&watching_path, Self::zz_watched_changed);
            
            if let Ok(data) = self.zk.get_data_w(&format!("{}/resps", self.zk_path), Self::zz_resps_changed) {
                let s = String::from_utf8(data.0).unwrap();
                info!("Member received this cluster data: {}", s);
            }

        } else {
            // No one found to watch, so we are the leader
            if self.leader == true {
                info!("I am already the leader");
            } else {
                info!("I am **newly promoted** to leader");
            }
            self.leader = true;

            // If we are newly promoted to leader, we need to get the member config

            // The cluster has changed, and this instance of the app is the leader.
            // We now build the map of node Id and their corresponding "info" ..
            // which allows us to contact them.
            // Filte matches nodes' files, not, e.g responsibilty file, 
            &cluster_member_paths.iter()
                //.filter(|p| { info!("Filter says [{}]", p); let x = p.starts_with("member_"); x} )
                .for_each(|p| {

                    let full_node_path = format!("{}/members/{}", self.zk_path, p);
                    let client_contact_info = self.zk.get_data(&full_node_path, false).map_or( None, |v| String::from_utf8(v.0).ok());
                    
                    let client_id = Self::sequence_no(p);
                    if let  (Some(info), Some(id) )= (client_contact_info, client_id) {
                        
                        // This doesn't work?
                        // a bit weird, I own it here (member_config)
                        match self.member_config.entry(id) {
                            Entry::Vacant(e) => {
                                info!("NEW member joined: {} @ {}", e.key(), info);
                                e.insert((info, String::new()) );
                            },
                            Entry::Occupied(e) => {
                                info!("Existing member ? {} @ {}", e.key(), info);
                            },
                        }

                    } else {
                        error!("Failed to get data? ");
                    }
                });

            // The leader always watches children, so we need to re-watch
            let _x = self.zk.get_children_w(&format!("{}/members", &self.zk_path) , Self::zz_children_changed) ;
                
            // call our custom call back to ensure its working
            info!("leadership_check: there are {} members", cluster_member_paths.len());
            let v = self.leader_ops.leader_cluster_changed(cluster_member_paths.len() as u16 );
            info!("No. of node responsibilities: {}", v.len());
            
            self.assign_node_resps(v);
        }
        
    }
    
    /// Assigns (and re-assigns) responsibilities to nodes.   Aim is to 
    /// keep responsibilties on their current nodes if need be
    fn assign_node_resps(&mut self, resps: Vec<String> ) {
        
        for (key, val) in &self.member_config {
            info!("{}  -->  {:?} ", key, val);
        }

        for resp in resps {
            // try and find in current list
            let node = self.member_config.iter_mut()
                .find( |(_k, v)| *v.1 == resp);
            
            if let Some(x) = node {
                info!("Responsibility {} already assigned to {:?}", resp, x);
            } else {
                // FIXME: logic does not work - test and see what happens when "gaps" appear
                // find a free node and set there
                if let Some(mut xx) = self.member_config.iter_mut().find( |(_k, v)| &*v.1 == "") {
                    // This looks horrid, we get tuple (key, value) using the map, but our
                    // valur is a tuple too!
                    (xx.1).1 = resp;
                    info!("Responsibility {:?} **newly** assigned", xx);
                }
            }
        }

        // Now we write the repsponsibility list out to the responsibility node so it's
        // visible to all members
        let resp_zk_path = format!("{}/resps", self.zk_path);

        if let Ok(opt) = self.zk.exists(&resp_zk_path, false) {
            // create string and vec it to write to 

            let mut node_data = String::new();
            for (k, v) in &self.member_config {
                node_data += &format!("{}\t{}\t{}\n", k, v.0, v.1);
            }

            info!("\n{}", node_data);
            let bytes = Vec::from(node_data.as_bytes());

            // FIXME: error handling
            // because it watches the entire path for members, if we put the cluster info here, 
            // it then causes the next watch to fire and so on and so forth!
            if let Some(node_data) = opt {
                info!("Creating resps node {}", &resp_zk_path);
                self.zk.set_data(&resp_zk_path, bytes, Some(node_data.version));
            } else {
                info!("Updating resps node {}", &resp_zk_path);
                self.zk.create(&resp_zk_path, bytes, Acl::open_unsafe().clone(), CreateMode::Ephemeral);
            }
            info!("Updated member data");
        }

    }


    /// We must be a member rather than leader, read latest responsibilities
    fn responsibilities_changed(&mut self) {

        // TODO: error handling
        if let Ok(data) = self.zk.get_data_w(&format!("{}/resps", self.zk_path), Self::zz_resps_changed) {
            let s = String::from_utf8(data.0).unwrap();
            info!("responsibilities_changed --member received this cluster data:\n{}", s);
        }


    }

    /// Called when the list of children has changed
    fn children_changed(&mut self) {

        //let children = self.zk.get_children_w(&self.zk_path , Self::zz_children_changed) ;
        // Don't set a watch, as don;t know whether we'll lead or not
        let children = self.zk.get_children(&format!("{}/members", &self.zk_path), false) ;

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
            Some(x) => x.children_changed(),
            _ => {},
        }
    }


    /// Called when all the children have changed
    fn zz_children_changed(event: WatchedEvent) {
        debug!("zz_children changed");

        match &event.event_type {
            WatchedEventType::NodeChildrenChanged => {
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


    /// Function called when we've detected change in responsibilties
    fn zz_resps_changed(event: WatchedEvent) {

        match &event.event_type {
            WatchedEventType::NodeDataChanged => {}
            _ => return,
        }

        // get ref to singleton
        let arc = Arc::clone(&CLUSTER);
        let mut opt = arc.lock().unwrap();
        
        let cluster = opt.as_mut();
        
        // we can get the path from the Cluster now
        match cluster {
            Some(x) => x.responsibilities_changed(),
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