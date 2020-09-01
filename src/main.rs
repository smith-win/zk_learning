#[macro_use]
extern crate log;
extern crate env_logger;

extern crate zookeeper;

use zookeeper::{ZooKeeper, Watcher, WatchedEvent, CreateMode, Acl, ZkError};
use zookeeper::WatchedEventType;
use std::time::Duration;

mod cluster;


// We should have some form of status here
struct LoggingWatcher;
impl Watcher for LoggingWatcher {

    fn handle(&self, e: WatchedEvent) {

        match e.event_type {
            WatchedEventType::None => info!("Session or client state changed {:?}", e.keeper_state),
            _ => debug!("event is related to a zknode")
        }

        info!("*** Big banana *** {:?}", e)
    }
}

/// Simple listener function
fn list_children(e: WatchedEvent) {
    info!("Children changed {:?}", e.path);

    // now I want to list the other members

}

fn main() {

    env_logger::Builder::from_env("APP_LOG").init();

    info!("My rust zookeeper client is starting");


    // attempt to connect to Zookeeper cluster, localhosr 2181
    debug!("Attempting new session");

    // Rule #1 no unwraps :-) .. so
    let zk = ZooKeeper::connect("127.0.0.1:2181", Duration::from_millis(100), LoggingWatcher{} ).unwrap();

    zk.add_listener(|zk_state| info!("New ZkState is {:?}", zk_state));

    // Create the initial node -- lets call it roger

    let create_result = zk.create("/roger", vec![], Acl::open_unsafe().clone(), CreateMode::Persistent);

    if let Err(x) = create_result {
        match x {
            ZkError::NodeExists => info!("Node /roger already exists - no drama"),
            _ => panic!("Unhandled error"),
        }
    } else {
        info!("Node roger did not already exist and created");
    }

    // NB error should not happen here - but shoud check
    let path = zk.create("/roger/party_cluster",
        vec![], // try zero length vec of data .. i.e .. no data!
        Acl::open_unsafe().clone(),
        CreateMode::EphemeralSequential);

    // check if it is okay
    match path {
        Ok(s) => info!("This instances zknode is [{}]", s),
        Err(e) => error!("Failed to create my node {:?}", e),
    }


    // we've got basic setup .. we can look at the node and get the children && watch it !!
    // let children = zk.get_children_w("/roger", |evnt: WatchedEvent| {
    //     info!("Children changed {:?}", evnt.path);
    // });

    // again, but we'll pass the children to it
    let children = zk.get_children_w("/roger", list_children);

    
    match children {
        Ok (v) => v.iter().for_each( |s| info!("Child node (not watched): {:?}", s) ),
        Err( _ ) => error!("Failed to list childrens"),
    }

    for i in 1..20 {
        info!("main thread sleeping... {}",i);
        std::thread::sleep(Duration::from_secs(10));
    }

}
