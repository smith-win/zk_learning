#[macro_use]
extern crate log;
extern crate env_logger;

extern crate zookeeper;

use zookeeper::{Watcher, WatchedEvent};
use zookeeper::WatchedEventType;
use std::time::Duration;

mod cluster;

use cluster::Cluster;

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


fn main() {

    env_logger::Builder::from_env("APP_LOG").init();

    info!("My rust zookeeper client is starting");




    // // NB error should not happen here - but shoud check
    // let path = zk.create("/roger/party_cluster",
    //     vec![], // try zero length vec of data .. i.e .. no data!
    //     Acl::open_unsafe().clone(),
    //     CreateMode::EphemeralSequential);

    // // check if it is okay
    // match path {
    //     Ok(s) => info!("This instances zknode is [{}]", s),
    //     Err(e) => error!("Failed to create my node {:?}", e),
    // }


    // // we've got basic setup .. we can look at the node and get the children && watch it !!
    // // let children = zk.get_children_w("/roger", |evnt: WatchedEvent| {
    // //     info!("Children changed {:?}", evnt.path);
    // // });

    // // again, but we'll pass the children to it
    // let children = zk.get_children_w("/roger", list_children);

    
    // match children {
    //     Ok (v) => v.iter().for_each( |s| info!("Child node (not watched): {:?}", s) ),
    //     Err( _ ) => error!("Failed to list childrens"),
    // }

    let _cluster = Cluster::new("my_cool_cluster");

    for i in 1..20 {
        info!("main thread sleeping... {}",i);
        std::thread::sleep(Duration::from_secs(10));
    }

}
