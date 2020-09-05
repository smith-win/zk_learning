#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate lazy_static;
extern crate zookeeper;

use std::time::Duration;

mod cluster;

use cluster::Cluster;


fn main() {

    env_logger::Builder::from_env("APP_LOG").init();

    info!("My rust zookeeper client is starting");


    // This does not return anything, .. 
    // TODO: consider builder pattern here / re-try if ZK unavailable etc etc
    Cluster::new("my_cool_cluster");

    for i in 1..20 {
        info!("main thread sleeping... {}",i);
        std::thread::sleep(Duration::from_secs(10));
    }

}
