#[macro_use]
extern crate log;
extern crate env_logger;

extern crate zookeeper;

use std::time::Duration;

mod cluster;

use cluster::Cluster;


fn main() {

    env_logger::Builder::from_env("APP_LOG").init();

    info!("My rust zookeeper client is starting");


    let cluster = Cluster::new("my_cool_cluster");

    info!("I joined cluster with id {}", cluster.client_id);


    for i in 1..20 {
        info!("main thread sleeping... {}",i);
        std::thread::sleep(Duration::from_secs(10));
    }

}
