#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate lazy_static;
extern crate zookeeper;
extern crate structopt;

use std::time::Duration;

mod cluster;

use structopt::StructOpt;

use cluster::Cluster;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opts {

    /// (local) Hostname other cluster members will contact this instance on
    #[structopt(short = "h", long="host")]
    host: String,

    /// Zookeeper cluster, comma-separated host:port pairs
    #[structopt(short = "z", long="zookeeper", default_value="localhost:2181")]
    zk: String,

    // /// Port for this nodes RPC interface to listen on
    // #[structopt(short = "p", long="port", default_value="8080")]
    // port: u16,

    /// Cluster name to Join
    #[structopt(short = "c", long="cluster")]
    cluster_name: String,

}

fn main() {

    env_logger::Builder::from_env("APP_LOG").init();

    info!("My rust zookeeper client is starting");

    let options = Opts::from_args();
    info!("Starting with options: {:?}", options);


    // This does not return anything, .. 
    // TODO: consider builder pattern here / re-try if ZK unavailable etc etc
    let my_leadership_ops = Banana{};
    Cluster::new(&options.cluster_name, &options.zk, my_leadership_ops);

    for i in 1..20 {
        info!("main thread sleeping... {}",i);
        std::thread::sleep(Duration::from_secs(10));
    }

}

/// A silly implemenation of a cluster leader call back
pub struct Banana{}


impl cluster::ClusterLeader for Banana {


    fn cluster_changed(&mut self, members: &Vec<String>) {
        for s in members {
            info!("[Custom Callback] Member: {}", s);
        }
    }

}


