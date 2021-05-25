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
    pub host: String,

    /// Zookeeper cluster, comma-separated host:port pairs
    #[structopt(short = "z", long="zookeeper", default_value="localhost:2181")]
    zk: String,

    // /// Port for this nodes RPC interface to listen on
    #[structopt(short = "p", long="port", default_value="8080")]
    pub port: u16,

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
    let my_url = format!("http://{}:{}/myservice", options.host, options.port);
    let my_leadership_ops = ExampleClusterMember{ rest_endpoint: my_url };
    Cluster::new(&options.cluster_name, &options.zk, my_leadership_ops);

    for _i in 1..20 {
        //info!("main thread sleeping... {}",i);
        std::thread::sleep(Duration::from_secs(10));
    }

}

/// A silly implemenation of a cluster leader call back
pub struct ExampleClusterMember {

    /// In out case, the nodes communicate vai rest, this is the endpoint info
    rest_endpoint: String,

}

/// Implement the cluster
impl cluster::ClusterLocalNode for ExampleClusterMember {


    fn leader_cluster_changed<'a>(&mut self, n: u16) -> Vec<String> {
        // In this case, we pretend each node looks after 500k records each
        // simple example, but shows how it works
        let mut responsibilities:Vec<String> = Vec::with_capacity(n as usize);
        let mut range_lower = 0;
        for _s in 0..n {
            responsibilities.push( format!("{}-{}", range_lower, range_lower+500000-1) );
            range_lower += 500000;
        }
        responsibilities
    }

    fn member_contact_info(&self) -> &str {
        &self.rest_endpoint
    }


    fn member_responsibility(&self, resp: &str) {
        info!("My responsibiltiy has been set to [{}]", resp);
    }


}


