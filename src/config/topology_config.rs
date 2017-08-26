//! An abstract for the configuration of the whole topology.
//!
//! Starting from a config file and the zookeeper address (specified by the user
//! when submitting a new topology), `TopologyConfig::from_config_file` generates
//! a representation of the topology configuration to be passed to different
//! components down the line.

use std::io::prelude::*;
use std::fs::File;
use rustc_serialize::json;

#[derive(RustcDecodable, RustcEncodable)]
#[derive(Debug, Clone)]
pub enum Component{
    Spout,
    Topology
}

#[derive(Debug)]
#[derive(RustcDecodable, RustcEncodable)]
pub struct TopologyConfigItem {
    pub item_type: Component,
    pub name: String,
    pub module: String,
    pub instance_count: i32,
    pub input_stream: Option<String>
}

#[derive(Debug)]
#[derive(RustcDecodable, RustcEncodable)]
pub struct TopologyConfig{
    pub name: String,
    pub sm_count: i32,
    pub instances_per_sm: i32,
    pub topology: Vec<TopologyConfigItem>,
    pub topology_dir: Option<String>,
    pub topology_archive: Option<String>,
    pub zookeeper: Option<String>,
    pub zk_node: Option<String>
}

impl TopologyConfig{
	pub fn from_config_file(config_file: String,
                            zookeeper: String) -> TopologyConfig{
        let mut file = File::open(config_file).unwrap();
        let mut config = String::new();
        let _ = file.read_to_string(&mut config);
        let mut config: TopologyConfig = json::decode(&config).unwrap();
        config.zookeeper = Some(zookeeper);
        config
	}
}