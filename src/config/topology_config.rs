//! An abstract for the configuration of the whole topology.
//!
//! Starting from a config file and the zookeeper address (specified by the user
//! when submitting a new topology), `TopologyConfig::from_config_file` generates
//! a representation of the topology configuration to be passed to different
//! components down the line.

use std::io::prelude::*;
use std::fs::File;
use rustc_serialize::json;
use utils::storage::Storage;
use std::fs::{self, DirBuilder};
use std::process::Command;

#[derive(RustcDecodable, RustcEncodable)]
#[derive(Debug, Clone)]
pub enum Component{
    Spout,
    Bolt
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
}

impl TopologyConfig{
    fn from_config_file(config_file: String, dir: String) -> TopologyConfig{
        let mut file = File::open(config_file).unwrap();
        let mut config = String::new();
        let _ = file.read_to_string(&mut config);
        let mut config: TopologyConfig = json::decode(&config).unwrap();
        config.topology_dir = Some(dir);
        config
    }

    pub fn from_archive(topology_archive: &str,
                        storage: Storage) -> Self{
        let dir_path = "/tmp/antimony/"; // config file
        let _ = DirBuilder::new().create(dir_path);
        let dir_path = format!("/tmp/antimony/{}", storage.topology_name);
        match DirBuilder::new().create(&dir_path){
            Ok(_) => {},
            Err(e) => panic!("{}", e)
        };
        let output = Command::new("unzip")
            .arg(&topology_archive)
            .arg("-d")
            .arg(&dir_path).output()
            .expect("Couldn't unzip archive");
        if !output.stderr.is_empty(){
            panic!("Couldn't unzip: {:?}", String::from_utf8(output.stderr).unwrap());
        }
        let topology_dir = match fs::read_dir(&dir_path){
            Ok(paths) => {
                let paths: Vec<String> = paths.map(|p|{p.unwrap().path().to_str().unwrap().to_string()}).collect();
                if paths.len() != 1{
                    panic!("Found more than one dir in: {}", dir_path);
                }else{
                    paths[0].clone()
                }
            },
            Err(e) => {
                panic!("{}", e);
            }
        };
        let config = TopologyConfig::from_config_file(
                                format!("{}/topology.json", topology_dir), topology_dir);
        storage.write_config(&config);
        config
    }

    pub fn from_storage(storage: &Storage) -> Self{
        let config = storage.get_config();
        let config: TopologyConfig = json::decode(&config).unwrap();
        config
    }
}