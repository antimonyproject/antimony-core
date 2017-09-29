use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper};
use std::time::Duration;
use config::topology_config::TopologyConfig;
use rustc_serialize::json;
use std::collections::HashMap;


struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, _: WatchedEvent){}
}

#[derive(Clone)]
pub enum StorageType{
    Zookeeper
}

pub struct Storage{
    zk_connection: Option<ZooKeeper>,
    pub topology_name: String,
    config: (StorageType, String, String)
}

impl Storage{
    pub fn new(storage_type: StorageType, address: &str, topology_name: &str) -> Self{
        match storage_type{
            StorageType::Zookeeper => {
                let zk = ZooKeeper::connect(address, Duration::from_secs(5), LoggingWatcher).unwrap();
                Storage::init_zk(&zk, topology_name);
                Storage{
                    zk_connection: Some(zk),
                    topology_name: topology_name.to_string(),
                    config: (storage_type, address.to_owned(), topology_name.to_owned())
                }
            },
            _ => {panic!("")}
        }
    }

    pub fn cloned(&self) -> Self{
        Storage::new(self.config.0.clone(), &self.config.1, &self.config.2)
    }

    fn init_zk(zk: &ZooKeeper, topology_name: &str){
        let paths = vec!["/antimony".to_string(),
                        format!("/antimony/{}", topology_name),
                        format!("/antimony/{}/config", topology_name),
                        format!("/antimony/{}/SMs", topology_name)];
        for node in paths{
            if !zk.exists(&node, false).unwrap().is_some(){
                let _ = zk.create(&node,
                    vec![],
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent);
            }
        }
    }

    pub fn write_config(self, config: &TopologyConfig){
        if self.zk_connection.is_some(){
            let _ = self.zk_connection.unwrap().set_data(
                &format!("/antimony/{}/config", self.topology_name)[..],
                json::encode(config).unwrap().into_bytes(),
                None
            );
        }
    }

    pub fn get_config(&self) -> String{
        let mut config = String::new();
        if self.zk_connection.is_some(){
            let data = self.zk_connection.as_ref().unwrap().get_data(
                &format!("/antimony/{}/config", self.topology_name)[..],
                true
            );
            config = String::from_utf8(data.unwrap().0).unwrap();
        }
        config
    }

    pub fn get_tm_addr(&self) -> String{
        let mut tm_addr = String::new();
        if self.zk_connection.is_some(){
            let data = self.zk_connection.as_ref().unwrap().get_data(
                &format!("/antimony/{}/tm", self.topology_name)[..],
                true
            );
            tm_addr = String::from_utf8(data.unwrap().0).unwrap();
        }
        tm_addr
    }

    pub fn register_tm(&self, tm_addr: String){
        if self.zk_connection.as_ref().is_some(){
            let node = format!("/antimony/{}/tm", self.topology_name);
            let _ = self.zk_connection.as_ref().unwrap().create(&node,
                    vec![],
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent);
            let _ = self.zk_connection.as_ref().unwrap().set_data(
                &node,
                tm_addr.into_bytes(),
                None
            );
        }
    }

    pub fn register_sm(&self, sm_addr: String, sm_id: usize){
        if self.zk_connection.as_ref().is_some(){
            let node = format!("/antimony/{}/SMs/{}", self.topology_name, sm_id);
            let _ = self.zk_connection.as_ref().unwrap().create(&node,
                    sm_addr.into_bytes(),
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent);
        }
    }

    pub fn list_managers(&self) -> HashMap<String, String>{
        let mut started_managers = HashMap::new();
        if self.zk_connection.as_ref().is_some(){
            let node = format!("/antimony/{}/SMs", self.topology_name);
            let children = self.zk_connection.as_ref().unwrap().get_children(&node, true);
            for sm in children.unwrap(){
                let sm_node = format!("/antimony/{}/SMs/{}", self.topology_name, sm);
                let data = self.zk_connection.as_ref().unwrap().get_data(
                    &sm_node,
                    true
                );
                started_managers.insert(sm, String::from_utf8(data.unwrap().0).unwrap());
            }
        }
        started_managers
    }
}