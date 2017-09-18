use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper};
use std::time::Duration;
use config::topology_config::TopologyConfig;
use rustc_serialize::json;


struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent){}
}

pub enum Storage_type{
	Zookeeper
}

pub struct Storage{
	zk_connection: Option<ZooKeeper>,
	pub topology_name: String
}

impl Storage{
	pub fn new(storage_type: Storage_type, address: &str, topology_name: &str) -> Self{
		match storage_type{
			Storage_type::Zookeeper => {
				let zk = ZooKeeper::connect(address, Duration::from_secs(5), LoggingWatcher).unwrap();
				Storage::init_zk(&zk, topology_name);
				Storage{
					zk_connection: Some(zk),
					topology_name: topology_name.to_string()
				}
			},
			_ => {panic!("")}
		}
	}

	fn init_zk(zk: &ZooKeeper, topology_name: &str){
		let paths = vec!["/antimony".to_string(),
						format!("/antimony/{}", topology_name),
						format!("/antimony/{}/config", topology_name),
						format!("/antimony/{}/SMs", topology_name)];
		for node in paths{
			if !zk.exists(&node, false).unwrap().is_some(){
				zk.create(&node,
                    vec![],
                    Acl::open_unsafe().clone(),
                    CreateMode::Persistent);
			}
		}
	}

	pub fn write_config(self, config: &TopologyConfig){
		if self.zk_connection.is_some(){
			self.zk_connection.unwrap().set_data(
				&format!("/antimony/{}/config", self.topology_name)[..],
				json::encode(config).unwrap().into_bytes(),
				None
			);
		}
	}

	pub fn get_config(self) -> String{
		let mut config = String::new();
		if self.zk_connection.is_some(){
			let data = self.zk_connection.unwrap().get_data(
				&format!("/antimony/{}/config", self.topology_name)[..],
				true
			);
			config = String::from_utf8(data.unwrap().0).unwrap();
		}
		config
	}
}