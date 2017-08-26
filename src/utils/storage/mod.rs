use zookeeper::{Acl, CreateMode, Watcher, WatchedEvent, ZooKeeper};
use std::time::Duration;


struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent){}
}

pub enum Storage_type{
	Zookeeper
}

pub struct Storage{
	zk_connection: Option<ZooKeeper>,
	topology_name: String
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
}