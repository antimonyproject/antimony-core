use std::collections::HashMap;
use config::topology_config::*;

#[derive(Debug, Clone)]
#[derive(RustcDecodable, RustcEncodable)]
pub struct Instance{
    pub name: String,
    pub cmd: String,
    pub instance_type: Component,
    pub input_stream: Option<String>,
    pub port: Option<i16>,
}

#[derive(Debug, Clone)]
#[derive(RustcDecodable, RustcEncodable)]
pub struct TopologySM{
    pub instances: Vec<Instance>,
    pub sm_id: i16,
    pub ip: Option<String>,
    pub port: Option<i16>,

}

#[derive(RustcDecodable, RustcEncodable)]
#[derive(Debug, Clone)]
pub struct PhysicalPlan{
    pub topology: HashMap<usize, TopologySM>,
    pub self_id: usize,
    pub self_ip: Option<String>
}

impl PhysicalPlan{

    pub fn empty() -> Self{
        PhysicalPlan{
            topology: HashMap::new(),
            self_id: 0,
            self_ip: None
        }
    }

    // Takes a topology_config object created from a config file
    // and builds an in-memory object of the physical mapping
    // sent to stream managers
    pub fn from_config(config: &TopologyConfig, bin_dir: String) -> Self{
        let mut starting_port = 5000i16; // configurable
        let mut topology: HashMap<usize, TopologySM> = HashMap::new();
        let mut sm = 1;
        let mut instances: Vec<Instance> = Vec::new();
        for instance in &config.topology{
            for i in 0..instance.instance_count{            
                starting_port += 1;
                let ins = Instance{
                    cmd: format!("{}/{}", bin_dir, &instance.name).to_string(),
                    instance_type: instance.item_type.clone(),
                    name: instance.name.to_string(),
                    input_stream: instance.input_stream.clone(),
                    port: Some(starting_port)
                };
                instances.push(ins);
                if instances.len() == config.instances_per_sm as usize{
                    starting_port += 1;
                    let topology_sm = TopologySM{
                        instances: instances,
                        sm_id: starting_port,
                        ip: None,
                        port: Some(starting_port),
                    };
                    topology.insert(sm.clone(), topology_sm);
                    sm += 1;
                    instances = Vec::new();
                    starting_port += 2;
                }
            }
        }
        if instances.len() > 0{
            starting_port += 1;
            let topology_sm = TopologySM{
                instances: instances,
                sm_id: starting_port,
                ip: None,
                port: Some(starting_port)
            };
            topology.insert(sm.clone(), topology_sm);
        }
        let topology = PhysicalPlan{
            topology: topology,
            self_id: 0,
            self_ip: None
        };
        topology
    }
}