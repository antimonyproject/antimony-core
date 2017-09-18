use config::topology_config::TopologyConfig;
use utils::storage::Storage;
use futures::Stream;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Core;
use futures::{Future, future};
use std::thread;
use std::time::Duration;
use tokio_io::{self, AsyncRead, AsyncWrite};
use config::physical_plan::PhysicalPlan;
use utils::message::Message;

pub struct TopologyMaster{
	topology_config: TopologyConfig 
}

impl TopologyMaster{
	pub fn new(storage: Storage) -> Self{
		TopologyMaster{
			topology_config: TopologyConfig::from_storage(storage)
		}
	}

	fn keep_sm(mut client: TcpStream) -> Box<Future<Item = (), Error = ()>>{
		future::loop_fn(client, |cc|{
	    let mut x = [0;10];
	    tokio_io::io::read(cc, x)
	        .and_then(|c| {
	            if c.2 == 0{
	                Ok(future::Loop::Break(c.0))
	            }else{
	                tokio_io::io::write_all(c.0, b"pong\n")
	                    .and_then(|c| Ok(future::Loop::Continue(c.0))).wait()
	                
	            }
	        })
		}).then(|_| Ok(())).boxed()
	}

	pub fn start_tm(self){
		let physical_plan = PhysicalPlan::from_config(&self.topology_config,
														"/tmp/bin".to_string());
		let physical_plan = Message::Config(physical_plan);
		let mut core = Core::new().unwrap();
	    let tm_listener = TcpListener::bind(&"0.0.0.0:5000".parse().unwrap(),
	                                     &core.handle()).unwrap();
	    let handle = core.handle();
	    let server = tm_listener.incoming();
	    let ss = server.for_each(|sm| {
	        tokio_io::io::write_all(sm.0, physical_plan.encoded())
                        .and_then(|c| {
                        	handle.spawn(TopologyMaster::keep_sm(c.0));
                        	Ok(())
                        })
	    });
	    core.run(ss).unwrap();
	}
}