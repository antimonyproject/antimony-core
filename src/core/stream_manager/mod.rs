use utils::storage::Storage;
use std::net::ToSocketAddrs;
use futures::{Future, future, Stream, self};
use tokio_uds::{UnixListener, UnixStream};
use tokio_core::net::{TcpListener, TcpStream};
use tokio_io::AsyncRead;
use tokio_core::reactor::Core;
use tokio_io;
use utils::message::Message;
use config::physical_plan::PhysicalPlan;
use config::routing_map::{RoutingMap, RoutingConnection};
use std::thread;
use std::time::Duration;
use std::collections::HashMap;
use futures_cpupool::CpuPool;
use std;
use std::sync::{Arc, Mutex};

pub struct StreamManager {
    physical_plan: PhysicalPlan,
    storage: Storage
}

impl StreamManager{
    pub fn new(storage: Storage) -> Self{
        let mut p_plan = PhysicalPlan::empty();
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let addr = storage.get_tm_addr().to_socket_addrs().unwrap().next().unwrap();
        // without this scope here, the mutable borrow of "p_plan" would go out of scope
        // only at the end of the function's body. This enables using p_plan after core.run()
        {            
            let socket = TcpStream::connect(&addr, &handle).and_then(|socket| {
                Message::from_tcp(socket).and_then(|x| {
                    if let Message::Config(physical_plan) = x.0{
                        p_plan = physical_plan;
                    }
                    Ok(())
                })
            });
            // TODO: keep the connection to the topology master open for heart beats.
            let _ = core.run(socket);
        }
        StreamManager{
            physical_plan: p_plan,
            storage: storage
        }
    }

    pub fn start(&self){
        let port = self.physical_plan.topology.get(&self.physical_plan.self_id).unwrap().port.unwrap();
        let tcp_addr = format!("0.0.0.0:{}", port);
        // TODO: file path
        let uds_sock = format!("sm_{}.sock", self.physical_plan.self_id);
        let mut core = Core::new().unwrap();
        let tcp_listener = TcpListener::bind(&tcp_addr.parse().unwrap(),
                                         &core.handle()).unwrap();
        let uds_listener = UnixListener::bind(&uds_sock,
                                         &core.handle()).unwrap();
        let handle = core.handle();
        let tcp = tcp_listener.incoming();
        let uds = uds_listener.incoming();

        let routing_map = Arc::new(Mutex::new(RoutingMap::from_physical_plan(&self.physical_plan)));
        let routing_map_clone = routing_map.clone();

        let h = handle.clone();
        let h2 = handle.clone();
        let servers = tcp.merge(uds).for_each(move |ca| {
            let routing_map = routing_map_clone.clone();
            let routing_map2 = routing_map_clone.clone();
            let handle = h2.clone();
            match ca{
                futures::stream::MergedItem::First((client, addr)) => {
                    let routing_map = routing_map2.clone();
                    let tcp_loop = future::loop_fn::<_, TcpStream, _, _>(client, move |conn|{
                        let h = handle.clone();
                        let r_map = routing_map.clone();
                        Message::from_tcp(conn).and_then( move |m| {
                            let r_map2 = r_map.clone();
                            if let Message::Data(s, d) = m.0{
                                r_map2.lock().unwrap().route_tcp(h, Message::Data(s, d));
                            }

                            Ok(future::Loop::Continue(m.1))
                        })

                    }).and_then(|_| Ok(())).map_err(|_| ());
                    h.spawn(tcp_loop);
                },
                futures::stream::MergedItem::Second((client, _)) => {
                    let r_map = routing_map.clone();
                    let uds = Message::from_uds(client).and_then(move |m| {
                        let (rx, tx) = m.1.split();
                        if let Message::Local(instance_id) = m.0{
                            routing_map.lock().unwrap().register(
                                instance_id,
                                RoutingConnection::LocalConnection(Some(tx))
                            );
                        }
                        Ok(rx)
                    }).and_then(|client| {
                        let uds_loop = future::loop_fn::<_, tokio_io::io::ReadHalf<UnixStream>, _, _>(client, move |conn|{
                            let r_map = r_map.clone();
                            let h = handle.clone();
                            Message::from_half_uds(conn).and_then(move |m| {
                                if let Message::Data(s, d) = m.0{
                                    r_map.lock().unwrap().route_uds(h, Message::Data(s, d));

                                }
                                Ok(future::Loop::Continue(m.1))
                            })

                        }).and_then(|_| Ok(()));
                        uds_loop
                    }).map_err(|_| ());

                    h.spawn(uds);
                },
                futures::stream::MergedItem::Both(_, _) => {}
            };
            Ok(())
        });
        self.storage.register_sm(tcp_addr, self.physical_plan.self_id);

        let pool = CpuPool::new_num_cpus();
        let storage = self.storage.cloned();
        let physical_plan = self.physical_plan.clone();
        let sm_count = self.physical_plan.topology.len();
        let a = pool.spawn_fn(move || {
            let connected = StreamManager::connect_to_sms(storage, physical_plan);
            let res: Result<HashMap<String, String>, std::io::Error> = Ok(connected);
            res
        }).and_then(move |t| {
            for (sm_id, addr) in t{
                let r_map = routing_map.clone();
                if sm_id != self.physical_plan.self_id.to_string(){
                    let addr = addr.clone().parse().unwrap();
                    handle.spawn(
                        TcpStream::connect(&addr, &handle).map_err(|_| {}).
                                    and_then(move |x| {
                                        let mut r_map = r_map.lock().unwrap();
                                        r_map.register(sm_id, RoutingConnection::SmConnection(Some(x)));
                                        if r_map.get_tcp_connections() == sm_count - 1{
                                            //start local components
                                        }
                                        Ok(())
                                    } )
                    );
                }
            }
            Ok(())
        });

        let aa = servers.join(a);
        core.run(aa).unwrap();
    }

    pub fn connect_to_sms(storage: Storage, physical_plan: PhysicalPlan) -> HashMap<String, String>{
        let sm_count = physical_plan.topology.len();
        loop{
            let connected = storage.list_managers();
            if connected.len() == sm_count{
                return connected
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
}