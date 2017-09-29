use std::collections::HashMap;
use config::physical_plan::PhysicalPlan;
use tokio_core::net::TcpStream;
use tokio_uds::UnixStream;
use tokio_io::io::WriteHalf;
use utils::message::Message;
use futures::Future;
use tokio_io;
use tokio_core::reactor::Handle;
#[derive(Debug)]
pub enum RoutingConnection{
                  //sm_id
    SmConnection(Option<TcpStream>),
    LocalConnection(Option<WriteHalf<UnixStream>>),
}

#[derive(Debug)]
pub enum ConnId{
    Tcp(String),
    Local(String)
}

impl ConnId{
    pub fn value(&self) -> String{
        match self{
            &ConnId::Tcp(ref s) => s.to_owned(),
            &ConnId::Local(ref s) => s.to_owned()
        }
    }

    pub fn is_local(&self) -> bool{
        match self{
            &ConnId::Local(_) => true,
            _ => false
        }
    }
}

#[derive(Debug)]
pub struct RoutingMap{
    //                   stream          component   id
    pub streams: HashMap<String, HashMap<String, Vec<ConnId>>>,
    pub connections: HashMap<String, RoutingConnection>
}

impl RoutingMap{
    pub fn from_physical_plan(physical_plan: &PhysicalPlan) -> Self{

        let mut streams: HashMap<String, HashMap<String, Vec<ConnId>>> = HashMap::new();

        for (sm, components) in &physical_plan.topology{
            for i in &components.instances{
                match i.input_stream.as_ref(){
                    Some(input_stream) => {
                        if !streams.contains_key(input_stream){
                            streams.insert(input_stream.clone(), HashMap::new());
                        }
                        if !streams.get(input_stream).unwrap().contains_key(&i.name){
                            streams.get_mut(input_stream).unwrap().insert(i.name.clone(), vec![]);
                        }
                        if sm == &physical_plan.self_id{
                            streams.get_mut(input_stream).unwrap().get_mut(&i.name).unwrap()
                                    .push(ConnId::Local(format!("{}_{}", i.name, i.port.unwrap())));
                        }else{
                            streams.get_mut(input_stream).unwrap().get_mut(&i.name).unwrap()
                                    .push(ConnId::Tcp(sm.to_string()));
                        }
                    },
                    None => {}
                }
            }                
        }
        RoutingMap{streams: streams, connections: HashMap::new()}

    }

    pub fn register(&mut self, id: String, conn: RoutingConnection){
        self.connections.insert(id, conn);
    }

    pub fn get_tcp_connections(&self) -> usize{
        let mut len = 0;
        for (_, conn) in &self.connections{
            if let &RoutingConnection::SmConnection(Some(_)) = conn{
                len += 1;
            }
        }
        len
    }
    // propably will need to a handle here.
    pub fn route_uds(&mut self, handle: Handle, tuple: Message){
        if let Message::Data(stream, value) = tuple{
            for (_, conns) in self.streams.get_mut(&stream).unwrap(){
                for connection in conns{                
                    match self.connections.get_mut(&connection.value()).unwrap(){
                        &mut RoutingConnection::LocalConnection(Some(ref mut conn)) => {
                            let _ = tokio_io::io::write_all(conn, Message::Data(stream.clone(), value.clone()).encoded()).wait();//and_then(|_| Ok(())).map_err(|_| ());
                        },
                        &mut RoutingConnection::SmConnection(Some(ref mut conn)) => {
                            let _ = tokio_io::io::write_all(conn, Message::Data(stream.clone(), value.clone()).encoded()).wait();//and_then(|_| Ok(())).map_err(|_| ());
                        },
                        _ => {}
                    } 
                }
            }
        }
    }

    pub fn route_tcp(&mut self, handle: Handle, tuple: Message){
        if let Message::Data(stream, value) = tuple{
            for (_, conns) in self.streams.get_mut(&stream).unwrap(){
                for connection in conns{                
                    if connection.is_local(){
                        match self.connections.get_mut(&connection.value()).unwrap(){
                            &mut RoutingConnection::LocalConnection(Some(ref mut conn)) => {
                                let _ = tokio_io::io::write_all(conn, Message::Data(stream.clone(), value.clone()).encoded()).wait();//and_then(|_| Ok(())).map_err(|_| ());
                            },
                            _ => {}
                        } 
                    }
                }
            }
        }
    }
}
