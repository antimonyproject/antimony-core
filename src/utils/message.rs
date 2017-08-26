use config::physical_plan::PhysicalPlan;
use rustc_serialize::json;
use std::io::prelude::*;

#[derive(Debug)]
#[derive(RustcEncodable, RustcDecodable)]
pub enum Message{
    Data(String, String, String), //stream, dest, data
    Ready,
    Metrics,
    HeartBeat,
    Config(PhysicalPlan),
}

impl Message{
	pub fn from_tcp() {
		unimplemented!();
	}

	pub fn to_tcp() {
		unimplemented!();
	}

	pub fn from_uds() {
		unimplemented!();
	}

	pub fn to_uds() {
		unimplemented!();
	}

	fn encode(){
		unimplemented!();
	}

	fn decode(){
		unimplemented!();
	}
}