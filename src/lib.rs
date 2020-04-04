#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

extern crate libc;
extern crate rand;

pub mod hdfs_fs;
pub use hdfs_fs::*;

#[cfg(test)]
mod tests {

    use std::ffi::CString;
	use std::io::{Write, BufReader, prelude::*};
	use rand::{thread_rng, Rng};
	use rand::distributions::Alphanumeric;


    use libc::c_void;
    use super::*;


	#[test]
	fn hdfs_raw_write(){
		unsafe{
			let name_node = CString::new("default").unwrap();
			let fs = hdfsConnect(name_node.as_ptr(), 0);

			let random_str: String = thread_rng().sample_iter(&Alphanumeric).take(10).collect();
			let write_path = ["/", random_str.as_str()].concat();
			let write_path = CString::new(write_path).unwrap();

			let write_file = hdfsOpenFile(fs, write_path.as_ptr(), (O_WRONLY |O_CREAT) as i32, 0, 0, 0);

			let buffer = String::from("HHHHHello worldddddd\n");
			let buffer_ptr = buffer.as_ptr() as *const c_void;

			let written_bytes = hdfsWrite(fs, write_file, buffer_ptr, buffer.len() as i32);
			let result = hdfsFlush(fs, write_file);
			
			assert_eq!(written_bytes, buffer.len() as i32);
			assert_eq!(result, 0);

			hdfsCloseFile(fs, write_file);
			hdfsDisconnect(fs);
		}
	}


	#[test] 
	fn test_hdfs_fs_write() {
		let random_str = get_ramdon_string();
		let path = ["/", random_str.as_str()].concat();

		let mut hdfs_file = HdfsFile::create(path.as_str()).unwrap();	

		let buffer = String::from("HHHHHello worldddddd\n");
		let written_bytes = hdfs_file.write(buffer.as_bytes()).unwrap();
		hdfs_file.flush().unwrap();

		assert_eq!(written_bytes, buffer.len());

		hdfs_file.delete().unwrap();
		hdfs_file.close();
	}

	#[test] 
	fn test_hdfs_fs_read(){
		let random_str = get_ramdon_string();
		let path = ["/", random_str.as_str()].concat();
		let mut hdfs_writer = HdfsFile::create(path.as_str()).unwrap();	

		let buffer = String::from("HHHHHello\nworldddddd\n");
		hdfs_writer.write(buffer.as_bytes()).unwrap();
		hdfs_writer.flush().unwrap();
		hdfs_writer.close();

		let hdfs_reader = HdfsFile::open(path.as_str()).unwrap();
		let reader = BufReader::new(hdfs_reader);
		println!("outputing file:");
		for line in reader.lines() {
			println!("{}", line.unwrap());
		}
	}

	#[test] 
	fn test_hdfs_fs_read_dir() {
		let path = String::from("/");
		let entries = read_dir(path);

		for entry in entries {
			println!("{}",entry.path.to_string_lossy());
			// let reader = BufReader::new(entry);
			// for line in reader.lines() {
			// 	println!("{}", line.unwrap());
			// }
		}
	}

	fn get_ramdon_string() -> String {
		thread_rng().sample_iter(&Alphanumeric).take(10).collect()
	}
}
