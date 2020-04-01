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

    use std::ffi::{CStr, CString};
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
	fn test_hdfs_io_read(){
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
	fn test_dir(){
		// test create dir and delete dir;
		unsafe {

			let name_node = CString::new("default").expect("CString::new failed");
			let fs = hdfsConnect(name_node.as_ptr(), 0);

			let random_str: String = thread_rng().sample_iter(&Alphanumeric).take(10).collect();
			let tmp_path = ["/", random_str.as_str()].concat();
			let tmp_path_cstr = CString::new(tmp_path).unwrap();

			hdfsCreateDirectory(fs, tmp_path_cstr.as_ptr());

			let path_info = hdfsGetPathInfo(fs, tmp_path_cstr.as_ptr());
			let path_type = (*path_info).mKind;
			assert_eq!(path_type, tObjectKind_kObjectKindDirectory);

			hdfsDelete(fs, tmp_path_cstr.as_ptr(), 1);

		}

	}

	#[test]
	fn test_pread(){
		unsafe {
			let name_node = CString::new("default").expect("CString::new failed");
			let fs = hdfsConnect(name_node.as_ptr(), 0);
			let file_name: String = thread_rng().sample_iter(&Alphanumeric).take(10).collect();
			
			// write file 
			let write_path = CString::new(file_name.clone()).expect("CString::new failed");
			let write_file = hdfsOpenFile(fs, write_path.as_ptr(), (O_WRONLY |O_CREAT) as i32, 0, 0, 0);

			let s: Vec<String> = (0..1000).map(|x| x.to_string()).collect();
			let write_buffer = s.join("-");
			let write_buffer_ptr = write_buffer.as_ptr() as *const c_void;

			let written_bytes = hdfsWrite(fs, write_file, write_buffer_ptr, write_buffer.len() as i32);
			let _result = hdfsFlush(fs, write_file);

			hdfsCloseFile(fs, write_file);
			assert_eq!(written_bytes as usize, write_buffer.len());

			// read file 
			
			let read_path = CString::new(file_name.clone()).expect("CString::new failed");
			let read_file = hdfsOpenFile(fs, read_path.as_ptr(), O_RDONLY as i32, 0, 0, 0);

			let read_buffer_size:i32 = 100;
			let read_buffer:Vec<u8> = Vec::with_capacity(read_buffer_size as usize + 1);
			let read_buffer = read_buffer.as_ptr();

			let mut remaining = written_bytes;
			
			while remaining > 0 {
				let pos = (written_bytes - remaining) as i64;
				let read_size = std::cmp::min(remaining, read_buffer_size);

				hdfsPread(fs, read_file, pos, read_buffer as *mut c_void, read_size);
				// let readed_str = CStr::from_ptr(read_buffer as *const i8).to_str().unwrap();
				// let mut readed_str = String::from(readed_str);
				// readed_str.truncate(read_size as usize);
				// println!("{}: {}", pos, readed_str);

				let readed_bytes = CStr::from_ptr(read_buffer as *const i8).to_bytes();
				let readed_bytes = readed_bytes.to_vec(); // [0..read_size];
				println!("{} - {:?}", pos, &readed_bytes[..read_size as usize]);

				remaining = remaining - read_size;
			}

			hdfsCloseFile(fs, read_file);
			hdfsDelete(fs, read_path.as_ptr(), 0);

		}

	}

	#[test] 
	fn test_hdfs_io_write() {
		let path = String::from("/write.txt");
		let mut hdfs_file = HdfsFile::create(path.as_str()).unwrap();	

		let buffer = String::from("HHHHHello worldddddd\n");
		hdfs_file.write(buffer.as_bytes()).unwrap();
		hdfs_file.flush().unwrap();
	}

	#[test] 
	fn test_hdfs_io_read_dir() {
		let path = String::from("/");
		let result_list = read_dir(path);

		for result in result_list {
			println!("{}",result.path.to_string_lossy());
		}
	}

	fn get_ramdon_string() -> String {
		thread_rng().sample_iter(&Alphanumeric).take(10).collect()
	}
}
