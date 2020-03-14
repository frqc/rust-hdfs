#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

extern crate libc;

#[cfg(test)]
mod tests {

    use std::ffi::CString;
    use libc::c_void;
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn hdfs_write(){
	unsafe{
		let name_node = CString::new("default").expect("CString::new failed");
		let fs = hdfsConnect(name_node.as_ptr(), 0);

		let write_path = CString::new("t.txt").expect("CString::new failed");
		let write_file = hdfsOpenFile(fs, write_path.as_ptr(), (O_WRONLY |O_CREAT) as i32, 0, 0, 0);

		let buffer = String::from("HHHHHello worldddddd\n");
		let buffer_ptr = buffer.as_ptr() as *const c_void;

		let written_bytes = hdfsWrite(fs, write_file, buffer_ptr, buffer.len() as i32);

		let result = hdfsFlush(fs, write_file);

		hdfsCloseFile(fs, write_file);
		

	}
   }
}
