#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

extern crate libc;
extern crate rand;


#[cfg(test)]
mod tests {

    use std::ffi::CString;
    use std::ffi::CStr;
	use std::str::Utf8Error;
	
	use rand::{thread_rng, Rng};
	use rand::distributions::Alphanumeric;


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

			let write_path = CString::new("/test.txt").expect("CString::new failed");
			let write_file = hdfsOpenFile(fs, write_path.as_ptr(), (O_WRONLY |O_CREAT) as i32, 0, 0, 0);

			let buffer = String::from("HHHHHello worldddddd\n");
			let buffer_ptr = buffer.as_ptr() as *const c_void;

			let _written_bytes = hdfsWrite(fs, write_file, buffer_ptr, buffer.len() as i32);

			let _result = hdfsFlush(fs, write_file);

			hdfsCloseFile(fs, write_file);
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
	fn test_is_dir(){
		unsafe {
			let name_node = CString::new("default").expect("CString::new failed");
			let fs = hdfsConnect(name_node.as_ptr(), 0);

			let path_1 = String::from("/data/CC-MAIN-20161202170900-00000-ip-10-31-129-80.ec2.internal.warc.wet.gz");
			let path_1_cstr = CString::new(path_1.clone()).unwrap();
			let path_1_info = hdfsGetPathInfo(fs, path_1_cstr.as_ptr());
			let path_1_type = (*path_1_info).mKind;


			let path_2 = String::from("/data");
			let path_2_cstr = CString::new(path_2.clone()).unwrap();
			let path_2_info = hdfsGetPathInfo(fs, path_2_cstr.as_ptr());
			let path_2_type = (*path_2_info).mKind;


			println!("file 1 is dir: {},\nfile 2 is dir: {}", 
			path_1_type == tObjectKind_kObjectKindDirectory, 
			path_2_type == tObjectKind_kObjectKindDirectory);
		}
	}

	#[test]
	fn test_list_dir(){
		unsafe{

			let name_node = CString::new("default").expect("CString::new failed");
			let fs = hdfsConnect(name_node.as_ptr(), 0);

			let path_2 = String::from("/data");
			let path_2_cstr = CString::new(path_2.clone()).unwrap();

			let mut num_entries: i32 = 0;
			let list_result = hdfsListDirectory(fs, path_2_cstr.as_ptr(), &mut num_entries);

			let list_result = std::slice::from_raw_parts(list_result, num_entries as usize);

			for result in list_result {
				let file_name = (*result).mName;
				let file_name = CStr::from_ptr(file_name).to_str().unwrap(); 

				let file_size = (*result).mSize;

				println!("inside dir: {}, size: {}", file_name, file_size);
			}

		}
	}

   #[test]
   fn get_info_for_file_split(){
      unsafe {

        let name_node = CString::new("default").expect("CString::new failed");
        let fs = hdfsConnect(name_node.as_ptr(), 0);

        let path = String::from("/data/CC-MAIN-20161202170900-00000-ip-10-31-129-80.ec2.internal.warc.wet.gz");
        let path_cstr = CString::new(path.clone()).unwrap();

        let file_info = hdfsGetPathInfo(fs, path_cstr.as_ptr());
        let file_name = (*file_info).mName;
        let file_size = (*file_info).mSize;
        let file_replication = (*file_info).mReplication;
        let file_block_size = (*file_info).mBlockSize;

        println!("file name: {}, size: {}, block size: {}", path , file_size, file_block_size);

        let hosts = hdfsGetHosts(fs, path_cstr.as_ptr(), 0, file_size);
        let len = (0..)
              .take_while(|i| {
                 let arg = hosts.offset(*i);
                 !(*arg).is_null()
                 })
               .count();

        for i in 0..len {
          let one_hosts = *(hosts.offset(i as isize));
          let one_hosts_len = (0..).take_while(|i| {
                                let arg = one_hosts.offset(*i);
                                !(*arg).is_null()
                              }).count();

          let one_hosts_str: Result<Vec<String>, Utf8Error> = std::slice::from_raw_parts(one_hosts, one_hosts_len)
                                .iter()
                                .map(|arg| CStr::from_ptr(*arg).to_str().map(ToString::to_string))
                                .collect();

          for s in one_hosts_str.unwrap() {
            println!("{}", s);
          }


        }
      }
   }
}
