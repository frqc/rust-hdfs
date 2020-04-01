include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::fs::{OpenOptions};
use std::ffi::{CStr, CString};
use libc::c_void;


pub struct HdfsFile {
    pub name_node: String, 
    pub path: PathBuf,
    pub read_pos: i64,
    pub size: i64,
    pub block_size: i64,
    fs: Option<hdfsFS>,
    opened_file: Option<hdfsFile>, 
}


impl HdfsFile {

    // Attempts to open a file in read-only mode.
    pub fn init_with_name_node<P:Into<String>, Q: Into<PathBuf>>(name_node: P, 
        path: Q) -> std::io::Result<HdfsFile> {
        let mut reader = HdfsFile {
            name_node: name_node.into(),
            path: path.into(),
            read_pos: 0,
            size: 0,
            block_size: 0,
            fs: None,
            opened_file: None, 
        };

        reader.connect();
        reader.open_with_flag(O_RDONLY).unwrap();
    
        Ok(reader)

    }

    // Attempts to open a file in read-only mode.
    pub fn open<P: Into<PathBuf>>(path: P) -> std::io::Result<HdfsFile> {
        let mut reader = HdfsFile {
            name_node: String::from("default"), 
            path: path.into(),
            read_pos: 0,
            size: 0,
            block_size: 0,
            fs: None,
            opened_file: None, 
        };
    
        reader.connect();
        reader.open_with_flag(O_RDONLY).unwrap();
    
        Ok(reader)
    }

    // Opens a file in write mode.
    pub fn create<P: Into<PathBuf>>(path: P) -> std::io::Result<HdfsFile> {
        let mut reader = HdfsFile {
            name_node: String::from("default"), 
            path: path.into(),
            read_pos: 0,
            size: 0,
            block_size: 0,
            fs: None,
            opened_file: None, 
        };
    
        reader.connect();
        reader.open_with_flag(O_WRONLY |O_CREAT).unwrap();
    
        Ok(reader)
    }

    
    pub fn with_option() -> OpenOptions {
        unimplemented!();
    }

    pub fn from_split<P: Into<PathBuf>>(path: P, start: i64, end: i64) -> HdfsFile {

        let mut reader = HdfsFile {
            name_node: String::from("default"), 
            path: path.into(),
            read_pos: start,
            size: end,
            block_size: 0,
            fs: None,
            opened_file: None, 
        };
    
        reader.connect();
        reader.open_with_flag(O_RDONLY).unwrap();

        reader
    }

    pub fn get_hosts(&mut self, start: u64, end: u64) -> std::io::Result<Vec<String>> {
        let fs = self.fs.unwrap();
        let file_path = self.path.to_string_lossy();
        let file_path = CString::new(file_path.as_bytes()).unwrap();
        
        unsafe {
            let block_hosts = hdfsGetHosts(fs, file_path.as_ptr(), start as i64, end as i64);
            let block_count = (0..).take_while(
                |i| { let arg = block_hosts.offset(*i); !(*arg).is_null() })
                .count();
            
            let mut hosts_strings: Vec<String> = Vec::new();
            for i in 0..block_count {
                let hosts = *(block_hosts.offset(i as isize));
                let hosts_len = (0..).take_while(
                    |i| { let arg = hosts.offset(*i); !(*arg).is_null() }
                ).count();

                let hosts_iter = std::slice::from_raw_parts(hosts, hosts_len).iter();
                for one_host in hosts_iter {
                    hosts_strings.push(CStr::from_ptr(*one_host).to_string_lossy().into_owned());
                }
            }
            Ok(hosts_strings)
        }
    }

    

    fn connect(&mut self) {
        let name_node_ptr = CString::new(self.name_node.as_bytes()).unwrap();
        let fs = unsafe {
            hdfsConnect(name_node_ptr.as_ptr(), 0)
        };
        self.fs = Some(fs);
    }

    fn open_with_flag(&mut self, flag: u32) -> std::io::Result<()> {
        let file_path = self.path.to_string_lossy();
        let file_path = CString::new(file_path.as_bytes()).unwrap();
        let fs = self.fs.unwrap();

        let file_exists = unsafe {
            hdfsExists(fs, file_path.as_ptr()) == 0
        };

        let create_flag = (flag & O_CREAT) != 0;
        match (file_exists, create_flag) {

            (false, false) => {
                Err(std::io::Error::new(std::io::ErrorKind::NotFound, format!("No such file: {:?}", self.path)))
            }

            (true, _) => {
                let file_info = unsafe {
                    *hdfsGetPathInfo(fs, file_path.as_ptr())
                };

                let file_size = file_info.mSize;
                let block_size = file_info.mBlockSize;
                
                let opened_file = unsafe {
                    hdfsOpenFile(fs, file_path.as_ptr(), flag as i32, 0, 0, 0)
                };
                
                self.size = file_size;
                self.block_size = block_size;
                self.opened_file = Some(opened_file);
                Ok(())

            }

            _ => {

                let opened_file = unsafe {
                    hdfsOpenFile(fs, file_path.as_ptr(), flag as i32, 0, 0, 0)
                };

                self.opened_file = Some(opened_file);
                Ok(())
            }
        }
    }

    pub fn close(&mut self) {
        match self.opened_file {
            Some(file) => {
                unsafe { hdfsCloseFile(self.fs.unwrap(), file); }
                self.opened_file = None; 
            }, 
            _ => {},
        }

        match self.fs {
            Some(fs) => {
                unsafe { hdfsDisconnect(fs); }
                self.fs = None; 
            }, 
            _ => {},
        }
    }
    
    pub fn delete(&mut self) -> std::io::Result<()>{

        match self.opened_file {
            Some(file) => {
                unsafe { hdfsCloseFile(self.fs.unwrap(), file); }
                self.opened_file = None; 
            }, 
            _ => {},
        };

        let file_path = self.path.to_string_lossy();
        let file_path = CString::new(file_path.as_bytes()).unwrap();

        let result = unsafe {
            hdfsDelete(self.fs.unwrap(), file_path.as_ptr(), 0)
        };

        match result {
            0 => Ok(()),
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("Failed to delete {:?}", self.path))),
        }
    }
}

impl Drop for HdfsFile {
    fn drop(&mut self) {
        match self.opened_file {
            Some(file) => {
                unsafe { hdfsCloseFile(self.fs.unwrap(), file); }
                self.opened_file = None; 
            }, 
            _ => {},
        }

        match self.fs {
            Some(fs) => {
                unsafe { hdfsDisconnect(fs); }
                self.fs = None; 
            }, 
            _ => {},
        }
    }
}

impl Read for HdfsFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining_size = self.size - self.read_pos;
        let read_size = std::cmp::min((buf.len()) as i32, remaining_size as i32);

        match self.opened_file {
            Some(_) => {},
            _ => {
                self.connect();
                self.open_with_flag(O_RDONLY).unwrap();
            }
        }

        unsafe {
            hdfsPread(
                self.fs.unwrap(), 
                self.opened_file.unwrap(), 
                self.read_pos, 
            buf.as_mut_ptr() as *mut c_void, read_size);
        };
        
        self.read_pos += read_size as i64;

        Ok(read_size as usize)
    }
}

impl Write for HdfsFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let buf_ptr = buf.as_ptr() as *const c_void;
        let written_bytes = unsafe {
            hdfsWrite(self.fs.unwrap(), self.opened_file.unwrap(), buf_ptr, buf.len() as i32)
        };

        Ok(written_bytes as usize)

    }

    fn flush(&mut self) -> std::io::Result<()> {

        let result = unsafe {
            hdfsFlush(self.fs.unwrap(), self.opened_file.unwrap())
        };

        match result {
            0 => Ok(()),
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "failed to flush to hdfs")),
        }
    }
}

pub fn read_dir<P: AsRef<Path>>(path: P) -> Vec<HdfsFile>{

    let mut file_list = Vec::new();

    let name_node = CString::new("default").unwrap();
    let fs = unsafe {
        hdfsConnect(name_node.as_ptr(), 0)
    };

    let file_path = path.as_ref().to_string_lossy();
    let file_path = CString::new(file_path.as_bytes()).unwrap();

    let mut num_entries: i32 = 0;
    let list_result = unsafe {
        hdfsListDirectory(fs, file_path.as_ptr(), &mut num_entries)
    };

    let list_result = unsafe {
        std::slice::from_raw_parts(list_result, num_entries as usize)
    };

    for result in list_result {
        let file_name = (*result).mName;
        let file_name = unsafe {
            CStr::from_ptr(file_name).to_str().unwrap()
        }; 

        let file_size = (*result).mSize;
        let file_block_size = (*result).mBlockSize;

        let hdfs_file = HdfsFile {
            name_node: String::from("default"),
            path: PathBuf::from(file_name),
            read_pos:0,
            size: file_size,
            block_size: file_block_size,
            fs: None,
            opened_file: None
        };

        file_list.push(hdfs_file);
    }

    unsafe { hdfsDisconnect(fs) };
    file_list
}