include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::io::{Read, Write};
use std::path::PathBuf;
use std::fs::{OpenOptions};
use std::ffi::{CStr, CString};
use libc::c_void;



pub struct HdfsFile {
    pub name_node: String, 
    pub path: PathBuf,
    pub read_pos: i64,
    pub size: i64,
    fs: Option<hdfsFS>,
    opened_file: Option<hdfsFile>, 

    // typedef struct  {
    //     tObjectKind mKind;   /* file or directory */
    //     char *mName;         /* the name of the file */
    //     tTime mLastMod;      /* the last modification time for the file in seconds */
    //     tOffset mSize;       /* the size of the file in bytes */
    //     short mReplication;    /* the count of replicas */
    //     tOffset mBlockSize;  /* the block size for the file */
    //     char *mOwner;        /* the owner of the file */
    //     char *mGroup;        /* the group associated with the file */
    //     short mPermissions;  /* the permissions associated with the file */
    //     tTime mLastAccess;    /* the last access time for the file in seconds */
    // } hdfsFileInfo;
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
                    hdfsGetPathInfo(fs, file_path.as_ptr())
                };

                let file_size = unsafe {
                    (*file_info).mSize
                };

                let opened_file = unsafe {
                    hdfsOpenFile(fs, file_path.as_ptr(), flag as i32, 0, 0, 0)
                };
                
                self.size = file_size;
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
