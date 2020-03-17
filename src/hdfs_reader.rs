include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::io::Read;
use std::ffi::CString;
use libc::c_void;

// #[derive(Default)]
pub struct HdfsReader {
    pub name_node: String, 
    pub path: String,
    pub read_pos: i64,
    pub size: i64,
    fs: Option<hdfsFS>,
    opened_file: Option<hdfsFile>, 
}

impl HdfsReader {

    pub fn init_with_name_node(name_node: String, path: String) -> HdfsReader {
        let mut reader = HdfsReader {
            name_node: name_node,
            path: path,
            read_pos: 0,
            size: 0,
            fs: None,
            opened_file: None, 
        };

        reader.connect();
        reader.open_file();
    
        reader

    }

    pub fn init(path: String) -> HdfsReader {
        let mut reader = HdfsReader {
            name_node: String::from("default"), 
            path: path,
            read_pos: 0,
            size: 0,
            fs: None,
            opened_file: None, 
        };
    
        reader.connect();
        reader.open_file();
    
        reader
    }

    pub fn from_split(path: String, start: i64, end: i64) -> HdfsReader {

        let mut reader = HdfsReader {
            name_node: String::from("default"), 
            path: path,
            read_pos: 0,
            size: 0,
            fs: None,
            opened_file: None, 
        };
    
        reader.connect();
        reader.open_file();

        reader.read_pos = start;
        reader.size = end;
    
        reader


    }

    fn connect(&mut self) {
        let name_node_ptr = CString::new(self.name_node.clone()).expect("CString::new failed");
        let fs = unsafe {
            hdfsConnect(name_node_ptr.as_ptr(), 0)
        };
        self.fs = Some(fs);
    }

    fn open_file(&mut self) {
        let read_path = CString::new(self.path.clone()).expect("CString::new failed");

        // TODO: check file exist or not first 
        let file_info = unsafe {
            hdfsGetPathInfo(self.fs.unwrap(), read_path.as_ptr())
        };

        let file_size = unsafe {
            (*file_info).mSize
        };

        self.size = file_size; 

        let opened_file = unsafe {
            hdfsOpenFile(self.fs.unwrap(), read_path.as_ptr(), O_RDONLY as i32, 0, 0, 0)
        };

        self.opened_file = Some(opened_file);
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
}

impl Drop for HdfsReader {
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

impl Read for HdfsReader {
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
