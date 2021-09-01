pub mod tasks {
    use std::{collections::HashMap, fs, io, path};

    pub trait Target {
        fn read(&self) -> io::Result<String>;
        fn write(&self, s: &str) -> io::Result<()>;
        fn delete(&self) -> io::Result<()>;
        fn exists(&self) -> bool;
    }

    pub struct FileTarget {
        pub cache_dir: String,
        pub local_filename: String,
    }

    impl FileTarget {
        fn filename(&self) -> path::PathBuf {
            path::Path::new(self.cache_dir.as_str()).join(self.local_filename.as_str())
        }

    }

    impl Target for FileTarget {
        fn read(&self) -> io::Result<String> {
            fs::read_to_string(&self.filename())
        }

        fn write(&self, s: &str) -> io::Result<()> {
            fs::write(self.filename(), s)
        }

        fn exists(&self) -> bool {
            self.filename().is_file()
        }

        fn delete(&self) -> io::Result<()> {
            if self.exists() {
                return fs::remove_file(self.filename());
            }
            Ok(())
        }
    }

    pub trait Task {
        fn get_target(&self) -> Box<dyn Target>;
        fn get_data(&self) -> io::Result<String>;
        fn run(&self) -> io::Result<String> {
            let target = self.get_target();
            if target.exists() {
                return target.read();
            } else {
                let data = self.get_data()?;
                target.write(data.as_str())?;
                return Ok(data);
            }
        }
        fn requires(&self) -> HashMap::<String, Box<dyn Target>> {
            HashMap::new()
        }
    }
}


#[cfg(test)]
mod tests {
    use std::io;

    use crate::tasks::{FileTarget, Target, Task};

    #[test]
    fn file_target() {
        let ft = FileTarget{cache_dir: String::from("/tmp"), local_filename: String::from("test_target.txt")};
        ft.delete().unwrap();
        assert!(!ft.exists());
        ft.write("test data").unwrap();
        assert!(ft.exists());
        assert_eq!(ft.read().unwrap(), String::from("test data"));
    }

    #[test]
    fn file_task() {
        struct FileTask {}
        impl Task for FileTask {
            fn get_target(&self) -> Box<dyn Target> {
                Box::new(FileTarget{cache_dir: String::from("/tmp"), local_filename: String::from("test_task_target.txt")})
            }

            fn get_data(&self) -> io::Result<String> {
                Ok(String::from("some data"))
            }
        }

        let task = FileTask{};
        let target = task.get_target();
        // test with no starting data
        target.delete().unwrap();
        assert_eq!(task.run().unwrap(), String::from("some data"));
        // test with cached starting data
        assert_eq!(task.run().unwrap(), String::from("some data"));
    }
}
