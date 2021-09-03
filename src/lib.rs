pub mod tasks {
    use std::{collections::HashMap, fs, io, path};

    /// The Target trait represents cached data. The data is stored as a String, and can be used 
    /// with serde for serialization of other types.
    pub trait Target {
        /// Read data from a cache
        fn read(&self) -> io::Result<String>;

        /// Write data to a cache
        fn write(&self, s: &str) -> io::Result<()>;

        /// Delete cache data
        fn delete(&self) -> io::Result<()>;

        /// Does the cache exist
        fn exists(&self) -> bool;
    }

    /// The FileTarget type implements Target, using a file as the cache destination.
    pub struct FileTarget {
        pub cache_dir: &'static str, 
        pub local_filename: &'static str,
    }

    impl FileTarget {
        pub fn new(cache_dir: &'static str, local_filename: &'static str) -> Self {
            FileTarget{cache_dir, local_filename}
        }

        /// Cache filename
        fn filename(&self) -> path::PathBuf {
            path::Path::new(self.cache_dir).join(self.local_filename)
        }
    }

    /// The implementation just uses std::fs file operations.
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

    /// The Task trait represents a piece of work with optional Task dependencies. This is modeled
    /// after the python luigi module.
    pub trait Task {
        /// Target for task output
        fn get_target(&self) -> Box<dyn Target>;

        /// The result of the task. This can use dependent task data as we will ensure that these
        /// have been run.
        fn get_data(&self) -> io::Result<String>;

        /// Dependencies, stored in a HashMap. These will be generated using the run method.
        fn get_dep_tasks(&self) -> HashMap::<&'static str, Box<dyn Task>> {
            HashMap::new()
        }

        /// Dependent task targets.
        fn get_dep_targets(&self) -> HashMap::<&'static str, Box<dyn Target>> {
            let mut result = HashMap::<&'static str, Box<dyn Target>>::new();
            for (k, task) in self.get_dep_tasks() {
                result.insert(k, task.get_target());
            }
            result
        }

        /// This method recursively generates dependent data, and then calls get_data for the Task.
        fn run(&self) -> io::Result<()> {
            // recursively run dependent tasks
            for (_, dep) in self.get_dep_tasks() {
                dep.run()?;
            }
            let target = self.get_target();
            if !target.exists() {
                let data = self.get_data()?;
                target.write(data.as_str())?;
                return Ok(());
            }
            Ok(())
        }

        /// Non-recursively delete dependencies, i.e., delete task outputs for dependent tasks
        fn delete_deps(&self) -> io::Result<()> {
            for (_, dep) in self.get_dep_targets() {
                dep.delete()?;
            }
            Ok(())
        }

        /// Recursively delete dependencies, i.e., delete task outputs for dependent tasks and
        /// their dependencies as well.
        fn recursively_delete_deps(&self) -> io::Result<()> {
            for (_, dep) in self.get_dep_tasks() {
                dep.get_target().delete()?;
                dep.delete_deps()?;
            }
            Ok(())
        }
    }
}


#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io};

    use crate::tasks::{FileTarget, Target, Task};

    #[test]
    fn file_target() {
        let ft = FileTarget::new( "/tmp",  "test_target.txt");
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
                Box::new(FileTarget::new( "/tmp",  "test_task_target.txt"))
            }

            fn get_data(&self) -> io::Result<String> {
                Ok(String::from("some data"))
            }
        }

        let task = FileTask{};
        let target = task.get_target();
        // test with no starting data
        target.delete().unwrap();
        // generate the data
        task.run().unwrap();
        assert_eq!(target.read().unwrap(), String::from("some data"));
        // test with cached starting data
        assert_eq!(target.read().unwrap(), String::from("some data"));
    }

    #[test]
    fn dependent_file_task() {
        struct Dep1 {}
        impl Task for Dep1 {
            fn get_target(&self) -> Box<dyn Target> {
                Box::new(FileTarget{cache_dir: "/tmp", local_filename: "test_task_target_dep1.txt"})
            }

            fn get_data(&self) -> io::Result<String> {
                Ok(String::from("dep1 data"))
            }
        }

        struct Dep2 {}
        impl Task for Dep2 {
            fn get_target(&self) -> Box<dyn Target> {
                Box::new(FileTarget{cache_dir: "/tmp", local_filename: "test_task_target_dep2.txt"})
            }

            fn get_data(&self) -> io::Result<String> {
                Ok(String::from("dep2 data"))
            }
        }

        struct FinalTask {}
        impl Task for FinalTask {
            fn get_target(&self) -> Box<dyn Target> {
                Box::new(FileTarget{cache_dir: "/tmp", local_filename: "test_task_target_depfinal.txt"})
            }

            fn get_dep_tasks(&self) -> HashMap::<&'static str, Box<dyn Task>> {
                let mut result = HashMap::<&'static str, Box<dyn Task>>::new();
                result.insert("dep1", Box::new(Dep1{}));
                result.insert("dep2", Box::new(Dep2{}));
                result
            }

            fn get_data(&self) -> io::Result<String> {
                let dep_targets = self.get_dep_targets();
                let s1 = dep_targets.get("dep1").unwrap().read()?;
                println!("s1={}", s1);
                let s2 = dep_targets.get("dep2").unwrap().read()?;
                println!("s2={}", s2);
                Ok([s1, s2].join(" - "))
            }
        }

        let task = FinalTask{};
        let requires = task.get_dep_tasks();
        task.recursively_delete_deps().unwrap();
        task.run().unwrap();
        assert_eq!(requires.get("dep1").unwrap().get_data().unwrap(), String::from("dep1 data"));
        assert_eq!(requires.get("dep2").unwrap().get_data().unwrap(), String::from("dep2 data"));
        assert_eq!(task.get_data().unwrap(), String::from("dep1 data - dep2 data"));
    }
}
