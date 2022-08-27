pub mod tasks {
    use anyhow::Result;
    use std::{collections::HashMap, fs, path};

    /// The Target trait represents cached data. The data is stored as a byte slice, and can be used
    /// with serde for serialization of other types.
    pub trait Target {
        /// Read data from a cache
        fn read(&self) -> Result<Vec<u8>>;

        /// Write data to a cache
        fn write(&self, s: &[u8]) -> Result<()>;

        /// Delete cache data
        fn delete(&self) -> Result<()>;

        /// Does the cache exist
        fn exists(&self) -> bool;
    }

    /// The FileTarget type implements Target, using a file as the cache destination.
    pub struct FileTarget {
        pub cache_dir: String,
        pub local_filename: String,
    }

    impl FileTarget {
        pub fn new(cache_dir: String, local_filename: String) -> Self {
            FileTarget {
                cache_dir,
                local_filename,
            }
        }

        /// Cache filename
        pub fn filename(&self) -> path::PathBuf {
            path::Path::new(self.cache_dir.as_str()).join(self.local_filename.as_str())
        }
    }

    /// The implementation just uses std::fs file operations.
    impl Target for FileTarget {
        fn read(&self) -> Result<Vec<u8>> {
            Ok(fs::read(&self.filename())?)
        }

        fn write(&self, s: &[u8]) -> Result<()> {
            Ok(fs::write(self.filename(), s)?)
        }

        fn exists(&self) -> bool {
            self.filename().is_file()
        }

        fn delete(&self) -> Result<()> {
            if self.exists() {
                Ok(fs::remove_file(self.filename())?)
            } else {
                Ok(())
            }
        }
    }

    pub struct DatedFileTarget {
        file_target: FileTarget,
        date: chrono::NaiveDate,
    }

    impl DatedFileTarget {
        pub fn new(cache_dir: String, local_filename: String, date: chrono::NaiveDate) -> Self {
            let file_target = FileTarget {
                cache_dir,
                local_filename,
            };
            DatedFileTarget { file_target, date }
        }

        fn filename(&self) -> path::PathBuf {
            let dstr = self.date.format("%Y%m%d").to_string();
            let local_filename = format!("{}_{}", dstr, self.file_target.local_filename);
            path::Path::new(self.file_target.cache_dir.as_str()).join(local_filename)
        }
    }

    // TODO: bad code smell - this implementation is the same as for FileTarget - investigate how to fix
    impl Target for DatedFileTarget {
        fn read(&self) -> Result<Vec<u8>> {
            Ok(fs::read(&self.filename())?)
        }

        fn write(&self, s: &[u8]) -> Result<()> {
            Ok(fs::write(self.filename(), s)?)
        }

        fn exists(&self) -> bool {
            self.filename().is_file()
        }

        fn delete(&self) -> Result<()> {
            if self.exists() {
                Ok(fs::remove_file(self.filename())?)
            } else {
                Ok(())
            }
        }
    }

    /// The Task trait represents a piece of work with optional Task dependencies. This is modeled
    /// after the python luigi module.
    pub trait Task {
        /// Target for task output
        fn get_target(&self) -> Result<Box<dyn Target>>;

        /// The result of the task. This can use dependent task data as we will ensure that these
        /// have been run.
        /// Don't call this directly unless you want to bypass the cache system
        fn get_data(&self) -> Result<Vec<u8>>;

        /// Dependencies, stored in a HashMap. These will be generated using the run method.
        /// This is like the requires() method in luigi.
        fn get_dep_tasks(&self) -> HashMap<String, Box<dyn Task>> {
            HashMap::new()
        }

        /// Dependent task targets.
        fn get_dep_targets(&self) -> Result<HashMap<String, Box<dyn Target>>> {
            let mut result = HashMap::<String, Box<dyn Target>>::new();
            for (k, task) in self.get_dep_tasks() {
                result.insert(k, task.get_target()?);
            }
            Ok(result)
        }

        /// This method recursively generates dependent data, and then calls get_data for the Task.
        fn run(&self) -> Result<()> {
            // recursively run dependent tasks
            for (_, dep) in self.get_dep_tasks() {
                dep.run()?;
            }
            let target = self.get_target()?;
            if !target.exists() {
                let data = self.get_data()?;
                target.write(&data)?;
            }
            Ok(())
        }

        /// Non-recursively delete dependencies, i.e., delete task outputs for dependent tasks
        fn delete_deps(&self) -> Result<()> {
            for (_, dep) in self.get_dep_targets()? {
                dep.delete()?;
            }
            Ok(())
        }

        /// Recursively delete dependencies, i.e., delete task outputs for dependent tasks and
        /// their dependencies as well.
        fn recursively_delete_deps(&self) -> Result<()> {
            for (_, dep) in self.get_dep_tasks() {
                dep.get_target()?.delete()?;
                dep.delete_deps()?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tasks::{DatedFileTarget, FileTarget, Target, Task};
    use std::collections::HashMap;
    extern crate serde;
    use anyhow::Result;
    use serde::{Deserialize, Serialize};

    #[test]
    fn file_target() {
        let ft = FileTarget::new("/tmp".to_string(), "test_target.txt".to_string());
        ft.delete().unwrap();
        assert!(!ft.exists());
        ft.write("test data".as_bytes()).unwrap();
        assert!(ft.exists());
        assert_eq!(ft.read().unwrap(), "test data".as_bytes().to_vec());
    }

    #[test]
    fn dated_file_target() {
        let ft = DatedFileTarget::new(
            "/tmp".to_string(),
            "dated_test_target.txt".to_string(),
            chrono::NaiveDate::from_ymd(2021, 9, 3),
        );
        ft.delete().unwrap();
        assert!(!ft.exists());
        ft.write("test data".as_bytes()).unwrap();
        assert!(ft.exists());
        assert_eq!(ft.read().unwrap(), "test data".as_bytes().to_vec());
    }

    #[test]
    fn file_task() {
        struct FileTask {}
        impl Task for FileTask {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget::new(
                    "/tmp".to_string(),
                    "test_task_target.txt".to_string(),
                )))
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                Ok("some data".as_bytes().to_vec())
            }
        }

        let task = FileTask {};
        let target = task.get_target().expect("Can't get target");
        // test with no starting data
        target.delete().unwrap();
        // generate the data
        task.run().unwrap();
        assert_eq!(target.read().unwrap(), "some data".as_bytes().to_vec());
        // test with cached starting data
        assert_eq!(target.read().unwrap(), "some data".as_bytes().to_vec());
    }

    #[test]
    fn serde_task() {
        struct FileTask {
            value: f64,
        }

        impl FileTask {
            fn get_value(&self) -> f64 {
                let v: f64 = serde_json::from_slice(
                    &self.get_target().expect("Can't get target").read().unwrap(),
                )
                .unwrap();
                v
            }
        }

        impl Task for FileTask {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget::new(
                    "/tmp".to_string(),
                    "test_serde_task_target.txt".to_string(),
                )))
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                let s = serde_json::to_vec(&self.value).unwrap();
                Ok(s)
            }
        }

        let task = FileTask { value: 1.23 };
        let target = task.get_target().expect("Can't get target");
        // test with no starting data
        target.delete().unwrap();
        // generate the data
        task.run().unwrap();
        assert_eq!(task.get_value(), 1.23);
    }

    #[test]
    fn serde_struct_task() {
        // the thing we want to compute and cache
        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
        struct Value {
            a: String,
            b: f64,
        }

        // value is hard-coded here. In a realistic example we'd compute something in get_data()
        struct FileTask {
            value: Value,
        }

        impl FileTask {
            fn get_value(&self) -> Value {
                let v: Value = serde_json::from_slice(
                    &self.get_target().expect("Can't get target").read().unwrap(),
                )
                .unwrap();
                v
            }
        }

        impl Task for FileTask {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget::new(
                    "/tmp".to_string(),
                    "test_serde_struct_task_target.txt".to_string(),
                )))
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                let s = serde_json::to_vec(&self.value).unwrap();
                Ok(s)
            }
        }

        let value = Value {
            a: String::from("a string"),
            b: 1.23,
        };
        let task = FileTask {
            value: value.clone(),
        };
        let target = task.get_target().expect("Can't get target");
        // test with no starting data
        target.delete().unwrap();
        // generate the data
        task.run().unwrap();
        let read_value = task.get_value();
        assert_eq!(value, read_value);
    }

    #[test]
    fn dependent_file_task() {
        struct Dep1 {}
        impl Task for Dep1 {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_task_target_dep1.txt".to_string(),
                }))
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                Ok("dep1 data".as_bytes().to_vec())
            }
        }

        struct Dep2 {}
        impl Task for Dep2 {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_task_target_dep2.txt".to_string(),
                }))
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                Ok("dep2 data".as_bytes().to_vec())
            }
        }

        struct FinalTask {}
        impl Task for FinalTask {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_task_target_depfinal.txt".to_string(),
                }))
            }

            fn get_dep_tasks(&self) -> HashMap<String, Box<dyn Task>> {
                let mut result = HashMap::<String, Box<dyn Task>>::new();
                result.insert("dep1".to_string(), Box::new(Dep1 {}));
                result.insert("dep2".to_string(), Box::new(Dep2 {}));
                result
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                let dep_targets = self
                    .get_dep_targets()
                    .expect("Couldn't get dependent targets");
                let mut s1 = dep_targets.get("dep1").unwrap().read()?;
                s1.extend(" - ".as_bytes());
                s1.extend(dep_targets.get("dep2").unwrap().read()?);
                Ok(s1)
            }
        }

        let task = FinalTask {};
        let requires = task.get_dep_tasks();
        task.recursively_delete_deps().unwrap();
        task.run().unwrap();
        assert_eq!(
            requires.get("dep1").unwrap().get_data().unwrap(),
            "dep1 data".as_bytes().to_vec()
        );
        assert_eq!(
            requires.get("dep2").unwrap().get_data().unwrap(),
            "dep2 data".as_bytes().to_vec()
        );
        assert_eq!(
            task.get_data().unwrap(),
            "dep1 data - dep2 data".as_bytes().to_vec()
        );
    }
}
