pub mod tasks {
    use anyhow::Result;
    use log::info;
    use std::{collections::HashMap, fmt, fs, path};

    /// The Target trait represents cached data. The data is stored as a byte slice, and can be used
    /// with serde for serialization of other types.
    pub trait Target {
        /// Read a Vec of bytes to target destination
        fn read(&self) -> Result<Vec<u8>>;

        /// Read from target destination to a Vec of bytes
        fn write(&self, s: &[u8]) -> Result<()>;

        /// Delete the target destination
        fn delete(&self) -> Result<()>;

        /// Does the cache exist?
        fn exists(&self) -> Result<bool>;
    }

    /// Target that does nothing, useful for wrapper tasks that exist solely to
    /// run dependencies
    #[derive(Debug, PartialEq, Eq)]
    pub struct NullTarget {}

    impl Target for NullTarget {
        /// Return an empty byte vector
        fn read(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        /// No-op
        fn write(&self, _: &[u8]) -> Result<()> {
            Ok(())
        }

        /// No-op
        fn delete(&self) -> Result<()> {
            Ok(())
        }

        /// Exists is false. This means that the run method on a task with a
        /// NullTarget will always call run on the dependent tasks
        fn exists(&self) -> Result<bool> {
            Ok(false)
        }
    }

    /// FileTarget implements Target, using a file as the cache destination.
    #[derive(Debug, PartialEq, Eq)]
    pub struct FileTarget {
        pub cache_dir: String,
        pub local_filename: String,
    }

    impl FileTarget {
        pub fn new(cache_dir: &str, local_filename: &str) -> Self {
            FileTarget {
                cache_dir: cache_dir.to_string(),
                local_filename: local_filename.to_string(),
            }
        }

        /// Cache full filename
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

        fn exists(&self) -> Result<bool> {
            Ok(self.filename().is_file())
        }

        fn delete(&self) -> Result<()> {
            if self.exists()? {
                Ok(fs::remove_file(self.filename())?)
            } else {
                Ok(())
            }
        }
    }

    /// DatedFileTarget uses dated files (date appended to the front of the
    /// filename). This implementation uses daily, not intraday dates
    #[derive(Debug, PartialEq, Eq)]
    pub struct DatedFileTarget {
        file_target: FileTarget,
        date: chrono::NaiveDate,
    }

    impl DatedFileTarget {
        pub fn new(cache_dir: &str, local_filename: &str, date: chrono::NaiveDate) -> Self {
            let file_target = FileTarget {
                cache_dir: cache_dir.to_string(),
                local_filename: local_filename.to_string(),
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

        fn exists(&self) -> Result<bool> {
            Ok(self.filename().is_file())
        }

        fn delete(&self) -> Result<()> {
            if self.exists()? {
                Ok(fs::remove_file(self.filename())?)
            } else {
                Ok(())
            }
        }
    }

    /// The Task trait represents a piece of work with optional Task
    /// dependencies. This is modeled after the python luigi module.
    ///
    /// The pattern is that you put any dependencies in get_dep_tasks,
    /// set the target in get_target(), and fill in the compute_output() method,
    /// where you can assume that dependencies' tasks are complete.
    /// Then to run the task you just invoke the run() method. After run()
    /// is invoked you can get the output using the self.get_data() method.
    pub trait Task: fmt::Debug + Sync + Send {
        /// Target for task output
        fn get_target(&self) -> Result<Box<dyn Target>>;

        /// The result of the task. This can use dependent task data as we will
        /// ensure that these have been run. Don't call this directly unless you
        /// want to bypass the cache system.
        fn compute_output(&self) -> Result<Vec<u8>>;

        /// Return the data from the target cache. If the target cache does not
        /// exist this will fail
        fn get_data(&self) -> Result<Vec<u8>> {
            self.get_target()?.read()
        }

        /// Run the task (i.e., recursively run dependent tasks) and return the
        /// data from the target cache
        fn run_and_get_data(&self) -> Result<Vec<u8>> {
            self.run()?;
            self.get_data()
        }

        /// Optional task name
        fn get_name(&self) -> String {
            "Unimplemented".to_string()
        }

        /// Dependencies, stored in a HashMap. These will be generated using the
        /// run method. This is like the requires() method in luigi.
        fn get_dep_tasks(&self) -> Result<HashMap<String, Box<dyn Task>>> {
            Ok(HashMap::new())
        }

        /// Dependent task targets
        fn get_dep_targets(&self) -> Result<HashMap<String, Box<dyn Target>>> {
            let mut result = HashMap::<String, Box<dyn Target>>::new();
            for (k, task) in self.get_dep_tasks()? {
                result.insert(k, task.get_target()?);
            }
            Ok(result)
        }

        /// Validate the task
        fn validate(&self, _data: &[u8]) -> Result<()> {
            info!("{}: invoking validate", self.get_name());
            Ok(())
        }

        /// This method recursively generates dependent data, and then calls
        /// get_data for the Task.
        fn run(&self) -> Result<()> {
            info!("{}: invoking run()", self.get_name());
            // recursively run dependent tasks
            for (_, dep) in self.get_dep_tasks()? {
                dep.run()?;
            }
            // run get_data() if the target doesn't exist
            let target = self.get_target()?;
            if !target.exists()? {
                info!(
                    "{}: target does not exist: invoking compute_output()",
                    self.get_name()
                );
                let data = self.compute_output()?;
                self.validate(&data)?;
                target.write(&data)?;
            } else {
                info!("{}: target exists", self.get_name());
            }
            Ok(())
        }

        /// Non-dependent run: just save get_data() to get_target(). This will fail if required
        /// dependencies are not present. For regular use just call run(). This method is used in the
        /// scheduler run method as dependencies are handled in the code there.
        fn run_no_deps(&self) -> Result<()> {
            info!("{}: invoking run_no_deps()", self.get_name());
            let target = self.get_target()?;
            if !target.exists()? {
                info!(
                    "{}: target does not exist: invoking get_data() without running dependencies",
                    self.get_name()
                );
                let data = self.compute_output()?;
                target.write(&data)?;
            }
            Ok(())
        }

        /// Delete target data: this is a convenience method as you can always
        /// just call self.get_target()?.delete()
        fn delete_data(&self) -> Result<()> {
            info!("{}: invoking delete_data()", self.get_name());
            self.get_target()?.delete()?;
            Ok(())
        }

        /// Non-recursively delete dependencies, i.e., delete task outputs for
        /// dependent tasks
        fn delete_deps(&self) -> Result<()> {
            info!("{}: invoking delete_deps()", self.get_name());
            for (_, dep) in self.get_dep_targets()? {
                dep.delete()?;
            }
            Ok(())
        }

        /// Recursively delete dependencies, i.e., delete task outputs for
        /// dependent tasks and their dependencies as well.
        fn recursively_delete_data(&self) -> Result<()> {
            info!("{}: invoking recursively_delete_data()", self.get_name());
            self.delete_data()?;
            for (_, dep) in self.get_dep_tasks()? {
                dep.recursively_delete_data()?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    extern crate serde;
    use anyhow::{anyhow, Result};
    use serde::{Deserialize, Serialize};

    use crate::tasks::{DatedFileTarget, FileTarget, Target, Task};

    #[test]
    fn file_target() {
        let ft = FileTarget::new("/tmp", "test_target.txt");
        ft.delete().unwrap();
        assert!(!ft.exists().expect("exists failed"));
        ft.write("test data".as_bytes()).unwrap();
        assert!(ft.exists().expect("exists failed"));
        assert_eq!(ft.read().unwrap(), "test data".as_bytes().to_vec());
    }

    #[test]
    fn dated_file_target() {
        let ft = DatedFileTarget::new(
            "/tmp",
            "dated_test_target.txt",
            chrono::NaiveDate::from_ymd(2021, 9, 3),
        );
        ft.delete().unwrap();
        assert!(!ft.exists().expect("exists failed"));
        ft.write("test data".as_bytes()).unwrap();
        assert!(ft.exists().expect("exists failed"));
        assert_eq!(ft.read().unwrap(), "test data".as_bytes().to_vec());
    }

    #[test]
    fn file_task() {
        #[derive(Debug)]
        struct FileTask {}
        impl Task for FileTask {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget::new("/tmp", "test_task_target.txt")))
            }

            fn compute_output(&self) -> Result<Vec<u8>> {
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
    fn validation() {
        #[derive(Debug)]
        struct FileTask {
            min_len: usize,
        }
        impl Task for FileTask {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget::new(
                    "/tmp",
                    "test_validation_target.txt",
                )))
            }

            fn compute_output(&self) -> Result<Vec<u8>> {
                Ok("some data".as_bytes().to_vec())
            }

            fn validate(&self, data: &[u8]) -> Result<()> {
                if data.len() < self.min_len {
                    Err(anyhow!("not enough data!"))
                } else {
                    Ok(())
                }
            }
        }

        let task = FileTask { min_len: 1000 };
        let target = task.get_target().expect("Can't get target");
        // test with no starting data
        target.delete().unwrap();
        // generate the data - should fail validation
        assert!(task.run().is_err());
        let task = FileTask { min_len: 1 };
        // generate the data - should pass validation
        assert!(!task.run().is_err());
    }

    #[test]
    fn serde_task() {
        #[derive(Debug)]
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
                    "/tmp",
                    "test_serde_task_target.txt",
                )))
            }

            fn compute_output(&self) -> Result<Vec<u8>> {
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
        #[derive(Debug)]
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
                    "/tmp",
                    "test_serde_struct_task_target.txt",
                )))
            }

            fn compute_output(&self) -> Result<Vec<u8>> {
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
        #[derive(Debug)]
        struct Dep1 {}
        impl Task for Dep1 {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_task_target_dep1.txt".to_string(),
                }))
            }

            fn compute_output(&self) -> Result<Vec<u8>> {
                Ok("dep1 data".as_bytes().to_vec())
            }
        }

        #[derive(Debug)]
        struct Dep2 {}
        impl Task for Dep2 {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_task_target_dep2.txt".to_string(),
                }))
            }

            fn compute_output(&self) -> Result<Vec<u8>> {
                Ok("dep2 data".as_bytes().to_vec())
            }
        }

        #[derive(Debug)]
        struct FinalTask {}
        impl Task for FinalTask {
            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget::new(
                    "/tmp",
                    "test_task_target_depfinal.txt",
                )))
            }

            fn get_dep_tasks(&self) -> Result<HashMap<String, Box<dyn Task>>> {
                let mut result = HashMap::<String, Box<dyn Task>>::new();
                result.insert("dep1".to_string(), Box::new(Dep1 {}));
                result.insert("dep2".to_string(), Box::new(Dep2 {}));
                Ok(result)
            }

            fn compute_output(&self) -> Result<Vec<u8>> {
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
        let requires = task.get_dep_tasks().expect("get_dep_tasks() failed");
        task.recursively_delete_data().unwrap();
        task.run().unwrap();
        assert_eq!(
            requires.get("dep1").unwrap().compute_output().unwrap(),
            "dep1 data".as_bytes().to_vec()
        );
        assert_eq!(
            requires.get("dep2").unwrap().compute_output().unwrap(),
            "dep2 data".as_bytes().to_vec()
        );
        assert_eq!(
            task.compute_output().unwrap(),
            "dep1 data - dep2 data".as_bytes().to_vec()
        );
    }
}

#[cfg(test)]
mod sqlite_tests {
    use anyhow::Result;
    use chrono::NaiveDate;
    use log::info;
    use rusqlite::Connection;
    use serde::{Deserialize, Serialize};

    use crate::tasks::{Target, Task};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Record {
        composer: String,
        title: String,
    }

    #[derive(Debug)]
    struct TestTarget {
        id: u32,
    }

    impl TestTarget {
        fn new(id: u32) -> Result<Self> {
            let conn = TestTarget::get_conn()?;
            conn.execute(
                "create table if not exists program (
             id integer,
             composer text not null,
             title text not null
         )",
                (),
            )?;

            Ok(Self { id })
        }

        fn get_filename() -> String {
            "/tmp/test_target.db".to_string()
        }

        fn get_conn() -> Result<Connection> {
            Ok(Connection::open(&TestTarget::get_filename())?)
        }
    }

    impl Target for TestTarget {
        fn read(&self) -> Result<Vec<u8>> {
            let conn = TestTarget::get_conn()?;
            let mut stmt = conn.prepare(&format!(
                "select composer, title from program where id={}",
                self.id
            ))?;
            let record_iter = stmt.query_map([], |row| {
                Ok(Record {
                    composer: row.get(0)?,
                    title: row.get(1)?,
                })
            })?;
            let mut records: Vec<Record> = Vec::new();
            for record_res in record_iter {
                if let Ok(record) = record_res {
                    records.push(record);
                }
            }
            let bytes: Vec<u8> = serde_json::to_string::<Vec<Record>>(&records)?
                .as_bytes()
                .to_vec();
            Ok(bytes)
        }

        fn write(&self, s: &[u8]) -> Result<()> {
            let records: Vec<Record> = serde_json::from_slice(s)?;
            let conn = TestTarget::get_conn()?;
            for record in records {
                conn.execute(
                    "insert into program (id, composer, title) values (?1, ?2, ?3)",
                    (&self.id, &record.composer, &record.title),
                )?;
            }
            Ok(())
        }

        fn delete(&self) -> Result<()> {
            let conn = TestTarget::get_conn()?;
            conn.execute(&format!("delete from program where id={}", self.id), ())?;
            Ok(())
        }

        fn exists(&self) -> Result<bool> {
            let conn = TestTarget::get_conn()?;
            let mut stmt = conn.prepare(&format!(
                "select count(id) from program where id={}",
                self.id
            ))?;
            let count = stmt
                .query_map((), |row| row.get::<usize, u32>(0))?
                .next()
                .unwrap_or(Ok(0))?;

            Ok(count > 0)
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestTask {
        id: u32,
        data: Vec<Record>,
    }

    impl TestTask {
        fn new(id: u32) -> Self {
            Self {
                id,
                data: vec![
                    Record {
                        composer: "Beethoven".to_string(),
                        title: "Symphony 5".to_string(),
                    },
                    Record {
                        composer: "Brahms".to_string(),
                        title: "Symphony 4".to_string(),
                    },
                ],
            }
        }
    }

    impl Task for TestTask {
        fn get_name(&self) -> String {
            format!("TestTask, id={}", self.id)
        }

        fn get_target(&self) -> Result<Box<dyn Target>> {
            Ok(Box::new(TestTarget::new(self.id)?))
        }

        fn compute_output(&self) -> Result<Vec<u8>> {
            Ok(serde_json::to_string(&self.data)?.as_bytes().to_vec())
        }
    }

    #[test]
    fn test_sqlite() {
        let id = 5;
        let task = TestTask::new(id);
        info!("task.data: {:?}", task.data);
        let target = task.get_target().expect("get_target failed");

        target.delete().expect("target.delete failed");
        assert!(!target.exists().expect("target.exists failed"));
        task.run().expect("task.run failed");
        assert!(target.exists().expect("target.exists failed"));

        let records_from_task: Vec<Record> =
            serde_json::from_slice(&task.compute_output().expect("compute_output failed"))
                .expect("serde_json::from_slice failed");
        // info!("Records from compute_output: {:?}", records_from_task);

        let records_from_target: Vec<Record> =
            serde_json::from_slice(&target.read().expect("target.read failed"))
                .expect("serde_json::from_slice failed");
        // info!("Records from target read: {:?}", records_from_target);
        assert_eq!(records_from_target, records_from_task);
    }

    #[test]
    fn test_duration() {
        let d0 = NaiveDate::from_ymd(2020, 1, 1);
        let d1 = NaiveDate::from_ymd(2020, 1, 3);
        let dur = d1 - d0;
        assert_eq!(dur.num_days(), 2);
    }
}
