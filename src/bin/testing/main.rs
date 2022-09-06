use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tasks::tasks::{Target, Task};

#[derive(Debug, Serialize, Deserialize)]
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

    fn is_verbose(&self) -> bool {
        true
    }

    fn get_target(&self) -> Result<Box<dyn tasks::tasks::Target>> {
        Ok(Box::new(TestTarget::new(self.id)?))
    }

    fn compute_output(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_string(&self.data)?.as_bytes().to_vec())
    }
}

fn main() -> Result<()> {
    let id = 5;
    let task = TestTask::new(id);
    println!("task.data: {:?}", task.data);
    let target = task.get_target()?;

    target.delete()?;
    assert!(!target.exists()?);
    task.run()?;
    assert!(target.exists()?);

    let records_from_task: Vec<Record> = serde_json::from_slice(&task.compute_output()?)?;
    println!("Records from compute_output: {:?}", records_from_task);

    let records_from_target: Vec<Record> = serde_json::from_slice(&target.read()?)?;
    println!("Records from target read: {:?}", records_from_target);

    Ok(())
}
