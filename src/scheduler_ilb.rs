/// Module to handle task schdeuling. Represent a taks as a DAG and run tasks from the leafs
/// through to the head of the tree. This should allow different forms of running including
/// local, local parallel, and distributed (e.g., to a compute cluster).
pub mod scheduler {
    use std::{
        collections::{HashMap, HashSet},
        fmt,
    };

    use crate::tasks::Task;
    use anyhow::Result;
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
    use uuid::Uuid;

    /// Node data for a DAG including an identifier, a task, parent and children ids,
    /// and done status
    pub struct Node {
        pub id: Uuid,
        pub task: Box<dyn Task>,
        pub is_done: bool,
        pub parent: Option<Uuid>,
        pub children: HashSet<Uuid>,
    }

    /// Tasks don't implemnet Debug so just print their names
    impl fmt::Debug for Node {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Node")
                .field("id", &self.id)
                .field("task", &self.task.get_name())
                .field("is_done", &self.is_done)
                .field("parent", &self.parent)
                .field("children", &self.children)
                .finish()
        }
    }

    /// ChildData is a struct that contains a task as well as its ID and parent ID
    struct ChildData {
        id: Uuid,
        task: Box<dyn Task>,
        parent: Uuid,
    }

    /// NodeWithChildren contains a node and a vec of dependencies, with enough information to
    /// add the dependencies to the tree later
    struct NodeWithChildren {
        node: Node,
        children: Vec<ChildData>,
    }

    pub enum RunStyle {
        LOCAL,
        PARALLEL,
        // CLUSTER
    }

    /// DAG represents a directed acylic graph corresponding to the logical structure of a task with dependencies.
    /// It's currently implemented as an arena (a vec of nodes where nodes specify dependencies), with UUIDs as
    /// node identifiers.
    #[derive(Debug)]
    pub struct DAG {
        nodes: HashMap<Uuid, Node>,
    }

    impl DAG {
        // Construct a DAG given a Task object
        pub fn new(head_task: Box<dyn Task>) -> Result<Self> {
            let mut to_process = Vec::new();
            let mut processed = HashMap::new();
            let node_data = DAG::make_node(head_task, None, Uuid::new_v4())?;
            processed.insert(node_data.node.id, node_data.node);
            to_process.extend(node_data.children);

            while let Some(child_data) = to_process.pop() {
                let node_data =
                    DAG::make_node(child_data.task, Some(child_data.parent), child_data.id)?;
                processed.insert(node_data.node.id, node_data.node);
                to_process.extend(node_data.children);
            }

            Ok(Self { nodes: processed })
        }

        // Run all tasks in the DAG according to run_style (e.g., local or multi-threaded parallel)
        pub fn run(&mut self, run_style: &RunStyle) -> Result<()> {
            let mut finished = self
                .nodes
                .values()
                .filter(|&node| node.is_done)
                .map(|node| node.id)
                .collect::<HashSet<_>>();
            let mut not_finished = self
                .nodes
                .values()
                .filter(|&node| !node.is_done)
                .map(|node| node.id)
                .collect::<HashSet<_>>();

            while !&not_finished.is_empty() {
                let candidate_ids = self.get_run_candidates(&not_finished);
                match run_style {
                    RunStyle::LOCAL => {
                        candidate_ids.clone().into_iter().for_each(|id| {
                            if let Some(node) = self.nodes.get(&id) {
                                let _ = node.task.run_no_deps().is_ok();
                            }
                        });
                    }
                    RunStyle::PARALLEL => {
                        candidate_ids.clone().into_par_iter().for_each(|id| {
                            if let Some(node) = self.nodes.get(&id) {
                                let _ = node.task.run_no_deps().is_ok();
                            }
                        });
                    }
                };
                for id in candidate_ids {
                    if let Some(node) = self.nodes.get_mut(&id) {
                        node.is_done = true;
                        finished.insert(id);
                        not_finished.remove(&id);
                    }
                }
            }
            Ok(())
        }

        // Delete all target data
        pub fn delete_all(&mut self) -> Result<()> {
            for node in &mut self.nodes.values_mut() {
                node.task.delete_data()?;
                node.is_done = false;
            }
            Ok(())
        }

        // return run candidates: nodes that are not already done and where the children are all done
        // (i.e., the dependencies are all satisfied)
        fn get_run_candidates(&self, not_finished: &HashSet<Uuid>) -> HashSet<Uuid> {
            let mut candidates = HashSet::new();
            for id in not_finished {
                if let Some(node) = self.nodes.get(id) {
                    if !node.is_done && node.children.intersection(not_finished).next().is_none() {
                        candidates.insert(*id);
                    }
                }
            }
            candidates
        }

        /// Make a node and a collection of children with enough information to connect them to the DAG
        fn make_node(
            task: Box<dyn Task>,
            parent_id: Option<Uuid>,
            node_id: Uuid,
        ) -> Result<NodeWithChildren> {
            let is_done = task.get_target()?.exists();
            let dep_tasks = task.get_dep_tasks();
            let child_tasks = dep_tasks.into_values().collect::<Vec<_>>();
            let mut children = Vec::new();
            for child in child_tasks {
                children.push(ChildData {
                    id: Uuid::new_v4(),
                    task: child,
                    parent: node_id,
                });
            }
            let node = Node {
                id: node_id,
                task,
                is_done,
                parent: parent_id,
                children: children.iter().map(|c| c.id).collect::<HashSet<_>>(),
            };
            Ok(NodeWithChildren { node, children })
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;

        use crate::{
            scheduler::DAG,
            tasks::{FileTarget, Target, Task},
        };
        use anyhow::Result;

        #[derive(Debug)]
        struct Dep1 {}
        impl Task for Dep1 {
            fn get_name(&self) -> String {
                "Dep1".to_string()
            }

            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_dag_target_dep1.txt".to_string(),
                }))
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                Ok("dep1 data".as_bytes().to_vec())
            }
        }

        #[derive(Debug)]
        struct Dep2 {}
        impl Task for Dep2 {
            fn get_name(&self) -> String {
                "Dep2".to_string()
            }

            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_dag_target_dep2.txt".to_string(),
                }))
            }

            fn get_dep_tasks(&self) -> HashMap<String, Box<dyn Task>> {
                let mut result = HashMap::<String, Box<dyn Task>>::new();
                result.insert("dep3".to_string(), Box::new(Dep3 {}));
                result
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                let dep_targets = self
                    .get_dep_targets()
                    .expect("Couldn't get dependent targets");
                let mut s1 = dep_targets.get("dep3").unwrap().read()?;
                s1.extend(" - dep2".as_bytes());
                Ok(s1)
            }
        }

        #[derive(Debug)]
        struct Dep3 {}
        impl Task for Dep3 {
            fn get_name(&self) -> String {
                "Dep3".to_string()
            }

            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget {
                    cache_dir: "/tmp".to_string(),
                    local_filename: "test_dag_target_dep3.txt".to_string(),
                }))
            }

            fn get_data(&self) -> Result<Vec<u8>> {
                Ok("dep3 data".as_bytes().to_vec())
            }
        }

        #[derive(Debug)]
        struct FinalTask {}
        impl Task for FinalTask {
            fn get_name(&self) -> String {
                "FinalTask".to_string()
            }

            fn get_target(&self) -> Result<Box<dyn Target>> {
                Ok(Box::new(FileTarget::new(
                    "/tmp",
                    "test_dag_target_depfinal.txt",
                )))
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

        #[test]
        fn construct_scheduler() {
            let task: Box<dyn Task> = Box::new(FinalTask {});
            task.recursively_delete_data()
                .expect("Failed to delete task and dependent task data");
            let dag = DAG::new(task).expect("Failed to construct DAG");
            let any_done = dag.nodes.values().any(|node| node.is_done);
            assert!(!any_done);

            let task: Box<dyn Task> = Box::new(FinalTask {});
            task.run().expect("task failed to run");
            let dag = DAG::new(task).expect("Failed to construct DAG");
            let all_done = dag.nodes.values().all(|node| node.is_done);
            assert!(all_done);
        }

        #[test]
        fn local_run() {
            let task: Box<dyn Task> = Box::new(FinalTask {});
            task.recursively_delete_data()
                .expect("Failed to delete task and dependent task data");
            let mut dag = DAG::new(task).expect("Failed to construct DAG");

            let any_done = dag.nodes.values().any(|node| node.is_done);
            assert!(!any_done);

            dag.run(&crate::scheduler::RunStyle::LOCAL)
                .expect("Failed to run the DAG");

            let all_done = dag.nodes.values().all(|node| node.is_done);
            assert!(all_done);
        }

        #[test]
        fn parallel_run() {
            let task: Box<dyn Task> = Box::new(FinalTask {});
            task.recursively_delete_data()
                .expect("Failed to delete task and dependent task data");
            let mut dag = DAG::new(task).expect("Failed to construct DAG");

            let any_done = dag.nodes.values().any(|node| node.is_done);
            assert!(!any_done);

            dag.run(&crate::scheduler::RunStyle::PARALLEL)
                .expect("Failed to run the DAG");

            let all_done = dag.nodes.values().all(|node| node.is_done);
            assert!(all_done);
        }

        #[test]
        fn delete_all() {
            let task: Box<dyn Task> = Box::new(FinalTask {});
            let mut dag = DAG::new(task).expect("Failed to construct DAG");
            dag.run(&crate::scheduler::RunStyle::LOCAL)
                .expect("Failed to run the DAG");
            let all_done = dag.nodes.values().all(|node| node.is_done);
            assert!(all_done);

            dag.delete_all().expect("delete_all failed");
            let any_done = dag.nodes.values().any(|node| node.is_done);
            assert!(!any_done);
        }
    }
}
