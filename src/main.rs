use scylla::SessionBuilder;
use scylla::Session;
use scylla::IntoTypedRows;
use scylla::query::Query;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Bound;

fn get_upper_bound_option(key_prefix: &[u8]) -> Option<Vec<u8>> {
    let len = key_prefix.len();
    for i in (0..len).rev() {
        let val = key_prefix[i];
        if val < u8::MAX {
            let mut upper_bound = key_prefix[0..i + 1].to_vec();
            upper_bound[i] += 1;
            return Some(upper_bound);
        }
    }
    None
}

fn get_upper_bound(key_prefix: &[u8]) -> Bound<Vec<u8>> {
    match get_upper_bound_option(key_prefix) {
        None => Bound::Unbounded,
        Some(upper_bound) => Bound::Excluded(upper_bound),
    }
}

pub fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let upper_bound = get_upper_bound(&key_prefix);
    (Bound::Included(key_prefix), upper_bound)
}

async fn find_key_values_by_prefix(session: &Session, key_prefix: Vec<u8>) -> Vec<(Vec<u8>,Vec<u8>)> {
    let len = key_prefix.len();
    let rows = match get_upper_bound_option(&key_prefix) {
        None => {
            let values = (key_prefix,);
            let query = "SELECT k,v FROM kv.pairs WHERE dummy = 0 AND k >= ? ALLOW FILTERING";
            session.query(query, values).await.unwrap()
        }
        Some(upper_bound) => {
            let values = (key_prefix, upper_bound);
            let query = "SELECT k,v FROM kv.pairs WHERE dummy = 0 AND k >= ? AND k < ? ALLOW FILTERING";
            session.query(query, values).await.unwrap()
        }
    };
    let mut key_values = Vec::new();
    if let Some(rows) = rows.rows {
        for row in rows.into_typed::<(Vec<u8>,Vec<u8>)>() {
            let key = row.unwrap();
            let short_key = key.0[len..].to_vec();
            key_values.push((short_key, key.1));
        }
    }
    key_values
}

#[derive(Clone, Debug)]
pub enum WriteOperation {
    Delete {
        key: Vec<u8>,
    },
    DeletePrefix {
        key_prefix: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

#[derive(Clone, Debug)]
pub struct Batch {
    pub operations: Vec<WriteOperation>,
}

fn print_batch(batch: &Batch) {
    println!("batch, n_operation={}", batch.operations.len());
    let mut pos = 0;
    for operation in &batch.operations {
        match operation {
            WriteOperation::Put { key, value } => {
                println!("{}: Put key={:?} value={:?}", pos, key, value);
            }
            WriteOperation::Delete { key } => {
                println!("{}: Delete key={:?}", pos, key);
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                println!("{}: DeletePrefix key_prefix={:?}", pos, key_prefix);
            }
        }
        pos += 1;
    }
}

fn detect_collision(batch: &Batch) {
    let mut key_puts = BTreeSet::new();
    let mut key_deletes = BTreeSet::new();
    let mut key_prefix_deletes = BTreeSet::new();
    for operation in &batch.operations {
        match operation {
            WriteOperation::Put { key, value: _ } => {
                key_puts.insert(key.clone());
            }
            WriteOperation::Delete { key } => {
                key_deletes.insert(key.clone());
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                key_prefix_deletes.insert(key_prefix.clone());
            }
        }
    }
    println!("|key_puts|={}", key_puts.len());
    println!("|key_deletes|={}", key_deletes.len());
    println!("|key_prefix_deletes|={}", key_prefix_deletes.len());
    let intersection: BTreeSet<_> = key_puts.intersection(&key_deletes).cloned().collect();
    println!("|key_puts int key_deletes|={}", intersection.len());
    for key_prefix in key_prefix_deletes {
        let key_list = key_puts
            .range(get_interval(key_prefix.clone()))
            .map(|x| x.to_vec())
            .collect::<Vec<_>>();
        println!("|key_prefix|={} |key_list|={}", key_prefix.len(), key_list.len());
    }
}



async fn write_batch_internal(
    session: &Session,
    operations: Vec<WriteOperation>,
) {
    let mut batch_query = scylla::statement::batch::Batch::new(scylla::frame::request::batch::BatchType::Logged);
    let mut batch_values = Vec::new();
    for ent in operations {
        let (query, values) = match ent {
            WriteOperation::Put { key, value } => {
                let query = "INSERT INTO kv.pairs (dummy, k, v) VALUES (0, ?, ?)";
                let values = vec![key, value];
                (query, values)
            }
            WriteOperation::Delete { key } => {
                let query = "DELETE FROM kv.pairs WHERE dummy = 0 AND k = ?";
                let values = vec![key];
                (query, values)
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                match get_upper_bound_option(&key_prefix) {
                    None => {
                        let values = vec![key_prefix];
                        let query = "DELETE FROM kv.pairs WHERE dummy = 0 AND k >= ?";
                        (query, values)
                    }
                    Some(upper_bound) => {
                        let values = vec![key_prefix, upper_bound];
                        let query = "DELETE FROM kv.pairs WHERE dummy = 0 AND k >= ? AND k < ?";
                        (query, values)
                    }
                }
            }
        };
        batch_values.push(values);
        let query = Query::new(query);
        batch_query.append_statement(query);
    }
    session.batch(&batch_query, batch_values).await.unwrap();
}

async fn create_test_session() -> Session {
    // Create a session builder and specify the ScyllaDB contact points
    let session_builder = SessionBuilder::new()
        .known_node("localhost:9042"); // Replace with your ScyllaDB contact point

    // Build the session
    let session = session_builder.build().await.unwrap();

    // Create a keyspace if it doesn't exist
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
            &[],
        )
        .await.unwrap();

    // Dropping the table
    session
        .query("DROP TABLE IF EXISTS kv.pairs;", &[])
        .await.unwrap();

    // Create a table if it doesn't exist
    session
        .query(
            "CREATE TABLE IF NOT EXISTS kv.pairs (dummy int, k blob, v blob, primary key (dummy, k))",
            &[],
        )
        .await.unwrap();
    session
}

fn update_via_batch(kv_state: &mut BTreeMap<Vec<u8>,Vec<u8>>, batch: &Batch) {
    for operation in &batch.operations {
        match operation {
            WriteOperation::Put { key, value } => {
                kv_state.insert(key.to_vec(), value.to_vec());
            }
            WriteOperation::Delete { key } => {
                kv_state.remove(key);
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                let key_list = kv_state
                    .range(get_interval(key_prefix.clone()))
                    .map(|x| x.0.to_vec())
                    .collect::<Vec<_>>();
                for key in key_list {
                    kv_state.remove(&key);
                }
            }
        }
    }
}

fn get_n_operation(line: String) -> usize {
    let parts : Vec<String> = line.split('=').map(|x| x.to_string()).collect();
    if parts.len() != 2 {
        panic!("Wrong size of the entries");
    }
    let n_operation = parts[1].parse::<usize>().unwrap();
    n_operation
}

fn read_vec(input: String) -> Vec<u8> {
    let elements = input
        .split(',')
        .map(|s| s.trim().parse::<u8>())
        .collect::<Result<Vec<u8>, _>>();
    elements.expect("Vector of Vec<u8>")
}

fn read_operation(i_operation: usize, line: String) -> WriteOperation {
    let parts : Vec<String> = line.clone().split(':').map(|x| x.to_string()).collect();
    let j_operation = parts[0].parse::<usize>().unwrap();
    if i_operation != j_operation {
        panic!("Wrong index");
    }
    let parts : Vec<String> = line.clone().split(" Put key=[").map(|x| x.to_string()).collect();
    if parts.len() == 2 {
        let parts_b: Vec<String> = parts[1].clone().split("] |value|=").map(|x| x.to_string()).collect();
        if parts_b.len() != 2 {
            panic!("Wrong length of parts_b");
        }
        let key = read_vec(parts_b[0].clone());
        let value = vec![0];
        return WriteOperation::Put { key, value };
    }
    let parts : Vec<String> = line.clone().split(" Delete key=[").map(|x| x.to_string()).collect();
    if parts.len() == 2 {
        let mut key_str = parts[1].clone();
        let len_str = key_str.len();
        key_str.truncate(len_str - 1);
        let key = read_vec(key_str);
        return WriteOperation::Delete { key };
    }
    let parts : Vec<String> = line.clone().split(" DeletePrefix key_prefix=[").map(|x| x.to_string()).collect();
    if parts.len() == 2 {
        let mut key_str = parts[1].clone();
        let len_str = key_str.len();
        key_str.truncate(len_str - 1);
        let key_prefix = read_vec(key_str);
        return WriteOperation::DeletePrefix { key_prefix };
    }
    panic!("Failed to find a relevant entry");
}

fn get_first_entry(operation: &WriteOperation) -> u8 {
    match operation {
        WriteOperation::Put { key, value: _ } => {
            key[0]
        }
        WriteOperation::Delete { key } => {
            key[0]
        }
        WriteOperation::DeletePrefix { key_prefix } => {
            key_prefix[0]
        }
    }
}

fn update_firsts(first_bytes: &mut BTreeSet<u8>, batch: &Batch) {
    for operation in &batch.operations {
        first_bytes.insert(get_first_entry(operation));
    }
}

#[tokio::main]
async fn main() {
    let session = create_test_session().await;
    //
    let mut arguments = Vec::new();
    for argument in std::env::args() {
        arguments.push(argument);
    }
    println!("arguments={:?}", arguments);
    let n_arg = arguments.len();
    println!("n_arg={}", n_arg);
    if n_arg != 2 {
        println!("test_scylla_db_batch_sequence [FileI]");
        std::process::exit(1)
    }
    let file = arguments[1].clone();
    println!("file={}", file);
    let file = File::open(file).expect("A file");
    let reader = BufReader::new(file);
    //
    let mut lines_a = Vec::new();
    for pre_line in reader.lines() {
        let line = pre_line.expect("line");
        lines_a.push(line);
    }
    let n_line = lines_a.len();
    //
    let mut batches = Vec::new();
    let mut i_batch = 0;
    for i_line in 0..n_line {
        let e_line = lines_a[i_line].clone();
        if e_line.starts_with("write_batch n_operation=") {
            let n_operation = get_n_operation(e_line);
            println!("i_batch={} n_operation={}", i_batch, n_operation);
            let mut operations = Vec::new();
            for i_operation in 0..n_operation {
                let line = lines_a[i_line + 1 + i_operation].clone();
                println!("i_operation={} line={}", i_operation, line);
                let operation = read_operation(i_operation, line);
                operations.push(operation)
            }
            let batch = Batch { operations };
            batches.push(batch);
            i_batch += 1;
        }
    }
    let n_batches = batches.len();
    //
    // Now looping over the batches
    //
    let mut kv_state = BTreeMap::new();
    let mut first_bytes = BTreeSet::new();
    let mut pos = 0;
    for batch in batches {
        update_firsts(&mut first_bytes, &batch);
        update_via_batch(&mut kv_state, &batch);
        write_batch_internal(&session, batch.operations.clone()).await;
        let mut kv_state_read = BTreeMap::new();
        for first_byte in &first_bytes {
            let key_prefix = vec![first_byte.clone()];
            let key_values = find_key_values_by_prefix(&session, key_prefix).await;
            for (key,value) in key_values {
                let mut big_key = vec![first_byte.clone()];
                big_key.extend(key);
                kv_state_read.insert(big_key, value);
            }
        }
        if kv_state_read != kv_state {
            println!("              ---------------------");
            println!("Inconsistency at pos={} n_batches={}", pos, n_batches);
            print_batch(&batch);
            detect_collision(&batch);
            panic!("Incoherence between the database and the current state");
        }
        pos += 1;
    }
}
