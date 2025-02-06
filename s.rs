use petgraph::graph::{Graph, NodeIndex};
use petgraph::Undirected;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Instant;
use std::{env, process};

mod node;
use node::*;

fn read_graph(input_file: &str) -> (Graph<i32, i32, Undirected>, usize, usize) {
    let input_buffer = std::fs::read_to_string(input_file).expect("Unable to open input file");
    let input_buffer = input_buffer.as_str();
    let mut lines = input_buffer.lines();
    let lines_ref = &mut lines;
    let nodes = lines_ref.next().unwrap();
    let num_nodes = u32::from_str(nodes).unwrap() as usize;
    
    let mut graph: Graph<i32, i32, Undirected> = Graph::default();
    let mut edges_vec = vec![];
    let mut num_edges = 0;
    for line in lines {
        num_edges += 1;
        let tuple_vec: Vec<&str> = line
            .trim_matches(|p| p == '(' || p == ')')
            .split(',')
            .collect();
        let tuple_vec: Vec<i32> = tuple_vec
            .iter()
            .map(|s| i32::from_str(s.trim()).unwrap())
            .collect::<Vec<i32>>();
        let tuple = (tuple_vec[0] as u32, tuple_vec[1] as u32, tuple_vec[2]);
        edges_vec.push(tuple);
    }
    graph.extend_with_edges(&edges_vec[..]);
    (graph, num_nodes, num_edges)
}

fn run_experiment(input_file: &str) -> (usize, usize, f64, usize) {
    let (graph, num_nodes, num_edges) = read_graph(input_file);
    let graph = Arc::new(RwLock::new(graph));
    let orig_mapping: Arc<RwLock<HashMap<NodeIndex, RwLock<Node>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let stop = Arc::new(RwLock::new(AtomicBool::new(false)));
    let message_count = Arc::new(AtomicUsize::new(0));

    for node_index in graph.read().unwrap().node_indices() {
        let node = Node::new(Arc::clone(&graph), node_index, Arc::clone(&stop), Arc::clone(&message_count));
        let mut mapping = orig_mapping.write().unwrap();
        mapping.insert(node_index, RwLock::new(node));
    }

    let mut sender_mapping: HashMap<NodeIndex, Sender<Message>> = HashMap::new();
    let mut receiver_mapping: HashMap<NodeIndex, Receiver<Message>> = HashMap::new();
    for node_index in graph.read().unwrap().node_indices() {
        let (sender, receiver) = mpsc::channel();
        sender_mapping.insert(node_index, sender);
        receiver_mapping.insert(node_index, receiver);
    }

    let start_time = Instant::now();

    let mut handles = vec![];
    for node_index in graph.read().unwrap().node_indices() {
        let move_mapping = Arc::clone(&orig_mapping);
        let sender_mapping = sender_mapping.clone();
        let receiver = receiver_mapping.remove(&node_index).unwrap();
        let handle = thread::Builder::new()
            .name(node_index.index().to_string())
            .spawn(move || {
                let receiver = receiver;
                let sender_mapping = sender_mapping;
                let mapping = move_mapping.read().unwrap();
                let node = mapping.get(&node_index).unwrap();
                let mut node = node.write().unwrap();
                node.initialize(&sender_mapping);
                loop {
                    if *node.stop.read().unwrap().get_mut() {
                        break;
                    }
                    let recv = receiver.try_recv();
                    let msg = match recv {
                        Err(TryRecvError::Empty) => continue,
                        Err(TryRecvError::Disconnected) => break,
                        Ok(message) => message,
                    };
                    match msg {
                        Message::Connect(_, sender_index) => node.process_connect(msg, &sender_mapping),
                        Message::Initiate(_, _, _, sender_index) => node.process_initiate(msg, &sender_mapping),
                        Message::Test(_, _, sender_index) => node.process_test(msg, &sender_mapping),
                        Message::Accept(sender_index) => node.process_accept(msg, &sender_mapping),
                        Message::Reject(sender_index) => node.process_reject(msg, &sender_mapping),
                        Message::Report(_, sender_index) => node.process_report(msg, &sender_mapping),
                        Message::ChangeRoot(sender_index) => node.process_change_root(msg, &sender_mapping),
                    }
                }
                (node_index, node.status.clone())
            });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed_time = start_time.elapsed().as_secs_f64();
    let total_messages = message_count.load(std::sync::atomic::Ordering::Relaxed);

    (num_nodes, num_edges, elapsed_time, total_messages)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: {} <input-files...> <output-file>", args[0]);
        process::exit(1);
    }

    let output_file = &args[args.len() - 1];
    let mut results = Vec::new();

    for input_file in &args[1..args.len() - 1] {
        println!("Running experiment for {}", input_file);
        let (num_nodes, num_edges, elapsed_time, total_messages) = run_experiment(input_file);
        results.push((input_file, num_nodes, num_edges, elapsed_time, total_messages));
    }

    let mut file = File::create(output_file).expect("Unable to create output file");
    writeln!(file, "Input File,Nodes,Edges,Time (s),Messages").unwrap();
    for (input_file, num_nodes, num_edges, elapsed_time, total_messages) in results {
        writeln!(
            file,
            "{},{},{},{:.6},{}", 
            input_file, num_nodes, num_edges, elapsed_time, total_messages
        ).unwrap();
    }

    println!("Experiments completed. Results saved to {}", output_file);
}