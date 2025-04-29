use core_affinity;
use socket2::Protocol;
use syslog_loose::parse_message;
use core::net::SocketAddr;
use std::cell::UnsafeCell;
use std::process::exit;
use crossbeam_channel::{bounded, Sender, Receiver};
use tokio::net::UdpSocket;
use tokio::runtime::Builder;
use std::thread;
use std::io;
use syslog_loose::Variant;

const BUFFER_SIZE: usize = 1024;
const CHANNEL_CAPACITY: usize = 100;

static mut RECEIVED_STATS: Vec<Stats> = Vec::new();
static mut DROPPED_STATS: Vec<Stats> = Vec::new();

#[repr(C)]
#[derive(Default)]
struct Stats {
    count: u64,
    /// We use a padding for the struct to allow each thread to have exclusive access to each WorkerStat
    /// Otherwise, there would be some cpu contention with threads needing to take ownership of the cache lines
    padding: [u64; 15],
}

fn create_socket() -> socket2::Socket {
    let address : SocketAddr = "127.0.0.1:50005".parse().unwrap();

    // Create a SO_REUSEADDR + SO_REUSEPORT listener.
    let socket = socket2::Socket::new(socket2::Domain::IPV4,socket2::Type::DGRAM, Some(Protocol::UDP)).unwrap();

    socket.set_reuse_address(true).unwrap();
    socket.set_reuse_port(true).unwrap();
    socket.set_nonblocking(true).unwrap();
    socket.bind(&address.into()).unwrap();
    socket
}

#[allow(static_mut_refs)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get available CPU core IDs
    let core_ids = core_affinity::get_core_ids().expect("Failed to get core IDs");
    let core_count = core_ids.len();
    println!("Detected {} CPU cores", core_count);

    let shared_mutable_received_stats: UnsafeSlice;
    let shared_mutable_dropped_stats: UnsafeSlice;

    unsafe {
        for _ in 0..core_count / 2 {
            RECEIVED_STATS.push(Stats::default());
            DROPPED_STATS.push(Stats::default());
        }

        println!("Length of received stats: {}", RECEIVED_STATS.len());
        println!("Length of dropped stats: {}", DROPPED_STATS.len());
    
        shared_mutable_received_stats = UnsafeSlice::new(&mut RECEIVED_STATS);
        shared_mutable_dropped_stats = UnsafeSlice::new(&mut DROPPED_STATS);
    }

    ctrlc::set_handler(move || {
        println!("Packets received: {}", shared_mutable_received_stats.sum());
        println!("Packets dropped: {}", shared_mutable_dropped_stats.sum());
        exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    thread::scope(|s| {
        // Create a receiver thread and corresponding worker thread for each core
        for (idx, core_id) in core_ids.into_iter().enumerate() {

            if idx >= core_count / 2 {
                break;
            }
            println!("Setting up threads for core {}", idx);
            
            // Create a SPSC channel for communication between receiver and worker
            let (sender, receiver) = bounded::<Vec<u8>>(CHANNEL_CAPACITY);
            
            // Spawn worker thread first
            let worker_core_id = core_id.clone();
            let _worker_handle = s.spawn(move || {
                // Set thread affinity to the corresponding core
                core_affinity::set_for_current(worker_core_id);
                println!("Worker thread {} affinity set to core {}", idx, worker_core_id.id);
                
                // Create a current_thread runtime for the worker
                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build tokio runtime for worker");
                    
                rt.block_on(async {
                    process_messages(receiver, idx).await;
                });
            });

            // Spawn receiver thread with tokio current_thread runtime
            let _receiver_handle = s.spawn(move || {
                // Set thread affinity to the corresponding core
                core_affinity::set_for_current(core_id);
                println!("Receiver thread {} affinity set to core {}", idx, core_id.id);
                
                // Create a current_thread runtime
                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build tokio runtime");
                
                // Start the UDP receiver in this runtime
                rt.block_on(async {
                    if let Err(e) = receive_udp(create_socket().into(), sender, idx, shared_mutable_received_stats, shared_mutable_dropped_stats).await {
                        println!("UDP receiver {} error: {}", idx, e);
                    }
                });
            });
        }
    });

    Ok(())
}

async fn receive_udp(
    std_socket: std::net::UdpSocket, 
    sender: Sender<Vec<u8>>, 
    thread_idx: usize,
    shared_mutable_received_stats: UnsafeSlice<'_>,
    shared_mutable_dropped_stats: UnsafeSlice<'_>,
) -> Result<(), io::Error> {
    // Convert from std UdpSocket to tokio UdpSocket
    std_socket.set_nonblocking(true)?;
    let socket = UdpSocket::from_std(std_socket)?;
    
    println!("UDP receiver {} listening on port {}", thread_idx, 50005);

    let mut buf = vec![0u8; BUFFER_SIZE];
    
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((size, _addr)) => {
                let data = buf[..size].to_vec();
                
                // Send the data to the worker thread through the channel
                if sender.try_send(data).is_ok() {
                    // Increment the received stats
                    unsafe { shared_mutable_received_stats.increment(thread_idx) };
                }
                else {
                    // Increment the dropped stats
                    unsafe { shared_mutable_dropped_stats.increment(thread_idx) };
                }
            }
            Err(e) => {
                println!("Receiver {}: Error receiving UDP packet: {}", thread_idx, e);
                break;
            }
        }
    }

    Ok(())
}

async fn process_messages(receiver: Receiver<Vec<u8>>, thread_idx: usize) {    
    while let Ok(data) = receiver.recv() {
        // Convert data to string and print it
        match std::str::from_utf8(&data) {
            Ok(s) => { 
                // println!("Worker {} received: {}", thread_idx, s);
                std::hint::black_box(_ = parse_message(s, Variant::Either));
            }
            Err(_) => {
                println!("Worker {}: invalid data receiver", thread_idx);
            }
        }
    }
    
    println!("Worker {} channel closed, exiting", thread_idx);
}

#[derive(Copy, Clone)]
struct UnsafeSlice<'a> {
    slice: &'a [UnsafeCell<Stats>],
}

unsafe impl Send for UnsafeSlice<'_> {}
unsafe impl Sync for UnsafeSlice<'_> {}

impl<'a> UnsafeSlice<'a> {
    fn new(slice: &'a mut [Stats]) -> Self {
        let ptr = slice as *mut [Stats] as *const [UnsafeCell<Stats>];
        Self {
            slice: unsafe { &*ptr },
        }
    }

    // SAFETY: It's assumed that no two threads will write to the same index at the same time
    #[inline(always)]
    unsafe fn increment(&self, i: usize) {
        let value = self.slice[i].get();
        unsafe { (*value).count += 1 };
    }

    #[inline(always)]
    fn sum(&self) -> u64 {
        self.slice
            .iter()
            .map(|cell| unsafe { (*cell.get()).count })
            .sum()
    }
}