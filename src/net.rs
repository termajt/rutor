use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Read, Write},
    net::SocketAddr,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes};
use crossbeam::channel::{Receiver, Sender};
use mio::{Events, Interest, Poll, Token, net::TcpStream};

use crate::{
    PeerMessage,
    bitfield::Bitfield,
    engine::{EngineEvent, Tick, duration_to_ticks},
    peer::{PeerAction, PeerManager},
    rate_limiter::RateLimiter,
};

pub const MAX_CONNECTIONS: usize = 50;
const CONNECTION_TIMEOUT: Tick = duration_to_ticks(Duration::from_secs(3));
const READ_CHUNK: usize = 16 * 1024;
const MAX_READ_PER_EVENT: usize = 256 * 1024;
const MAX_WRITE_PER_EVENT: usize = 256 * 1024;

#[derive(Debug)]
pub enum NetError {
    ConnectFailure { addr: SocketAddr, reason: io::Error },
    CloseFailure { addr: SocketAddr, reason: io::Error },
}

#[derive(Debug, Default)]
struct ConnectionPool {
    available_queue: VecDeque<SocketAddr>,
    available_set: HashSet<SocketAddr>,
}

impl ConnectionPool {
    fn add_peers(&mut self, peers: impl IntoIterator<Item = SocketAddr>) {
        for addr in peers {
            if self.available_set.insert(addr) {
                self.available_queue.push_back(addr);
            }
        }
    }

    fn pop_next(&mut self) -> Option<SocketAddr> {
        let addr = self.available_queue.pop_front()?;
        self.available_set.remove(&addr);

        Some(addr)
    }

    fn requeue(&mut self, _addr: SocketAddr) {
        /*
        if self.available_set.insert(addr) {
            self.available_queue.push_back(addr);
        }
        */
    }
}

#[derive(Debug)]
struct TokenGenerator {
    next_token: usize,
}

impl TokenGenerator {
    fn new() -> Self {
        Self { next_token: 1 }
    }

    fn get_next_token(&mut self) -> Token {
        let t = Token(self.next_token);
        self.next_token += 1;

        t
    }
}

#[derive(Debug)]
struct Connection {
    addr: SocketAddr,
    token: Token,
    socket: TcpStream,
    outgoing: VecDeque<Bytes>,
    connecting: bool,
    connect_deadline: Tick,
    last_interest: Interest,
    read_limiter: RateLimiter,
    write_limiter: RateLimiter,
}

impl Connection {
    fn new(
        addr: SocketAddr,
        token: Token,
        socket: TcpStream,
        current_tick: Tick,
        interest: Interest,
        max_read_bytes_per_sec: usize,
        max_write_bytes_per_sec: usize,
    ) -> Self {
        Self {
            addr,
            token,
            socket,
            outgoing: VecDeque::new(),
            connecting: true,
            connect_deadline: current_tick + CONNECTION_TIMEOUT,
            last_interest: interest,
            read_limiter: RateLimiter::new(max_read_bytes_per_sec),
            write_limiter: RateLimiter::new(max_write_bytes_per_sec),
        }
    }

    fn enqueue_message(&mut self, data: Bytes) {
        self.outgoing.push_back(data);
    }

    fn update_interest(&mut self, poll: &mut Poll) -> io::Result<()> {
        let interest = self.interest();

        if interest != self.last_interest {
            match poll
                .registry()
                .reregister(&mut self.socket, self.token, interest)
            {
                Ok(()) => {
                    self.last_interest = interest;
                }
                Err(e) => {
                    log::error!("{} failed to reregister poll: {:?}", self.addr, e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    fn interest(&self) -> Interest {
        if self.connecting || !self.outgoing.is_empty() {
            Interest::READABLE.add(Interest::WRITABLE)
        } else {
            Interest::READABLE
        }
    }
}

#[derive(Debug)]
pub enum NetEvent {
    NewPeers {
        addrs: Vec<SocketAddr>,
    },
    Tick {
        tick: Tick,
        endgame: bool,
    },
    UpdatePeerInterest {
        addr: SocketAddr,
        interested: bool,
    },
    SendPeerMessages {
        token: Token,
        messages: Vec<PeerMessage>,
    },
    RequestMissingBlocks {
        token: Token,
        missing_blocks: Vec<(u32, u32)>,
    },
    CancelBlock {
        piece: u32,
        offset: u32,
        length: u32,
    },
    Shutdown,
}

const POLL_TIMEOUT: Duration = Duration::from_millis(200);
const CHANNEL_TIMEOUT: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub struct NetManager {
    join: Option<JoinHandle<()>>,
}

impl NetManager {
    pub fn new() -> Self {
        Self { join: None }
    }

    pub fn start(
        &mut self,
        rx: &Receiver<NetEvent>,
        total_pieces: usize,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        engine_tx: &Sender<EngineEvent>,
        max_read_bytes_per_sec: usize,
        max_write_bytes_per_sec: usize,
    ) {
        let rx = rx.clone();
        let engine_tx = engine_tx.clone();
        let join = std::thread::spawn(move || {
            log::info!(
                "net io thread {:?} starting...",
                std::thread::current().id()
            );
            let mut poll = match Poll::new() {
                Ok(p) => p,
                Err(e) => {
                    log::error!("failed to create poll: {}", e);
                    return;
                }
            };
            let mut peer_mgr = PeerManager::new(total_pieces, info_hash, peer_id);
            let mut events = Events::with_capacity(1024);
            let mut connections = HashMap::new();
            let mut token_to_addr = HashMap::new();
            let mut token_gen = TokenGenerator::new();
            let mut current_tick: Tick = Tick(0);
            let mut connection_pool = ConnectionPool::default();
            let mut idle_since = None;
            let mut global_read_limiter = RateLimiter::new(max_read_bytes_per_sec);
            let mut global_write_limiter = RateLimiter::new(max_write_bytes_per_sec);
            loop {
                let (shutdown, tick) = consume_events(
                    &rx,
                    &mut connections,
                    &mut token_to_addr,
                    &mut peer_mgr,
                    &mut poll,
                    current_tick,
                    &engine_tx,
                    &mut connection_pool,
                );
                current_tick = tick;
                if shutdown {
                    break;
                }

                let free = MAX_CONNECTIONS.saturating_sub(connections.len());
                for _ in 0..free {
                    let Some(addr) = connection_pool.pop_next() else {
                        break;
                    };
                    if connections.contains_key(&addr) {
                        continue;
                    }

                    let token = token_gen.get_next_token();
                    if let Err(e) = handle_connect(
                        addr,
                        &mut poll,
                        token,
                        &mut connections,
                        &mut token_to_addr,
                        current_tick,
                        max_read_bytes_per_sec,
                        max_write_bytes_per_sec,
                    ) {
                        log::warn!("{} connect failed: {:?}", addr, e);
                    }
                }

                let mut timed_out = Vec::new();
                for (addr, conn) in connections.iter() {
                    if conn.connecting && current_tick >= conn.connect_deadline {
                        timed_out.push(*addr);
                    }
                }

                for addr in timed_out {
                    if let Some(conn) = connections.get(&addr) {
                        log::warn!("{} connect timed out", addr);
                        let _ = handle_close(
                            conn.token,
                            &mut poll,
                            &mut connections,
                            &mut token_to_addr,
                            &mut peer_mgr,
                            &engine_tx,
                            &mut connection_pool,
                        );
                    }
                }

                if let Err(e) = poll.poll(&mut events, Some(POLL_TIMEOUT)) {
                    log::error!("failed to poll connections: {}", e);
                    continue;
                }

                let mut peer_actions = Vec::new();
                for event in &events {
                    let token = event.token();
                    let addr = match token_to_addr.get(&token) {
                        Some(a) => a,
                        None => continue,
                    };
                    let conn = match connections.get_mut(&addr) {
                        Some(c) => c,
                        None => continue,
                    };

                    if event.is_readable() {
                        match socket_read(
                            conn,
                            &mut global_read_limiter,
                            &mut peer_mgr,
                            current_tick,
                        ) {
                            Ok(()) => {
                                let actions = peer_mgr.parse_messages(conn.token, current_tick);
                                peer_actions.extend(actions);
                            }
                            Err(e) => {
                                log::error!("{} read error: {:?}", conn.addr, e);
                                let _ = handle_close(
                                    token,
                                    &mut poll,
                                    &mut connections,
                                    &mut token_to_addr,
                                    &mut peer_mgr,
                                    &engine_tx,
                                    &mut connection_pool,
                                );
                                continue;
                            }
                        }
                    }

                    if event.is_writable() {
                        if conn.connecting {
                            if let Ok(Some(err)) = conn.socket.take_error() {
                                log::warn!("{} connect error: {:?}", conn.addr, err);
                                let _ = handle_close(
                                    token,
                                    &mut poll,
                                    &mut connections,
                                    &mut token_to_addr,
                                    &mut peer_mgr,
                                    &engine_tx,
                                    &mut connection_pool,
                                );
                                continue;
                            }
                            conn.connecting = false;
                            let actions = peer_mgr.on_connected(conn.addr, token, current_tick);
                            peer_actions.extend(actions);
                        }
                        if let Err(e) = socket_write(conn, &mut poll, &mut global_write_limiter) {
                            log::error!("{} write error: {:?}", conn.addr, e);
                            let _ = handle_close(
                                token,
                                &mut poll,
                                &mut connections,
                                &mut token_to_addr,
                                &mut peer_mgr,
                                &engine_tx,
                                &mut connection_pool,
                            );
                        }
                    }
                }
                let idle = events.is_empty() && peer_actions.is_empty();
                consume_peer_actions(
                    peer_actions,
                    &mut poll,
                    &mut connections,
                    &mut token_to_addr,
                    &mut peer_mgr,
                    &engine_tx,
                    &mut connection_pool,
                );

                if idle {
                    let since = idle_since.get_or_insert_with(Instant::now);
                    let elapsed = since.elapsed();

                    let sleep = if elapsed < Duration::from_millis(2) {
                        // 1 spin cycle is cheap
                        Duration::from_millis(0)
                    } else if elapsed < Duration::from_millis(10) {
                        Duration::from_millis(1)
                    } else {
                        Duration::from_millis(5)
                    };
                    if sleep.as_millis() > 0 {
                        std::thread::sleep(sleep);
                    }
                } else {
                    idle_since = None;
                }
            }

            log::info!("net io thread {:?} exiting...", std::thread::current().id());
        });
        self.join = Some(join);
    }

    pub fn join(&mut self) {
        let join = match self.join.take() {
            Some(j) => j,
            None => return,
        };
        let _ = join.join();
    }
}

fn consume_peer_actions(
    actions: Vec<PeerAction>,
    poll: &mut Poll,
    connections: &mut HashMap<SocketAddr, Connection>,
    token_to_addr: &mut HashMap<Token, SocketAddr>,
    peer_mgr: &mut PeerManager,
    engine_tx: &Sender<EngineEvent>,
    connection_pool: &mut ConnectionPool,
) {
    for action in actions {
        consume_peer_action(
            action,
            poll,
            connections,
            token_to_addr,
            peer_mgr,
            engine_tx,
            connection_pool,
        );
    }
}

fn consume_peer_action(
    action: PeerAction,
    poll: &mut Poll,
    connections: &mut HashMap<SocketAddr, Connection>,
    token_to_addr: &mut HashMap<Token, SocketAddr>,
    peer_mgr: &mut PeerManager,
    engine_tx: &Sender<EngineEvent>,
    connection_pool: &mut ConnectionPool,
) {
    match action {
        PeerAction::Close { token } => {
            let _ = handle_close(
                token,
                poll,
                connections,
                token_to_addr,
                peer_mgr,
                engine_tx,
                connection_pool,
            );
        }
        PeerAction::PeerBitfield { token, bitfield } => {
            let addr = match token_to_addr.get(&token) {
                Some(a) => a,
                None => return,
            };
            let _ = engine_tx.send(EngineEvent::PeerBitfield {
                addr: *addr,
                bitfield,
            });
        }
        PeerAction::PeerHave { token, piece } => {
            let addr = match token_to_addr.get(&token) {
                Some(a) => a,
                None => return,
            };
            let _ = engine_tx.send(EngineEvent::PeerHave { addr: *addr, piece });
        }
        PeerAction::PeerConnected { token } => {
            let addr = match token_to_addr.get(&token) {
                Some(a) => a,
                None => return,
            };
            notify_peer_connected(engine_tx, *addr, peer_mgr);
        }
        PeerAction::HandshakeSuccess { token } => {
            let addr = match token_to_addr.get(&token) {
                Some(a) => a,
                None => return,
            };
            let _ = engine_tx.send(EngineEvent::PeerHandshakeSuccess { addr: *addr });
        }
        PeerAction::PickBlocks { token, count } => {
            if let Some(bitfield) = peer_mgr.get_bitfield(&token) {
                let _ = engine_tx.send(EngineEvent::PickBlocks {
                    token,
                    bitfield,
                    count,
                });
            }
        }
        PeerAction::Write { token } => {
            if let Err(e) = collect_messages(token, peer_mgr, token_to_addr, connections, poll) {
                log::error!("{:?} failed to collect outgoing messages: {:?}", token, e);
            }
        }
        PeerAction::BlockReceived {
            token: _,
            piece,
            offset,
            data,
        } => {
            let _ = engine_tx.send(EngineEvent::PieceBlock {
                piece,
                offset,
                data,
            });
        }
        PeerAction::TimeoutBlocks { blocks } => {
            let _ = engine_tx.send(EngineEvent::TimeoutBlocks { blocks });
        }
        PeerAction::RequestBlocks => {
            let actions = peer_mgr.on_request_blocks();
            consume_peer_actions(
                actions,
                poll,
                connections,
                token_to_addr,
                peer_mgr,
                engine_tx,
                connection_pool,
            );
        }
        PeerAction::Choked { token } => {
            let _ = engine_tx.send(EngineEvent::PeerChoked { token });
        }
        PeerAction::RequestMissingBlocks { token, bitfield } => {
            let _ = engine_tx.send(EngineEvent::RequestMissingBlocks { token, bitfield });
        }
    }
}

fn consume_events(
    rx: &Receiver<NetEvent>,
    connections: &mut HashMap<SocketAddr, Connection>,
    token_to_addr: &mut HashMap<Token, SocketAddr>,
    peer_mgr: &mut PeerManager,
    poll: &mut Poll,
    mut current_tick: Tick,
    engine_tx: &Sender<EngineEvent>,
    connection_pool: &mut ConnectionPool,
) -> (bool, Tick) {
    while let Ok(job) = rx.recv_timeout(CHANNEL_TIMEOUT) {
        match job {
            NetEvent::Shutdown => return (true, current_tick),
            NetEvent::Tick { tick, endgame } => {
                current_tick = tick;
                let actions = peer_mgr.on_tick(tick, endgame);
                consume_peer_actions(
                    actions,
                    poll,
                    connections,
                    token_to_addr,
                    peer_mgr,
                    engine_tx,
                    connection_pool,
                );
            }
            NetEvent::UpdatePeerInterest { addr, interested } => {
                let conn = match connections.get(&addr) {
                    Some(c) => c,
                    None => return (false, current_tick),
                };
                let actions = peer_mgr.on_update_interest(conn.token, interested, current_tick);
                consume_peer_actions(
                    actions,
                    poll,
                    connections,
                    token_to_addr,
                    peer_mgr,
                    engine_tx,
                    connection_pool,
                );
            }
            NetEvent::SendPeerMessages { token, messages } => {
                let actions = peer_mgr.enqueue_messages(token, &messages, current_tick);
                consume_peer_actions(
                    actions,
                    poll,
                    connections,
                    token_to_addr,
                    peer_mgr,
                    engine_tx,
                    connection_pool,
                );
            }
            NetEvent::NewPeers { addrs } => {
                connection_pool.add_peers(addrs);
            }
            NetEvent::RequestMissingBlocks {
                token,
                missing_blocks,
            } => {
                let actions = peer_mgr.on_request_missing_blocks(token, missing_blocks);
                consume_peer_actions(
                    actions,
                    poll,
                    connections,
                    token_to_addr,
                    peer_mgr,
                    engine_tx,
                    connection_pool,
                );
            }
            NetEvent::CancelBlock {
                piece,
                offset,
                length,
            } => {
                let actions = peer_mgr.on_cancel_block(piece, offset, length, current_tick);
                consume_peer_actions(
                    actions,
                    poll,
                    connections,
                    token_to_addr,
                    peer_mgr,
                    engine_tx,
                    connection_pool,
                );
            }
        }
    }

    (false, current_tick)
}

fn socket_read(
    conn: &mut Connection,
    global_limiter: &mut RateLimiter,
    peer_mgr: &mut PeerManager,
    now: Tick,
) -> io::Result<()> {
    let mut budget = conn
        .read_limiter
        .allow(MAX_READ_PER_EVENT)
        .min(global_limiter.allow(MAX_READ_PER_EVENT));
    let mut total_read = 0;
    let mut buf = [0u8; READ_CHUNK];
    while budget > 0 && total_read < MAX_READ_PER_EVENT {
        let remaining = MAX_READ_PER_EVENT.saturating_sub(total_read);
        let to_read = buf.len().min(budget).min(remaining);
        match conn.socket.read(&mut buf[..to_read]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "connection closed",
                ));
            }
            Ok(n) => {
                peer_mgr.append_incoming(&conn.token, &buf[..n], now);
                budget -= n;
                total_read += n;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

fn socket_write(
    conn: &mut Connection,
    poll: &mut Poll,
    global_limiter: &mut RateLimiter,
) -> io::Result<()> {
    let mut budget = conn
        .write_limiter
        .allow(MAX_WRITE_PER_EVENT)
        .min(global_limiter.allow(MAX_WRITE_PER_EVENT));
    let mut total_written = 0;

    while budget > 0 && total_written < MAX_WRITE_PER_EVENT {
        let front = match conn.outgoing.front_mut() {
            Some(f) => f,
            None => break,
        };

        let remaining = MAX_WRITE_PER_EVENT.saturating_sub(total_written);
        let to_write = front.len().min(budget).min(remaining);

        match conn.socket.write(&front[..to_write]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "connection closed",
                ));
            }
            Ok(n) => {
                front.advance(n);
                if front.is_empty() {
                    conn.outgoing.pop_front();
                }
                budget -= n;
                total_written += n;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        }
    }
    conn.update_interest(poll)?;

    Ok(())
}

fn collect_messages(
    token: Token,
    peer_mgr: &mut PeerManager,
    token_to_addr: &HashMap<Token, SocketAddr>,
    connections: &mut HashMap<SocketAddr, Connection>,
    poll: &mut Poll,
) -> io::Result<()> {
    let addr = token_to_addr[&token];
    let conn = connections.get_mut(&addr).unwrap();
    for data in peer_mgr.collect_outgoing(&token) {
        conn.enqueue_message(data);
    }
    conn.update_interest(poll)?;

    Ok(())
}

fn handle_close(
    token: Token,
    poll: &mut Poll,
    connections: &mut HashMap<SocketAddr, Connection>,
    token_to_addr: &mut HashMap<Token, SocketAddr>,
    peer_mgr: &mut PeerManager,
    engine_tx: &Sender<EngineEvent>,
    connection_pool: &mut ConnectionPool,
) -> Result<(), NetError> {
    let addr = match token_to_addr.remove(&token) {
        Some(a) => a,
        None => return Ok(()),
    };
    let mut conn = match connections.remove(&addr) {
        Some(c) => c,
        None => return Ok(()),
    };
    let bitfield = peer_mgr.get_bitfield(&token);
    if peer_mgr.on_close(&token) {
        notify_peer_disconnected(engine_tx, addr, token, peer_mgr.peer_count(), bitfield);
    }
    connection_pool.requeue(addr);

    poll.registry()
        .deregister(&mut conn.socket)
        .map_err(|err| NetError::CloseFailure { addr, reason: err })?;

    Ok(())
}

fn notify_peer_disconnected(
    tx: &Sender<EngineEvent>,
    addr: SocketAddr,
    token: Token,
    peer_count: usize,
    bitfield: Option<Bitfield>,
) {
    let _ = tx.send(EngineEvent::PeerDisconnected {
        addr,
        token,
        bitfield,
    });
    let _ = tx.send(EngineEvent::PeerCount { count: peer_count });
}

fn notify_peer_connected(tx: &Sender<EngineEvent>, addr: SocketAddr, peer_mgr: &PeerManager) {
    let _ = tx.send(EngineEvent::PeerConnected { addr });
    let _ = tx.send(EngineEvent::PeerCount {
        count: peer_mgr.peer_count(),
    });
}

fn handle_connect(
    addr: SocketAddr,
    poll: &mut Poll,
    token: Token,
    connections: &mut HashMap<SocketAddr, Connection>,
    token_to_addr: &mut HashMap<Token, SocketAddr>,
    now: Tick,
    max_read_bytes_per_sec: usize,
    max_write_bytes_per_sec: usize,
) -> Result<(), NetError> {
    let mut stream =
        TcpStream::connect(addr).map_err(|err| NetError::ConnectFailure { addr, reason: err })?;

    let interest = Interest::READABLE.add(Interest::WRITABLE);
    poll.registry()
        .register(&mut stream, token, interest)
        .map_err(|err| NetError::ConnectFailure { addr, reason: err })?;

    token_to_addr.insert(token, addr);
    connections.insert(
        addr,
        Connection::new(
            addr,
            token,
            stream,
            now,
            interest,
            max_read_bytes_per_sec,
            max_write_bytes_per_sec,
        ),
    );

    Ok(())
}
