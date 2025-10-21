use std::{
    collections::{HashMap, VecDeque},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::fd::{AsRawFd, RawFd},
    ptr,
    sync::mpsc::{Receiver, Sender},
};

use libc::{
    EPOLL_CTL_ADD, EPOLL_CTL_DEL, EPOLL_CTL_MOD, EPOLLET, EPOLLIN, EPOLLOUT, F_SETFL, O_NONBLOCK,
    epoll_create1, epoll_ctl, epoll_event, epoll_wait, fcntl,
};

/// Commands sent to the `SocketManager` from other threads.
#[derive(Debug)]
pub enum Command {
    /// Send data to the client with the given file descriptor.
    Send(SocketAddr, Vec<u8>, bool),
    /// Close the client with the given file descriptor.
    Close(SocketAddr),
    // Close the client with the given socket.
    CloseSocket((Socket, SocketAddr)),
    // Add socket.
    Add((Socket, SocketAddr, Option<usize>)),
}

/// Represents a socket managed by the `SocketManager`.
#[derive(Debug)]
pub enum Socket {
    /// A listening TCP socket.
    Listener(TcpListener),
    /// A connected TCP client socket.
    Client((TcpStream, SocketAddr)),
}

impl Socket {
    pub fn get_raw_fd(&self) -> RawFd {
        match self {
            Socket::Listener(tcp_listener) => tcp_listener.as_raw_fd(),
            Socket::Client((tcp_stream, _)) => tcp_stream.as_raw_fd(),
        }
    }
}

#[derive(Debug)]
struct Connection {
    socket: Socket,
    send_queue: VecDeque<Vec<u8>>,
    max_send_queue_size: Option<usize>,
}

impl Connection {
    fn new(socket: Socket, max_send_queue_size: Option<usize>) -> Self {
        Connection {
            socket: socket,
            send_queue: VecDeque::new(),
            max_send_queue_size: max_send_queue_size,
        }
    }
}

/// Manages multiple TCP connections using `epoll` with edge-triggered mode.
#[derive(Debug)]
pub struct SocketManager {
    epoll_fd: RawFd,
    conns: HashMap<SocketAddr, Connection>,
    fd_addrs: HashMap<RawFd, SocketAddr>,
}

impl SocketManager {
    /// Creates a new `SocketManager` with an epoll instance.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if `epoll_create1` fails.
    pub fn new() -> io::Result<Self> {
        let epfd = unsafe { epoll_create1(0) };
        if epfd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(SocketManager {
            epoll_fd: epfd,
            conns: HashMap::new(),
            fd_addrs: HashMap::new(),
        })
    }

    /// Adds a new socket (listener or client) to the `SocketManager` and epoll set.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if `epoll_ctl` fails.
    fn add_socket(
        &mut self,
        addr: SocketAddr,
        socket: Socket,
        max_send_queue_size: Option<usize>,
    ) -> io::Result<()> {
        let fd = socket.get_raw_fd();
        let mut ev = epoll_event {
            events: (EPOLLIN | EPOLLET) as u32,
            u64: fd as u64,
        };
        let ret = unsafe { epoll_ctl(self.epoll_fd, EPOLL_CTL_ADD, fd, &mut ev) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe {
            fcntl(fd, F_SETFL, O_NONBLOCK);
        }
        self.conns
            .insert(addr, Connection::new(socket, max_send_queue_size));
        self.fd_addrs.insert(fd, addr);
        Ok(())
    }

    fn remove_socket(&mut self, addr: SocketAddr) {
        if let Some(conn) = self.conns.remove(&addr) {
            self.remove_fd(conn.socket.get_raw_fd());
        }
    }

    fn remove_fd(&mut self, fd: RawFd) {
        // best-effort: remove from epoll (ignore errors)
        let _ = unsafe { epoll_ctl(self.epoll_fd, EPOLL_CTL_DEL, fd, ptr::null_mut()) };
        self.fd_addrs.remove(&fd);
    }

    fn queue_message(&mut self, addr: SocketAddr, data: Vec<u8>, is_critical: bool) {
        if let Some(conn) = self.conns.get_mut(&addr) {
            if let Some(max_queue_size) = conn.max_send_queue_size {
                if conn.send_queue.len() >= max_queue_size {
                    if is_critical {
                        return;
                    }
                    conn.send_queue.pop_front();
                }
            }
            conn.send_queue.push_back(data);
            let fd = conn.socket.get_raw_fd();
            let mut ev = epoll_event {
                events: (EPOLLIN | EPOLLOUT | EPOLLET) as u32,
                u64: fd as u64,
            };
            unsafe {
                epoll_ctl(self.epoll_fd, EPOLL_CTL_MOD, fd, &mut ev);
            }
        }
    }

    /// Runs a single iteration of the event loop.
    ///
    /// Processes epoll events and applies any queued commands received from other threads.
    ///
    /// # Arguments
    ///
    /// * `rx` - Receiver channel for `Command`s from other threads.
    /// * `sender` - Sender channel to notify about received data.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if `epoll_wait` or socket operations fail.
    pub fn run_once(
        &mut self,
        rx: &Receiver<Command>,
        sender: &Sender<(Vec<u8>, SocketAddr)>,
    ) -> io::Result<Vec<SocketAddr>> {
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                Command::Send(addr, data, is_critical) => {
                    self.queue_message(addr, data, is_critical);
                }
                Command::Close(addr) => {
                    self.remove_socket(addr);
                }
                Command::Add((socket, addr, max_send_queue_size)) => {
                    self.add_socket(addr, socket, max_send_queue_size)?;
                }
                Command::CloseSocket((_socket, addr)) => {
                    self.remove_socket(addr);
                }
            }
        }

        let mut disconnected_sockets = Vec::new();
        const MAX_EVENTS: usize = 32;
        let mut events = [epoll_event { events: 0, u64: 0 }; MAX_EVENTS];
        let nfds =
            unsafe { epoll_wait(self.epoll_fd, events.as_mut_ptr(), MAX_EVENTS as i32, 100) };
        if nfds < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                return Ok(disconnected_sockets);
            }
            return Err(err);
        }

        for i in 0..(nfds as usize) {
            let ev = events[i];
            let fd = ev.u64 as RawFd;
            let Some(addr) = self.fd_addrs.remove(&fd) else {
                self.remove_fd(fd);
                continue;
            };
            let Some(mut conn) = self.conns.remove(&addr) else {
                self.remove_socket(addr);
                continue;
            };

            match &mut conn.socket {
                Socket::Listener(listener) => {
                    if let Err(e) = self.handle_accept(listener) {
                        eprintln!("listener accept error: {e}");
                        self.remove_socket(addr);
                        disconnected_sockets.push(addr);
                        continue;
                    }
                }
                Socket::Client((stream, addr)) => {
                    if (ev.events & EPOLLIN as u32) != 0 {
                        match self.handle_read(stream) {
                            Ok(data) => {
                                if let Err(e) = sender.send((data, *addr)) {
                                    eprintln!("failed to notify data received: {e}");
                                }
                            }
                            Err(e) => {
                                eprintln!("client read error: {e}");
                                self.remove_socket(*addr);
                                disconnected_sockets.push(*addr);
                                continue;
                            }
                        }
                    }
                    if (ev.events & EPOLLOUT as u32) != 0 {
                        if let Err(e) = self.handle_write(stream, &mut conn.send_queue) {
                            eprintln!("client write error: {e}");
                            self.remove_socket(*addr);
                            disconnected_sockets.push(*addr);
                            continue;
                        }
                    }
                }
            }

            self.conns.insert(addr, conn);
            self.fd_addrs.insert(fd, addr);
        }

        Ok(disconnected_sockets)
    }

    fn handle_accept(&mut self, listener: &mut TcpListener) -> io::Result<()> {
        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    if let Err(e) = self.add_socket(addr, Socket::Client((stream, addr)), None) {
                        eprintln!("Failed to add client connection: {e}");
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn handle_read(&self, stream: &mut TcpStream) -> io::Result<Vec<u8>> {
        let mut buf = [0u8; 4096];
        let mut result = Vec::new();
        loop {
            match stream.read(&mut buf) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "client disconnected",
                    ));
                }
                Ok(n) => {
                    result.extend_from_slice(&buf[..n]);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(result)
    }

    fn handle_write(
        &self,
        stream: &mut TcpStream,
        send_queue: &mut VecDeque<Vec<u8>>,
    ) -> io::Result<()> {
        while let Some(front) = send_queue.front_mut() {
            match stream.write(front) {
                Ok(0) => break,
                Ok(n) => {
                    front.drain(..n);
                    if front.is_empty() {
                        send_queue.pop_front();
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if send_queue.is_empty() {
            let mut ev = epoll_event {
                events: (EPOLLIN | EPOLLET) as u32,
                u64: stream.as_raw_fd() as u64,
            };
            unsafe {
                epoll_ctl(self.epoll_fd, EPOLL_CTL_MOD, stream.as_raw_fd(), &mut ev);
            }
        }
        Ok(())
    }

    pub fn close(&mut self) {
        let addrs = self.conns.keys().copied().collect::<Vec<SocketAddr>>();
        for addr in addrs {
            self.remove_socket(addr);
        }
    }
}
