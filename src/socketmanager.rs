use std::{
    collections::{HashMap, VecDeque},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::fd::{AsRawFd, RawFd},
    ptr,
};

use libc::{
    EPOLL_CTL_ADD, EPOLL_CTL_DEL, EPOLL_CTL_MOD, EPOLLET, EPOLLIN, EPOLLOUT, F_SETFL, O_NONBLOCK,
    epoll_create1, epoll_ctl, epoll_event, epoll_wait, fcntl,
};

use crate::queue::{Receiver, Sender};

/// Commands sent to the `SocketManager` from other threads.
#[derive(Debug)]
pub enum Command {
    /// Send data to the client with the given file descriptor.
    Send(RawFd, Vec<u8>),
    /// Close the client with the given file descriptor.
    Close(RawFd),
}

/// Represents a socket managed by the `SocketManager`.
#[derive(Debug)]
pub enum Socket {
    /// A listening TCP socket.
    Listener(TcpListener),
    /// A connected TCP client socket.
    Client(TcpStream),
}

struct Connection {
    socket: Socket,
    send_queue: VecDeque<Vec<u8>>,
}

impl Connection {
    fn new(socket: Socket) -> Self {
        Connection {
            socket: socket,
            send_queue: VecDeque::new(),
        }
    }
}

/// Manages multiple TCP connections using `epoll` with edge-triggered mode.
pub struct SocketManager {
    epoll_fd: RawFd,
    conns: HashMap<RawFd, Connection>,
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
        })
    }

    fn get_socket_fd(&self, socket: &Socket) -> RawFd {
        match socket {
            Socket::Client(stream) => stream.as_raw_fd(),
            Socket::Listener(listener) => listener.as_raw_fd(),
        }
    }

    /// Adds a new socket (listener or client) to the `SocketManager` and epoll set.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if `epoll_ctl` fails.
    pub fn add_socket(&mut self, socket: Socket) -> io::Result<()> {
        let fd = self.get_socket_fd(&socket);
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
        self.conns.insert(fd, Connection::new(socket));
        Ok(())
    }

    fn remove_socket(&mut self, fd: RawFd) {
        // best-effort: remove from epoll (ignore errors)
        let _ = unsafe { epoll_ctl(self.epoll_fd, EPOLL_CTL_DEL, fd, ptr::null_mut()) };
        self.conns.remove(&fd);
    }

    fn queue_message(&mut self, fd: RawFd, data: Vec<u8>) {
        if let Some(conn) = self.conns.get_mut(&fd) {
            conn.send_queue.push_back(data);

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
        sender: &Sender<(Vec<u8>, SocketAddr, RawFd)>,
    ) -> io::Result<()> {
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                Command::Send(fd, data) => self.queue_message(fd, data),
                Command::Close(fd) => self.remove_socket(fd),
            }
        }

        const MAX_EVENTS: usize = 32;
        let mut events = [epoll_event { events: 0, u64: 0 }; MAX_EVENTS];
        let nfds =
            unsafe { epoll_wait(self.epoll_fd, events.as_mut_ptr(), MAX_EVENTS as i32, 100) };
        if nfds < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                return Ok(());
            }
            return Err(err);
        }

        for i in 0..(nfds as usize) {
            let ev = events[i];
            let fd = ev.u64 as RawFd;

            if let Some(mut conn) = self.conns.remove(&fd) {
                match &mut conn.socket {
                    Socket::Listener(listener) => {
                        if let Err(e) = self.handle_accept(listener) {
                            eprintln!("listener accept error: {e}");
                            self.remove_socket(fd);
                            continue;
                        }
                    }
                    Socket::Client(stream) => {
                        if (ev.events & EPOLLIN as u32) != 0 {
                            match self.handle_read(stream) {
                                Ok(data) => {
                                    if let Err(e) = sender.send((
                                        data,
                                        stream.peer_addr().unwrap(),
                                        stream.as_raw_fd(),
                                    )) {
                                        eprintln!("failed to notify data received: {e}");
                                    }
                                }
                                Err(e) => {
                                    eprintln!("client read error: {e}");
                                    self.remove_socket(fd);
                                    continue;
                                }
                            }
                        }
                        if (ev.events & EPOLLOUT as u32) != 0 {
                            if let Err(e) = self.handle_write(stream, &mut conn.send_queue) {
                                eprintln!("client write error: {e}");
                                self.remove_socket(fd);
                                continue;
                            }
                        }
                    }
                }

                self.conns.insert(fd, conn);
            } else {
                self.remove_socket(fd);
            }
        }

        Ok(())
    }

    fn handle_accept(&mut self, listener: &mut TcpListener) -> io::Result<()> {
        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    println!("New connection: {addr:?}");
                    if let Err(e) = self.add_socket(Socket::Client(stream)) {
                        eprintln!("Failed to add client connection: {e}");
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn handle_read(&mut self, stream: &mut TcpStream) -> io::Result<Vec<u8>> {
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
        &mut self,
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
}
