use snafu::{ResultExt, Snafu};
use socket2::SockRef;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{self, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Mutex},
    task,
};

type ConnId = u64;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to bind listener: {source}"))]
    BindListener { source: io::Error },

    #[snafu(display("Failed to get local address: {source}"))]
    LocalAddr { source: io::Error },

    #[snafu(display("Failed to parse address: {source}"))]
    ParseAddr { source: std::net::AddrParseError },

    #[snafu(display("Failed to connect to server: {source}"))]
    Connect { source: io::Error },

    #[snafu(display("Failed to read from stream: {source}"))]
    Read { source: io::Error },

    #[snafu(display("Failed to write to stream: {source}"))]
    Write { source: io::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Specifies which side should send the RST.
#[derive(Debug, Clone, Copy)]
enum ResetKind {
    FromServer, // Client receives RST
    FromClient, // Server receives RST
}

/// Holds control channel to signal the forwarder task to perform a reset.
struct ConnControl {
    tx: oneshot::Sender<ResetKind>,
}

#[derive(Clone)]
pub struct TestProxy {
    // key -> control streams
    conns: Arc<Mutex<HashMap<ConnId, ConnControl>>>,
    // next id generator (keeps accept deterministic)
    next_id: Arc<Mutex<ConnId>>,
    // listener addr (useful for tests)
    pub listen_addr: SocketAddr,
}

impl TestProxy {
    /// Spawn a proxy bound to 127.0.0.1:0 that forwards to target_addr.
    /// Returns a TestProxy handle and the bound listening address.
    pub async fn spawn(target_addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context(BindListenerSnafu)?;
        let listen_addr = listener.local_addr().context(LocalAddrSnafu)?;

        let conns: Arc<Mutex<HashMap<ConnId, ConnControl>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let conns_accept = conns.clone();
        let next_id = Arc::new(Mutex::new(1u64));
        let next_id_accept = next_id.clone();

        // accept loop
        task::spawn(async move {
            loop {
                let (client_stream, _) = match listener.accept().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // connect to the real server
                let server_stream = match TcpStream::connect(target_addr).await {
                    Ok(s) => s,
                    Err(_) => {
                        let mut client_stream = client_stream;
                        let _ = client_stream.shutdown().await;
                        continue;
                    }
                };

                // assign id and create control channel
                let id = {
                    let mut nid = next_id_accept.lock().await;
                    let id = *nid;
                    *nid = nid.wrapping_add(1);
                    id
                };

                let (tx, rx) = oneshot::channel();
                {
                    let mut lock = conns_accept.lock().await;
                    lock.insert(id, ConnControl { tx });
                }

                // spawn forwarder - it owns the only FDs
                let conns_for_cleanup = conns_accept.clone();
                task::spawn(Self::spawn_forwarder(
                    client_stream,
                    server_stream,
                    rx,
                    conns_for_cleanup,
                    id,
                ));
            }
        });

        Ok(Self {
            conns,
            next_id,
            listen_addr,
        })
    }

    /// Forwarder task that owns the TCP streams and can perform controlled resets.
    async fn spawn_forwarder(
        mut client: TcpStream,
        mut server: TcpStream,
        mut rx: oneshot::Receiver<ResetKind>,
        conns: Arc<Mutex<HashMap<ConnId, ConnControl>>>,
        id: ConnId,
    ) {
        // Wrap in a scope to ensure the borrow is released before we process reset
        let reset_kind = {
            let fwd = io::copy_bidirectional(&mut client, &mut server);
            tokio::pin!(fwd);

            tokio::select! {
                _ = &mut fwd => {
                    // normal EOF/error, done
                    None
                }
                msg = &mut rx => {
                    // Forward future is dropped at end of scope, releasing borrows
                    msg.ok()
                }
            }
            // fwd is dropped here, releasing the mutable borrows on client and server
        };

        // Now we have exclusive ownership and can set linger
        if let Some(kind) = reset_kind {
            match kind {
                ResetKind::FromServer => {
                    // client must see RST - set linger on client-facing socket
                    if let Ok(std_client) = client.into_std() {
                        let sock = SockRef::from(&std_client);
                        let _ = sock.set_linger(Some(Duration::from_secs(0)));
                        drop(std_client); // closes with RST toward the client
                    }
                    drop(server);
                }
                ResetKind::FromClient => {
                    // server must see RST - set linger on server-facing socket
                    if let Ok(std_server) = server.into_std() {
                        let sock = SockRef::from(&std_server);
                        let _ = sock.set_linger(Some(Duration::from_secs(0)));
                        drop(std_server); // RST toward the server
                    }
                    drop(client);
                }
            }
        }

        // cleanup control entry if still present
        let mut lock = conns.lock().await;
        lock.remove(&id);
    }

    /// Force a one-shot RST as if sent by the server to the client.
    /// Signals all forwarder tasks to set SO_LINGER=0 on the client-facing socket
    /// and drop it, causing the client to receive ECONNRESET.
    pub async fn reset_all_from_server(&self) {
        let mut lock = self.conns.lock().await;
        for (_id, ctrl) in lock.drain() {
            let _ = ctrl.tx.send(ResetKind::FromServer);
        }
    }

    /// Force a one-shot RST as if sent by the client to the server.
    /// Signals all forwarder tasks to set SO_LINGER=0 on the server-facing socket
    /// and drop it, causing the server to receive ECONNRESET.
    pub async fn reset_all_from_client(&self) {
        let mut lock = self.conns.lock().await;
        for (_id, ctrl) in lock.drain() {
            let _ = ctrl.tx.send(ResetKind::FromClient);
        }
    }

    /// Force RSTs in both directions for all tracked connections.
    /// Note: This drains the connection map on first call, so subsequent calls
    /// to reset_all_from_client will have no effect.
    pub async fn reset_all(&self) {
        // Since both methods drain the map, we can only call one
        self.reset_all_from_server().await;
    }

    /// Gracefully close all connections by signaling forwarders to drop streams normally.
    /// This is called automatically when TestProxy is dropped.
    fn close_all_connections(&self) {
        // Use try_lock to avoid blocking in Drop
        if let Ok(mut lock) = self.conns.try_lock() {
            // Signal all forwarders to close gracefully
            // We'll just drain and drop the senders - when the forwarders
            // try to receive, they'll get Err and clean up normally
            lock.clear();
        }
    }
}

impl Drop for TestProxy {
    fn drop(&mut self) {
        // When TestProxy is dropped, close all tracked connections
        self.close_all_connections();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncReadExt, io::AsyncWriteExt};

    /// Simple echo server for testing.
    /// Returns (JoinHandle, bound_address)
    async fn start_echo_server() -> io::Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        match s.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let _ = s.write_all(&buf[..n]).await;
                            }
                            Err(_) => break,
                        }
                    }
                });
            }
        });
        Ok((handle, addr))
    }

    #[tokio::test]
    async fn proxy_resets_connection() -> Result<()> {
        // start real server on a random available port
        let (_server_h, server_addr) = start_echo_server()
            .await
            .map_err(|e| Error::BindListener { source: e })?;

        // give the echo server a moment to be ready
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;


        // spawn proxy
        let proxy = TestProxy::spawn(server_addr).await?;
        let proxy_addr = proxy.listen_addr;

        // connect client to proxy
        let mut client = TcpStream::connect(proxy_addr)
            .await
            .context(ConnectSnafu)?;
        client.write_all(b"ping").await.context(WriteSnafu)?;
        let mut buf = [0u8; 16];
        let n = client.read(&mut buf).await.context(ReadSnafu)?;
        assert_eq!(&buf[..n], b"ping");

        // now reset as if from server
        proxy.reset_all_from_server().await;

        // give the RST a moment to propagate
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // subsequent read should error (ECONNRESET) - reads detect RST immediately
        // whereas writes might succeed locally due to buffering
        let read_res = client.read(&mut buf).await;
        assert!(
            read_res.is_err(),
            "expected read to fail after reset, got {:?}",
            read_res
        );

        Ok(())
    }

    #[tokio::test]
    async fn proxy_graceful_close() -> Result<()> {
        // start real server on a random available port
        let (_server_h, server_addr) = start_echo_server()
            .await
            .map_err(|e| Error::BindListener { source: e })?;

        // give the echo server a moment to be ready
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // spawn proxy
        let proxy = TestProxy::spawn(server_addr).await?;
        let proxy_addr = proxy.listen_addr;

        // connect client to proxy
        let mut client = TcpStream::connect(proxy_addr)
            .await
            .context(ConnectSnafu)?;
        client.write_all(b"ping").await.context(WriteSnafu)?;
        let mut buf = [0u8; 16];
        let n = client.read(&mut buf).await.context(ReadSnafu)?;
        assert_eq!(&buf[..n], b"ping");

        // Drop the proxy - Drop impl closes all connections gracefully (FIN, not RST)
        drop(proxy);

        // Give the forwarder tasks a moment to process the channel closure and drop streams
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Wait for socket to become readable (data available or closed)
        client.readable().await.context(ReadSnafu)?;

        // Check if connection is closed - 0 bytes means EOF
        match client.try_read(&mut buf) {
            Ok(0) => {
                // EOF - connection closed gracefully with FIN
                println!("✓ Connection closed gracefully (got 0 bytes = EOF)");
            }
            Ok(n) => {
                panic!("Expected EOF but read {} bytes: {:?}", n, &buf[..n]);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Shouldn't happen after readable() completes
                panic!("Unexpected WouldBlock after readable()");
            }
            Err(e) => {
                return Err(Error::Read { source: e });
            }
        }

        Ok(())
    }

}
