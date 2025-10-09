use std::path::PathBuf;
use std::process::Stdio;
use tokio::{
    net::TcpListener,
    process::{Child, Command},
};
use tracing::info;
use uuid::Uuid;

#[cfg(unix)]
#[allow(unused_imports)] // Needed for pre_exec method
use std::os::unix::process::CommandExt;

const HOST: &str = "127.0.0.1";

pub struct TokenIndexerInstance {
    // Handle to the spawned pnpm dev process
    pub child: Child,
    pub api_server_url: String,
}

impl TokenIndexerInstance {
    pub async fn new(
        interactive: bool,
        rpc_url: &str,
        ws_url: &str,
        pipe_output: bool,
        chain_id: u64,
        database_url: String,
    ) -> std::io::Result<Self> {
        let workspace_root = std::env::var("CARGO_MANIFEST_DIR")
            .map(PathBuf::from)
            .map(|p| p.ancestors().nth(1).unwrap().to_path_buf()) // Go up 2 levels from crates/devnet
            .unwrap_or_else(|_| PathBuf::from("."));

        let token_indexer_dir = workspace_root.join("evm-token-indexer");

        let ponder_port = if interactive {
            50104_u16
        } else {
            let listener = TcpListener::bind((HOST, 0))
                .await
                .expect("Should be able to bind to port");

            listener
                .local_addr()
                .expect("Should have a local address")
                .port()
        };

        // uuid for the schema
        let schema_uuid = Uuid::new_v4();
        let mut cmd = Command::new("pnpm");
        cmd.args([
            "dev",
            "--disable-ui",
            "--port",
            ponder_port.to_string().as_str(),
            "--schema",
            schema_uuid.to_string().as_str(),
        ])
        .kill_on_drop(true)
        .current_dir(token_indexer_dir)
        .env("DATABASE_URL", database_url)
        .env("PONDER_CHAIN_ID", chain_id.to_string())
        .env("PONDER_RPC_URL_HTTP", rpc_url)
        .env("PONDER_WS_URL_HTTP", ws_url)
        .env("PONDER_DISABLE_CACHE", "true")
        .env("PONDER_CONTRACT_START_BLOCK", "0")
        .env("PONDER_LOG_LEVEL", "trace");

        // Set the child process to be the leader of its own process group
        // This prevents killing the parent test process when we clean up
        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                // setpgid(0, 0) makes this process the leader of a new process group
                libc::setpgid(0, 0);
                Ok(())
            });
        }

        if pipe_output {
            cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }

        let child = cmd.spawn().expect("Failed to spawn token indexer process");

        let api_server_url = format!("http://{HOST}:{ponder_port}");
        info!("Indexer API server URL: {api_server_url}");

        Ok(Self {
            child,
            api_server_url,
        })
    }

    /// Check if the process is still running
    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Kill the process
    pub async fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill().await
    }

    /// Wait for the process to finish
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }
}

impl Drop for TokenIndexerInstance {
    fn drop(&mut self) {
        if let Some(pid) = self.child.id() {
            self.kill_process_tree(pid);
        }
    }
}

impl TokenIndexerInstance {
    fn kill_process_tree(&self, pid: u32) {
        let pgid = unsafe { libc::getpgid(pid as i32) };

        if pgid >= 0 {
            // Send SIGTERM to entire process group
            // This is safe because the child process was spawned with its own process group
            // via setpgid(0, 0), so this will only kill the child and its descendants,
            // not the parent test process
            unsafe {
                libc::kill(-pgid, libc::SIGTERM);
            }
        } else {
            // Fallback: just kill the parent process if getpgid failed
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .output();
        }
    }
}
