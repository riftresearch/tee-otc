use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    time::{sleep, timeout},
};
use tracing::{info, warn};
use uuid::Uuid;

const HOST: &str = "127.0.0.1";

pub struct TokenIndexerInstance {
    // Handle to the spawned pnpm dev process
    pub child: Child,
    pub api_server_url: String,
}

impl TokenIndexerInstance {
    pub async fn new(
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

        let listener = TcpListener::bind((HOST, 0))
            .await
            .expect("Should be able to bind to port");

        let ponder_port = listener
            .local_addr()
            .expect("Should have a local address")
            .port();

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
        let mut pids = vec![pid];

        // Get direct children
        if let Ok(output) = std::process::Command::new("pgrep")
            .arg("-P")
            .arg(pid.to_string())
            .output()
        {
            let children: Vec<u32> = String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| line.trim().parse().ok())
                .collect();
            pids.extend(children);
        }

        // Kill all processes in one command
        let pid_args: Vec<String> = pids.iter().map(|p| p.to_string()).collect();
        let _ = std::process::Command::new("kill").args(&pid_args).output();
    }
}
