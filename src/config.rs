use std::net::SocketAddr;

use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(name = "findb", about = "FinanceDB - Domain specific database for finance")]
pub struct CliArgs {
    /// Path to config file
    #[arg(short, long, default_value = "findb.toml")]
    pub config: String,

    /// Port to listen on (overrides config file)
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Log level (overrides config file)
    #[arg(short, long)]
    pub log_level: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_server")]
    pub server: ServerConfig,

    #[serde(default = "default_logging")]
    pub logging: LoggingConfig,

    #[serde(default)]
    pub auth: AuthConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,

    #[serde(default)]
    pub json: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AuthConfig {
    /// When true, all API endpoints (except /health and /metrics) require authentication.
    #[serde(default)]
    pub enabled: bool,

    /// Static API keys. Each key has a name (for audit) and a role.
    #[serde(default)]
    pub api_keys: Vec<ApiKeyEntry>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiKeyEntry {
    pub name: String,
    pub key: String,
    #[serde(default = "default_role")]
    pub role: String,
}

fn default_role() -> String {
    "reader".to_string()
}

fn default_server() -> ServerConfig {
    ServerConfig {
        host: default_host(),
        port: default_port(),
    }
}

fn default_logging() -> LoggingConfig {
    LoggingConfig {
        level: default_log_level(),
        json: false,
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    3000
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: default_server(),
            logging: default_logging(),
            auth: AuthConfig::default(),
        }
    }
}

impl Config {
    pub fn load(cli: &CliArgs) -> Self {
        let mut config = match std::fs::read_to_string(&cli.config) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_else(|e| {
                eprintln!("Warning: Failed to parse config file: {}", e);
                Config::default()
            }),
            Err(_) => Config::default(),
        };

        // CLI overrides
        if let Some(port) = cli.port {
            config.server.port = port;
        }
        if let Some(ref level) = cli.log_level {
            config.logging.level = level.clone();
        }

        config
    }

    pub fn listen_addr(&self) -> SocketAddr {
        format!("{}:{}", self.server.host, self.server.port)
            .parse()
            .expect("Invalid listen address")
    }
}
