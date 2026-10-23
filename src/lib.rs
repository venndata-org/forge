//! Forge turns GitHub issues into merged code with Claude sessions.
//! See BUILD.md for how the parts fit.

pub mod api;
pub mod attach;
pub mod auth;
pub mod cli;
pub mod config;
pub mod db;
pub mod engine;
pub mod github;
pub mod gitops;
pub mod mcp;
pub mod notify;
pub mod pair;
pub mod pool;
pub mod proxy;
pub mod session;
pub mod slot;
pub mod status;
pub mod tmux;
pub mod types;
