use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use forge::config::{read_secret, Config};

#[derive(Parser)]
#[command(name = "forge", version, about = "Turns GitHub issues into merged code with Claude sessions.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon: the loop, the Claude proxy, the dashboard and API.
    Serve {
        #[arg(long, env = "FORGE_CONFIG", default_value = "/etc/forge/forge.toml")]
        config: PathBuf,
    },
    /// Check a config file and the secret files it names.
    CheckConfig {
        #[arg(long, env = "FORGE_CONFIG", default_value = "/etc/forge/forge.toml")]
        config: PathBuf,
    },
    /// Sign in by email and save a token for the other commands.
    Login {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        email: String,
    },
    /// What Forge is doing, and what waits on you.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Run a task's branch on this machine with synthetic data.
    Try {
        issue: u64,
        #[arg(long)]
        repo: Option<String>,
        /// Use your own clone of the repo instead of Forge's (in ~/.cache/forge).
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Fetch and check out the branch only.
        #[arg(long)]
        no_run: bool,
    },
    /// Set this machine up: sign in, add Forge to Claude Code (MCP and skill), check SSH.
    Setup {
        #[arg(long)]
        server: Option<String>,
        #[arg(long)]
        email: Option<String>,
    },
    /// Ask for a task: opens an issue, Asked on the Project.
    New {
        title: String,
        /// What to change and why; "-" reads it from stdin.
        #[arg(long)]
        body: Option<String>,
        #[arg(long)]
        repo: Option<String>,
    },
    /// Move a task to Ready so Forge starts it (the repo's orderer only).
    Ready {
        issue: u64,
        #[arg(long)]
        repo: Option<String>,
    },
    /// Park a task in the Backlog, which Forge never picks up.
    Backlog {
        issue: u64,
        #[arg(long)]
        repo: Option<String>,
    },
    /// Accept a Reviewed task you verified, so Forge merges it.
    Accept {
        issue: u64,
        #[arg(long)]
        repo: Option<String>,
    },
    /// Stop what `forge try` started for a task (the repo's stop command).
    Stop {
        issue: u64,
        #[arg(long)]
        repo: Option<String>,
        /// The clone you passed to `forge try --dir`, if you did.
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    /// Join a task's live Claude session (its tmux session).
    Attach {
        issue: u64,
        #[arg(long)]
        repo: Option<String>,
    },
    /// Watch a task's live Claude session, read-only.
    Watch {
        issue: u64,
        #[arg(long)]
        repo: Option<String>,
    },
    /// Serve Forge's MCP tools on stdio, for your own Claude.
    Mcp,
    /// Run a command holding the box's build slot.
    Slot {
        #[arg(long, env = "FORGE_SLOT", default_value = "/var/lib/forge/build.lock")]
        lock: PathBuf,
        #[arg(trailing_var_arg = true, required = true)]
        command: Vec<String>,
    },
    /// Claude Code's Stop hook in Forge's tmux sessions (server side).
    #[command(hide = true)]
    TurnDone,
    /// Mark a task's folder trusted for the session user's Claude (server side).
    #[command(hide = true)]
    Trust { dir: PathBuf },
    /// The SSH forced command for `forge attach` and `forge watch` (server side).
    AttachGate {
        login: String,
        #[arg(long, env = "FORGE_CONFIG", default_value = "/etc/forge/forge.toml")]
        config: PathBuf,
    },
    /// Write the session user's authorized_keys from members' and collaborators' GitHub keys.
    Keys {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, env = "FORGE_CONFIG", default_value = "/etc/forge/forge.toml")]
        config: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    // `slot` must not start a runtime or log: it wraps a build.
    if let Command::Slot { lock, command } = &cli.command {
        let code = forge::slot::run_in_slot(lock, command)?;
        std::process::exit(code);
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "forge=info".into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run(cli.command))
}

async fn run(command: Command) -> Result<()> {
    match command {
        Command::Serve { config } => serve(Config::load(&config)?).await,
        Command::CheckConfig { config } => check_config(&config),
        Command::Login { server, email } => forge::cli::cmd_login(server, email).await,
        Command::Status { json } => forge::cli::cmd_status(json).await,
        Command::Try { issue, repo, dir, no_run } => forge::cli::cmd_try(repo, issue, dir, no_run).await,
        Command::Stop { issue, repo, dir } => forge::cli::cmd_stop(repo, issue, dir).await,
        Command::New { title, body, repo } => forge::cli::cmd_new(repo, title, body).await,
        Command::Setup { server, email } => forge::cli::cmd_setup(server, email).await,
        Command::Ready { issue, repo } => forge::cli::cmd_move(repo, issue, "Ready").await,
        Command::Accept { issue, repo } => forge::cli::cmd_move(repo, issue, "Accepted").await,
        Command::Backlog { issue, repo } => forge::cli::cmd_move(repo, issue, "Backlog").await,
        Command::Attach { issue, repo } => forge::cli::cmd_attach(repo, issue).await,
        Command::Watch { issue, repo } => forge::cli::cmd_watch(repo, issue).await,
        Command::Mcp => {
            let cfg = forge::cli::ClientConfig::load()?;
            forge::mcp::serve_stdio(forge::cli::Client::from_config(&cfg)?).await
        }
        Command::TurnDone => {
            use std::io::Read as _;
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input)?;
            forge::tmux::turn_done(&input)
        }
        Command::Trust { dir } => forge::tmux::trust(&dir),
        Command::AttachGate { login, config } => {
            let code = forge::attach::attach_gate(&Config::load(&config)?, &login).await?;
            std::process::exit(code);
        }
        Command::Keys { out, config } => {
            let cfg = Config::load(&config)?;
            let gh = forge::github::AppClient::new(&cfg.github)?;
            let n = forge::attach::sync_keys(&cfg, &gh, &out).await?;
            println!("wrote {n} keys to {}", out.display());
            Ok(())
        }
        Command::Slot { .. } => unreachable!("handled before the runtime starts"),
    }
}

fn check_config(path: &std::path::Path) -> Result<()> {
    let cfg = Config::load(path)?;
    read_secret(&cfg.github.private_key_file)?;
    read_secret(&cfg.auth.secret_file)?;
    for plan in &cfg.pool.plans {
        read_secret(&plan.token_file).with_context(|| format!("plan {}", plan.label))?;
    }
    if let Some(t) = &cfg.telegram {
        read_secret(&t.bot_token_file)?;
    }
    if let Some(m) = &cfg.mail {
        read_secret(&m.resend_api_key_file)?;
    }
    for repo in &cfg.repos {
        for (name, role) in &repo.roles {
            std::fs::read_to_string(&role.prompt_file)
                .with_context(|| format!("repo {} role {name}: {}", repo.name, role.prompt_file.display()))?;
        }
    }
    println!("config ok: {} repo(s), {} plan(s)", cfg.repos.len(), cfg.pool.plans.len());
    Ok(())
}

async fn serve(cfg: Config) -> Result<()> {
    use forge::{api, auth, db, engine, github, gitops, notify, pool, proxy, session};

    let cfg = Arc::new(cfg);
    std::fs::create_dir_all(cfg.state_dir()).with_context(|| format!("creating {}", cfg.state_dir().display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(cfg.state_dir(), std::fs::Permissions::from_mode(0o700))?;
    }
    let db = Arc::new(db::Db::open(&cfg.db_path())?);
    // Attach keys live in memory, so nobody is still attached after a restart.
    for task in db.all_tasks()?.iter().filter(|t| t.attached_by.is_some()) {
        db.detach(&task.repo, task.issue)?;
    }
    let pool = Arc::new(pool::Pool::load(&cfg.pool)?);
    pool.restore(db.all_limits()?);
    let keys = proxy::SessionKeys::new();
    let notifier: Arc<dyn notify::Notifier> = Arc::new(notify::LiveNotifier::from_config(&cfg)?);
    let gh: Arc<dyn github::GitHubApi> = Arc::new(github::AppClient::new(&cfg.github)?);
    let git: Arc<dyn gitops::GitOps> = Arc::new(gitops::CliGit {
        data_dir: cfg.data_dir.clone(),
        remote_base: cfg.github.remote_base.clone(),
        author_name: cfg.github.bot_login(),
        author_email: format!("{}+{}@users.noreply.github.com", cfg.github.app_id, cfg.github.bot_login()),
        session_user: cfg.claude.session_user.clone(),
    });
    // With a session user, each task's Claude runs in tmux, for people to watch or join.
    std::fs::create_dir_all(cfg.sessions_dir()).with_context(|| format!("creating {}", cfg.sessions_dir().display()))?;
    let runner: Arc<dyn session::Runner> = match cfg.claude.session_user {
        Some(_) => Arc::new(forge::tmux::TmuxRunner::new(&cfg, keys.clone())),
        None => Arc::new(session::ClaudeRunner::new(&cfg)),
    };
    let secret = read_secret(&cfg.auth.secret_file)?.into_bytes();
    let auth = Arc::new(auth::Auth::new(db.clone(), secret, cfg.clone(), notifier.clone()));

    let proxy_state = proxy::ProxyState {
        pool: pool.clone(),
        keys: keys.clone(),
        db: db.clone(),
        upstream: cfg.pool.upstream.clone(),
        client: reqwest::Client::new(),
    };
    // Every plan's usage stays known, for the dashboard and the pool's choices.
    let checks = proxy_state.clone();
    tokio::spawn(async move {
        loop {
            proxy::check_usage(&checks).await;
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
        }
    });
    let engine = Arc::new(engine::Engine::new(cfg.clone(), db.clone(), gh.clone(), runner, git, notifier, keys.clone()));
    let api_state =
        api::ApiState { cfg: cfg.clone(), db: db.clone(), auth, pool: pool.clone(), keys, busy: engine.busy(), gh: gh.clone(), control: Default::default() };

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let proxy_addr = cfg.claude.proxy_listen.parse().context("claude.proxy_listen")?;
    let api_addr = cfg.listen.parse().context("listen")?;
    let internal_addr = cfg.internal_listen.parse().context("internal_listen")?;

    tracing::info!(repos = cfg.repos.len(), plans = cfg.pool.plans.len(), "forge starting");
    let loop_task = tokio::spawn(engine.clone().run(stop_rx));
    tokio::select! {
        r = proxy::serve(proxy_addr, proxy_state) => r.context("proxy")?,
        r = api::serve(api_addr, api::router(api_state.clone())) => r.context("api")?,
        r = api::serve(internal_addr, api::internal_router(api_state)) => r.context("internal api")?,
        _ = stopped() => tracing::info!("stopping"),
    }
    let _ = stop_tx.send(true);
    let _ = loop_task.await;
    Ok(())
}

/// Resolves on Ctrl-C, or on SIGTERM (how systemd stops a service).
async fn stopped() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            },
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
