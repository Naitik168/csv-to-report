use tracing_subscriber::{fmt, EnvFilter};

/// Initialise structured logging.
///
/// * `LOG_FORMAT=json` (default) – one JSON object per line, ready for Loki/ELK/Datadog.
/// * `LOG_FORMAT=pretty`         – human-friendly output for local development.
/// * `RUST_LOG`                  – standard env-filter directives (default `info`).
pub fn init(service: &'static str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,lapin=warn,tower_http=info"));
    let format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "json".into());

    if format == "pretty" {
        fmt().with_env_filter(filter).with_target(false).init();
    } else {
        fmt().json().with_env_filter(filter).with_current_span(true).with_span_list(false).flatten_event(true).init();
    }
    tracing::info!(service, "logging initialised");
}

/// Logging for command-line tools: human-readable, to stderr, warnings and errors only by default
/// (override with `STORAGE_ADMIN_LOG`, e.g. `info`). Ignores the services' `LOG_FORMAT`/`RUST_LOG`.
pub fn init_cli() {
    let filter = EnvFilter::try_from_env("STORAGE_ADMIN_LOG").unwrap_or_else(|_| EnvFilter::new("warn"));
    fmt().with_env_filter(filter).with_target(false).with_writer(std::io::stderr).init();
}
