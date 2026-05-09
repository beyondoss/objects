//! Helpers for spinning up the HTTP server in integration tests on an ephemeral
//! port. The server runs as a detached background task; tests speak to it over
//! reqwest.

use std::sync::Arc;

use anyhow::Result;
use secrecy::ExposeSecret;

use beyond_objects_index::Index;
use beyond_objects_storage::Storage;

use crate::{AppState, Config, build_router, metrics::Metrics};

pub struct TestServer {
    pub url: String,
    pub metrics_url: String,
    pub addr: std::net::SocketAddr,
    pub root_token: String,
}

pub async fn start(config: Config) -> Result<TestServer> {
    crate::telemetry::init_simple("error");

    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.index_dir).await?;

    let storage = Storage::new(&config.data_dir);
    let index = Arc::new(Index::open(&config.index_dir)?);

    // Match production: reconcile the listing index against the filesystem before serving.
    {
        let idx = index.clone();
        let data_dir = config.data_dir.clone();
        tokio::task::spawn_blocking(move || idx.reconcile(&data_dir))
            .await
            .map_err(|e| anyhow::anyhow!("reconcile join: {e}"))??;
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let metrics_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let metrics_addr = metrics_listener.local_addr()?;

    let root_token = config.objects_root_token.expose_secret().clone();

    let state = AppState {
        config: Arc::new(config),
        storage,
        index,
        metrics: Arc::new(Metrics::new()),
    };
    let app = build_router(state.clone());
    let metrics_app = crate::build_metrics_router(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::spawn(async move {
        axum::serve(metrics_listener, metrics_app).await.ok();
    });

    Ok(TestServer {
        url: format!("http://{addr}"),
        metrics_url: format!("http://{metrics_addr}"),
        addr,
        root_token,
    })
}
