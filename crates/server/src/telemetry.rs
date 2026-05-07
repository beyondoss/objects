use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider},
};
use tracing_subscriber::{
    EnvFilter, Registry, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

pub use opentelemetry::KeyValue;

#[derive(Debug, Clone)]
pub struct OtelConfig {
    pub enabled: bool,
    pub otlp_endpoint: String,
    pub service_name: String,
    pub sample_rate: f64,
}

/// Flushes and shuts down the tracer provider on drop. Hold for the lifetime
/// of the process.
pub struct OtelGuard {
    provider: SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            eprintln!("error shutting down tracer provider: {e:?}");
        }
    }
}

/// Initialize tracing. When `config.enabled`, exports spans via OTLP. Format is
/// JSON in production, pretty in development (`ENVIRONMENT=development` or
/// `RUST_LOG_FORMAT=pretty`).
pub fn init(
    config: &OtelConfig,
    resource_attrs: Vec<KeyValue>,
    default_filter: &str,
) -> anyhow::Result<OtelGuard> {
    let provider = if config.enabled {
        create_tracer_provider(config, resource_attrs)?
    } else {
        SdkTracerProvider::builder().build()
    };

    let tracer = provider.tracer(config.service_name.clone());
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = env_filter(default_filter);

    let subscriber = Registry::default().with(filter).with(otel_layer);

    if is_pretty() {
        if let Err(e) = subscriber
            .with(tracing_subscriber::fmt::layer().pretty())
            .try_init()
        {
            eprintln!("tracing subscriber init failed: {e}");
        }
    } else if let Err(e) = subscriber
        .with(tracing_subscriber::fmt::layer().json())
        .try_init()
    {
        eprintln!("tracing subscriber init failed: {e}");
    }

    Ok(OtelGuard { provider })
}

pub fn init_simple(default_filter: &str) {
    let filter = env_filter(default_filter);
    let result = if is_pretty() {
        Registry::default()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().pretty())
            .try_init()
    } else {
        Registry::default()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .try_init()
    };
    if let Err(e) = result {
        eprintln!("tracing subscriber init failed: {e}");
    }
}

fn create_tracer_provider(
    config: &OtelConfig,
    resource_attrs: Vec<KeyValue>,
) -> anyhow::Result<SdkTracerProvider> {
    let mut attrs = vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ];
    attrs.extend(resource_attrs);

    let resource = Resource::builder_empty().with_attributes(attrs).build();

    let sampler = if config.sample_rate >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sample_rate <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sample_rate)
    };

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build OTLP exporter: {e}"))?;

    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(resource)
        .build())
}

fn env_filter(default_filter: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter))
}

fn is_pretty() -> bool {
    std::env::var("ENVIRONMENT").is_ok_and(|e| e == "development")
        || std::env::var("RUST_LOG_FORMAT").is_ok_and(|f| f == "pretty")
}
