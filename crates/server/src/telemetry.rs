use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider},
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{FmtContext, FormatEvent, FormatFields, format},
    layer::SubscriberExt as _,
    registry::LookupSpan,
    util::SubscriberInitExt as _,
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
        // Flush buffered spans with a hard 5-second deadline. Without this bound,
        // shutdown blocks indefinitely if the OTLP collector is unreachable at exit time.
        let provider = std::mem::replace(&mut self.provider, SdkTracerProvider::builder().build());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(provider.shutdown());
        });
        match rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("error shutting down tracer provider: {e:?}"),
            Err(_) => eprintln!("tracer provider shutdown timed out or panicked"),
        }
    }
}

/// Initialize tracing. When `config.enabled`, exports spans via OTLP. Format is
/// JSON in production, pretty with trace_id prefix when `ENVIRONMENT=development`
/// or `RUST_LOG_FORMAT=pretty`.
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

    let fmt_layer = if is_pretty() {
        tracing_subscriber::fmt::layer()
            .event_format(WithTraceContext(format::Format::default()))
            .boxed()
    } else {
        tracing_subscriber::fmt::layer().json().boxed()
    };

    if let Err(e) = Registry::default()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
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
            .with(tracing_subscriber::fmt::layer())
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

pub fn create_tracer_provider(
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

/// Get the current span's W3C traceparent string, if one is active.
pub fn get_current_traceparent() -> Option<String> {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let ctx = tracing::Span::current().context();
    let span_ctx = ctx.span().span_context().clone();

    span_ctx.is_valid().then(|| {
        format!(
            "00-{}-{}-{:02x}",
            span_ctx.trace_id(),
            span_ctx.span_id(),
            span_ctx.trace_flags().to_u8()
        )
    })
}

/// Extract an OTel context from incoming W3C `traceparent`/`tracestate` headers.
pub fn extract_trace_context(headers: &axum::http::HeaderMap) -> opentelemetry::Context {
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

    impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }
        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }

    static PROPAGATOR: std::sync::LazyLock<TraceContextPropagator> =
        std::sync::LazyLock::new(TraceContextPropagator::new);

    PROPAGATOR.extract(&HeaderExtractor(headers))
}

fn env_filter(default_filter: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter))
}

fn is_pretty() -> bool {
    std::env::var("ENVIRONMENT").is_ok_and(|e| e == "development")
        || std::env::var("RUST_LOG_FORMAT").is_ok_and(|f| f == "pretty")
}

/// Prepends the OTel trace_id to each log line in dev format.
/// Enables log-to-trace correlation (Loki → Tempo via derived field).
struct WithTraceContext<F>(F);

impl<S, N, F> FormatEvent<S, N> for WithTraceContext<F>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        use opentelemetry::trace::TraceContextExt as _;
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let span_ctx = tracing::Span::current().context();
        let span_ctx = span_ctx.span().span_context().clone();

        if span_ctx.is_valid() {
            write!(writer, "trace_id={} ", span_ctx.trace_id())?;
        }

        self.0.format_event(ctx, writer, event)
    }
}
