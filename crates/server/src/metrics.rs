#[allow(unused_imports)]
use prometheus::{
    Counter, CounterVec, Encoder as _, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    Opts, Registry, TextEncoder,
};

macro_rules! define_metrics {
    (
        $(#[$struct_meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $metric_type:ident $field:ident($metric_name:literal)
                $([$($label:literal),+ $(,)?])?
                $(buckets = $buckets:expr)?
                => $help:literal
            ),* $(,)?
        }
    ) => {
        $(#[$struct_meta])*
        $vis struct $name {
            pub registry: Registry,
            $(pub $field: define_metrics!(@field_type $metric_type $([$($label),+])?),)*
        }

        impl $name {
            pub fn new() -> Self {
                let registry = Registry::new();
                $(
                    let $field = define_metrics!(
                        @create $metric_type $metric_name $help
                        $([$($label),+])?
                        $(buckets = $buckets)?
                    );
                    registry.register(Box::new($field.clone())).expect("metric not yet registered");
                )*
                Self { registry, $($field,)* }
            }

            #[allow(dead_code)]
            pub fn registry(&self) -> &Registry { &self.registry }

            pub fn encode(&self) -> String {
                let mut buf = Vec::new();
                TextEncoder::new().encode(&self.registry.gather(), &mut buf)
                    .expect("encoding to vec never fails");
                String::from_utf8(buf).expect("prometheus outputs valid utf-8")
            }
        }

        impl Default for $name {
            fn default() -> Self { Self::new() }
        }
    };

    (@field_type counter) => { Counter };
    (@field_type counter [$($label:literal),+]) => { CounterVec };
    (@field_type counter_vec) => { CounterVec };
    (@field_type counter_vec [$($label:literal),+]) => { CounterVec };
    (@field_type gauge) => { Gauge };
    (@field_type gauge [$($label:literal),+]) => { GaugeVec };
    (@field_type gauge_vec) => { GaugeVec };
    (@field_type gauge_vec [$($label:literal),+]) => { GaugeVec };
    (@field_type histogram) => { Histogram };
    (@field_type histogram [$($label:literal),+]) => { HistogramVec };
    (@field_type histogram_vec) => { HistogramVec };
    (@field_type histogram_vec [$($label:literal),+]) => { HistogramVec };

    (@create counter $name:literal $help:literal) => {
        Counter::new($name, $help).expect("valid metric")
    };
    (@create counter $name:literal $help:literal [$($label:literal),+]) => {
        CounterVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create counter_vec $name:literal $help:literal [$($label:literal),+]) => {
        CounterVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create gauge $name:literal $help:literal) => {
        Gauge::new($name, $help).expect("valid metric")
    };
    (@create gauge $name:literal $help:literal [$($label:literal),+]) => {
        GaugeVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create gauge_vec $name:literal $help:literal [$($label:literal),+]) => {
        GaugeVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal) => {
        Histogram::with_opts(HistogramOpts::new($name, $help)).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal buckets = $buckets:expr) => {
        Histogram::with_opts(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec())
        ).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+] buckets = $buckets:expr) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec()),
            &[$($label),+],
        ).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+] buckets = $buckets:expr) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec()),
            &[$($label),+],
        ).expect("valid metric")
    };
}

// Bucket sets grouped by operation latency profile.

/// HTTP request latency — from 5ms fast-path to 2.5s slow requests.
const HTTP_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];
/// Storage I/O operations — 100µs for cache hits through 5s for large reads/writes.
const STORAGE_BUCKETS: &[f64] = &[
    0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
];
/// Handoff lifecycle durations — drain typically tens of ms, seal sub-second,
/// but allow headroom for unusually slow workloads.
const HANDOFF_BUCKETS: &[f64] = &[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0];

define_metrics! {
    pub struct Metrics {
        counter_vec http_requests_total("http_requests_total")["method", "path", "status"]
            => "Total HTTP requests",
        histogram_vec http_request_duration_seconds("http_request_duration_seconds")["method", "path"]
            buckets = HTTP_BUCKETS
            => "HTTP request duration in seconds",
        gauge http_connections_active("http_connections_active")
            => "Number of HTTP requests currently in flight",
        // op label: write | read | head | delete | copy | move | initiate_multipart | upload_part | complete_multipart | abort_multipart
        histogram_vec storage_operation_seconds("objects_storage_operation_seconds")["op"]
            buckets = STORAGE_BUCKETS
            => "Storage operation duration in seconds",
        counter_vec bytes_uploaded_total("objects_bytes_uploaded_total")["bucket"]
            => "Total bytes uploaded to the object store",
        counter_vec bytes_downloaded_total("objects_bytes_downloaded_total")["bucket"]
            => "Total bytes downloaded from the object store",
        gauge multipart_uploads_active("objects_multipart_uploads_active")
            => "Multipart upload sessions currently in progress",
        // outcome label: completed | aborted
        counter_vec multipart_uploads_total("objects_multipart_uploads_total")["outcome"]
            => "Multipart upload sessions that reached a terminal state (completed or aborted)",
        // result label: committed | seal_failed | resumed
        counter_vec handoff_handoffs_total("handoff_handoffs_total")["result"]
            => "Handoff attempts grouped by outcome",
        counter handoff_seal_failures_total("handoff_seal_failures_total")
            => "Total seal failures during handoff",
        counter handoff_rolled_back_total("handoff_rolled_back_total")
            => "Total handoffs that ran resume_after_abort on the incumbent",
        histogram handoff_drain_seconds("handoff_drain_seconds")
            buckets = HANDOFF_BUCKETS
            => "Wall-clock duration of the drain phase, in seconds",
        histogram handoff_seal_seconds("handoff_seal_seconds")
            buckets = HANDOFF_BUCKETS
            => "Wall-clock duration of the seal phase, in seconds",
    }
}
