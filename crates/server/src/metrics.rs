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
                $(buckets = [$($bucket:expr),+ $(,)?])?
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
                        $(buckets = [$($bucket),+])?
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
    (@create histogram $name:literal $help:literal buckets = [$($bucket:expr),+]) => {
        Histogram::with_opts(
            HistogramOpts::new($name, $help).buckets(vec![$($bucket),+])
        ).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+] buckets = [$($bucket:expr),+]) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets(vec![$($bucket),+]),
            &[$($label),+],
        ).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+] buckets = [$($bucket:expr),+]) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets(vec![$($bucket),+]),
            &[$($label),+],
        ).expect("valid metric")
    };
}

define_metrics! {
    pub struct Metrics {
        counter_vec http_requests_total("http_requests_total")["method", "path", "status"]
            => "Total HTTP requests",
        histogram_vec http_request_duration_seconds("http_request_duration_seconds")["method", "path"]
            buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
            => "HTTP request duration in seconds",
    }
}
