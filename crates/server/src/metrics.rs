use prometheus::{CounterVec, HistogramVec, Opts, Registry, histogram_opts};

pub struct Metrics {
    pub http_requests_total: CounterVec,
    pub http_request_duration_seconds: HistogramVec,
    pub registry: Registry,
}

impl Metrics {
    pub fn try_new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let http_requests_total = CounterVec::new(
            Opts::new("http_requests_total", "Total HTTP requests"),
            &["method", "path", "status"],
        )?;

        let http_request_duration_seconds = HistogramVec::new(
            histogram_opts!(
                "http_request_duration_seconds",
                "HTTP request duration in seconds",
                vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
            ),
            &["method", "path"],
        )?;

        registry.register(Box::new(http_requests_total.clone()))?;
        registry.register(Box::new(http_request_duration_seconds.clone()))?;

        Ok(Self {
            http_requests_total,
            http_request_duration_seconds,
            registry,
        })
    }

    pub fn render(&self) -> Result<String, prometheus::Error> {
        use prometheus::{Encoder, TextEncoder};
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        String::from_utf8(buf)
            .map_err(|e| prometheus::Error::Msg(format!("metrics produced invalid UTF-8: {e}")))
    }
}
