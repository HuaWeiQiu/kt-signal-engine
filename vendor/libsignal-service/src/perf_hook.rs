//! KT (send perf): one-way timing hook so the host application can feed
//! vendor-internal send-path segment durations into its own metrics
//! registry. The hook is a plain function pointer installed once per
//! process; with no installation `emit` is a no-op, so the library keeps
//! working standalone.

use std::sync::OnceLock;

pub type Sink = fn(key: &'static str, us: u64);

static SINK: OnceLock<Sink> = OnceLock::new();

/// Install the process-wide sink. Succeeds once; further calls report the
/// already-installed sink back as `Err`.
pub fn install(sink: Sink) -> Result<(), Sink> {
    SINK.set(sink)
}

/// Report one segment duration in microseconds. No-op without installation.
pub fn emit(key: &'static str, us: u64) {
    if let Some(sink) = SINK.get() {
        sink(key, us);
    }
}

/// Segment key literals. These must stay string-equal with the host's
/// metrics word list (kt-signal-engine `perf_metrics::keys`); the host
/// pins the equality with a cross-crate test.
pub mod keys {
    pub const SENDER_SETUP: &str = "send_sender_setup";
    pub const CERT_FETCH: &str = "send_cert_fetch";
    pub const SESSION_ESTABLISH: &str = "send_session_establish";
    pub const ENCRYPT: &str = "send_encrypt";
    pub const WS_RTT: &str = "send_ws_rtt";
}

#[cfg(test)]
mod tests {
    use super::keys;
    use super::{emit, install};
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEEN_VALUES: AtomicU64 = AtomicU64::new(0);

    fn capture(key: &'static str, us: u64) {
        assert_eq!(key, keys::WS_RTT);
        SEEN_VALUES.store(us, Ordering::SeqCst);
    }

    #[test]
    fn emit_without_install_is_a_no_op() {
        emit("anything", 5);
    }

    #[test]
    fn install_once_then_emit_reaches_the_sink() {
        install(capture).expect("first install wins");
        assert!(
            install(capture).is_err(),
            "second install reports the existing sink"
        );

        emit(keys::WS_RTT, 42);
        assert_eq!(SEEN_VALUES.load(Ordering::SeqCst), 42);
    }

    #[test]
    fn segment_key_literals_are_stable() {
        assert_eq!(keys::SENDER_SETUP, "send_sender_setup");
        assert_eq!(keys::CERT_FETCH, "send_cert_fetch");
        assert_eq!(keys::SESSION_ESTABLISH, "send_session_establish");
        assert_eq!(keys::ENCRYPT, "send_encrypt");
        assert_eq!(keys::WS_RTT, "send_ws_rtt");
    }
}
