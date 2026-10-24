// SPDX-License-Identifier: AGPL-3.0-only

//! zkgroup 共享常量面（P4c 切一提升）：生产 GenericServerPublicParams 与
//! legacy ServerPublicParams 单一真相源——云备份凭据链（cloud_backup）与
//! call-link 凭据材料链（call_links）同源消费，不重复嵌常量。

use zkgroup::api::generic_server_params::GenericServerPublicParams;

/// 生产环境 GenericServerPublicParams（legacy V0 形，首字节 0x00；官方
/// config/production.json `genericServerPublicParams` 在 28a0430c59「Upgrade
/// libsignal to 0.101.2」（2026-09-10）前的逐字节值，zkgroup 公钥材料可入库）。
/// 2026-09-10 起官方轮换为 V1 Standard 形（首字节 0x01），仅 libsignal ≥0.101
/// 可解；本仓 zkgroup 钉 v0.99.0（ReservedByte=0 legacy 形），故钉 V0 常量——
/// 服务器对不带 `v101=true` 的请求照旧签发 legacy 族凭据（旧客户端长过渡窗）。
pub const GENERIC_SERVER_PUBLIC_PARAMS_B64: &str = "AByD873dTilmOSG0TjKrvpeaKEsUmIO8Vx9BeMmftwUs9v7ikPwM8P3OHyT0+X3EUMZrSe9VUp26Wai51Q9I8mdk0hX/yo7CeFGJyzoOqn8e/i4Ygbn5HoAyXJx5eXfIbqpc0bIxzju4H/HOQeOpt6h742qii5u/cbwOhFZCsMIbElZTaeU+BWMBQiZHIGHT5IE0qCordQKZ5iPZom0HeFa8Yq0ShuEyAl0WINBiY6xE3H/9WnvzXBbMuuk//eRxXgzO8ieCeK8FwQNxbfXqZm6Ro1cMhCOF3u7xoX83QhpN";

/// 常量反序列化（zkgroup bincode 编码；损坏 = 引擎内嵌常量错误，fail-closed）。
pub fn generic_server_public_params() -> Result<GenericServerPublicParams, ()> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(GENERIC_SERVER_PUBLIC_PARAMS_B64)
        .map_err(|_| ())?;
    zkgroup::deserialize(&bytes).map_err(|_| ())
}

/// legacy zkgroup 服务器公共参数（Production 固定，PushService 同参；官方
/// configuration.rs 同源常量字节，bincode 反序列化）。群凭据 present 面与
/// endorsements 公钥派生面共用。
pub(crate) fn legacy_server_public_params() -> zkgroup::ServerPublicParams {
    use presage::libsignal_service::configuration::{ServiceConfiguration, SignalServers};
    let config: ServiceConfiguration = SignalServers::Production.into();
    config.zkgroup_server_public_params
}

/// EndorsementPublicKey 序列化字节（官方 `serverPublicParams.
/// getEndorsementPublicKey()` 同位，zkgroup pin 树 api/server_params.rs:347；
/// RingRTC getCallLinkCall 的 endorsementPublicKey 槽材料）。常量派生不触网。
pub(crate) fn endorsement_public_key_bytes() -> Vec<u8> {
    zkgroup::serialize(&legacy_server_public_params().get_endorsement_public_key())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 常量面完整性：Generic 公钥可反序列化且与钉定字节 round-trip 一致；
    /// legacy 公钥派生 endorsement 字节跨调用确定（常量面不漂）。
    #[test]
    fn constants_roundtrip_and_deterministic() {
        let params = generic_server_public_params().expect("generic params 常量可解");
        assert_eq!(
            zkgroup::serialize(&params),
            {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD
                    .decode(GENERIC_SERVER_PUBLIC_PARAMS_B64)
                    .expect("常量 b64")
            },
            "序列化 round-trip 逐字节一致（常量面零漂移）"
        );
        let first = endorsement_public_key_bytes();
        let second = endorsement_public_key_bytes();
        assert!(!first.is_empty());
        assert_eq!(first, second, "常量派生跨调用确定");
    }
}
