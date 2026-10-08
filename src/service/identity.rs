// SPDX-License-Identifier: AGPL-3.0-only

//! 安全码引擎面：`listIdentities` 读投影与 `trust` 写面（compat §3
//! 两方法行 + §5「安全码引擎面差异」块）。
//!
//! 数字串与 scannable 同源：`Fingerprint::new(version=2, iterations=5200,
//! own_aci_binary, own_aci_key, peer_sid_binary, peer_key)`（官方
//! safetyNumber.preload.ts ITERATION_COUNT=5200/SERVICE_ID_VERSION=2
//! 同参数链；signal-cli NumericFingerprintGenerator(5200).createFor 同构）。
//! `display_string()` 60 位 → 12 组×5 位空格分隔；`scannable.serialize()`
//! = CombinedFingerprints protobuf（官方 QR 码内容，base64 后即
//! scannableSafetyNumber）。
//!
//! trust 态引擎自有 sidecar `<账号目录>/identity-trust.json`（presage
//! store 无 trust 级别与 identity 时间戳位）：`verified` →
//! TRUSTED_VERIFIED、`unverified` → UNTRUSTED、`default`/缺条目 →
//! TRUSTED_UNVERIFIED；`addedTimestamp` = 引擎首次投影建档时刻（store 无
//! 历史可考，与 signal-cli dateAdded 的偏差在 compat §5 登记）。verified
//! 臂以三形态码比对为闸（60 位数字/66 位 hex fingerprint/base64
//! scannable，signal-cli IdentityVerificationCode.parse 同构），比对不过
//! 不落 TRUSTED_VERIFIED。

use std::path::Path;

use presage::libsignal_service::protocol::{
    Fingerprint, IdentityKey, IdentityKeyStore, ProtocolAddress, ServiceId,
};

/// scannable 版本与迭代数（官方 safetyNumber.preload.ts:16-18 同值钉定）。
pub(crate) const FINGERPRINT_VERSION: u32 = 2;
pub(crate) const FINGERPRINT_ITERATIONS: u32 = 5200;

/// trust 态（sidecar 词表小写；读投影词表大写经 [`trust_level_word`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrustState {
    Verified,
    Unverified,
    Default,
}

impl TrustState {
    /// sidecar `state` 词。
    pub(crate) fn word(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Unverified => "unverified",
            Self::Default => "default",
        }
    }

    /// `SyncMessage.verified` state 枚举（verified 同步面自发用）。
    pub(crate) fn sync_state(self) -> presage::proto::verified::State {
        match self {
            Self::Verified => presage::proto::verified::State::Verified,
            Self::Unverified => presage::proto::verified::State::Unverified,
            Self::Default => presage::proto::verified::State::Default,
        }
    }
}

/// 读投影 trustLevel 词：sidecar `verified` → TRUSTED_VERIFIED、
/// `unverified` → UNTRUSTED、`default`/缺条目 → TRUSTED_UNVERIFIED
/// （presage OnNewIdentity::Trust 入库即信任的引擎口径）。
pub(crate) fn trust_level_word(sidecar_state: Option<&str>) -> &'static str {
    match sidecar_state {
        Some("verified") => "TRUSTED_VERIFIED",
        Some("unverified") => "UNTRUSTED",
        _ => "TRUSTED_UNVERIFIED",
    }
}

// ---------------------------------------------------------------------------
// sidecar（identity-trust.json；坏文件/缺键容错——条目级缺席即 default 态）
// ---------------------------------------------------------------------------

const SIDECAR_FILE: &str = "identity-trust.json";

/// 既有条目（state 词, addedAtMs）；文件/键缺席 → None。
fn sidecar_entry(account_dir: &Path, sid: &str) -> Option<(String, u64)> {
    let root: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(account_dir.join(SIDECAR_FILE)).ok()?)
            .ok()?;
    let entry = root.get("identities")?.get(sid)?;
    Some((
        entry.get("state")?.as_str()?.to_string(),
        entry.get("addedAtMs")?.as_u64()?,
    ))
}

/// 建档/迁移（read-modify-write 保其余条目原样；tmp+rename 原子替换，
/// storage-manifest-state.json 先例同构）。写失败 false（trust 写面折叠
/// [`TrustError::Io`]，读建档面静默——档案位不影响主读）。
fn save_trust_state(
    account_dir: &Path,
    sid: &str,
    state: TrustState,
    added_at: u64,
    changed_at: u64,
) -> bool {
    let mut root: serde_json::Value = std::fs::read_to_string(account_dir.join(SIDECAR_FILE))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let Some(object) = root.as_object_mut() else {
        return false;
    };
    let Some(identities) = object
        .entry("identities".to_string())
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
    else {
        return false;
    };
    identities.insert(
        sid.to_string(),
        serde_json::json!({
            "state": state.word(),
            "addedAtMs": added_at,
            "changedAtMs": changed_at,
        }),
    );
    let Ok(bytes) = serde_json::to_vec_pretty(&root) else {
        return false;
    };
    let path = account_dir.join(SIDECAR_FILE);
    let tmp = account_dir.join(format!("{SIDECAR_FILE}.tmp"));
    if std::fs::write(&tmp, bytes).is_err() {
        return false;
    }
    std::fs::rename(&tmp, &path).is_ok()
}

// ---------------------------------------------------------------------------
// 指纹计算与三形态码
// ---------------------------------------------------------------------------

/// 双方指纹（版本/迭代数钉定；id 形参 = 双方 service_id_binary，ACI 16B）。
pub(crate) fn compute_fingerprint(
    own: &ServiceId,
    own_key: &IdentityKey,
    peer: &ServiceId,
    peer_key: &IdentityKey,
) -> Option<Fingerprint> {
    Fingerprint::new(
        FINGERPRINT_VERSION,
        FINGERPRINT_ITERATIONS,
        &own.service_id_binary(),
        own_key,
        &peer.service_id_binary(),
        peer_key,
    )
    .ok()
}

/// 60 位数字串 → 12 组×5 位空格分隔（signal-cli Util.formatSafetyNumber
/// 同排版：自左起每 5 位一组）。
pub(crate) fn format_safety_number(digits: &str) -> String {
    digits
        .as_bytes()
        .chunks(5)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect::<Vec<_>>()
        .join(" ")
}

/// verified 臂三形态（signal-cli IdentityVerificationCode.parse 同构：
/// 仅剥空格后按长度/字符集分流，其余 base64 兜底）。
pub(crate) enum VerificationCode {
    /// 60 位数字串（与未格式化 display 原串直比）。
    Digits(String),
    /// 66 位 hex → 33B identity key 序列（与对端 key serialize 直比）。
    FingerprintKey(Vec<u8>),
    /// base64 CombinedFingerprints protobuf（scannable.compare 臂）。
    Scannable(Vec<u8>),
}

/// 码解析（仅剥 ' '；66 位 hex 只认小写——signal-cli encodeHex 输出面
/// 同口径，其余格式 → None → InvalidCode）。
pub(crate) fn parse_verification_code(raw: &str) -> Option<VerificationCode> {
    let stripped: String = raw.chars().filter(|&c| c != ' ').collect();
    if stripped.len() == 60 && stripped.bytes().all(|b| b.is_ascii_digit()) {
        return Some(VerificationCode::Digits(stripped));
    }
    if stripped.len() == 66 {
        if let Some(key) = crate::service::hex_decode(&stripped) {
            return Some(VerificationCode::FingerprintKey(key));
        }
    }
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(stripped.as_bytes())
        .ok()
        .map(VerificationCode::Scannable)
}

/// 三臂比对（数字臂比对未格式化 60 位原串——signal-cli 同口径；指纹臂 =
/// 对端 identity key 33B 序列全等；scannable 臂 = 本端 scannable 对对端
/// 序列化 compare）。
pub(crate) fn code_matches(
    fingerprint: &Fingerprint,
    peer_key_bytes: &[u8],
    code: &VerificationCode,
) -> bool {
    match code {
        VerificationCode::Digits(digits) => fingerprint
            .display_string()
            .map(|display| display == *digits)
            .unwrap_or(false),
        VerificationCode::FingerprintKey(key) => peer_key_bytes == key.as_slice(),
        VerificationCode::Scannable(scannable) => {
            fingerprint.scannable.compare(scannable).unwrap_or(false)
        }
    }
}

// ---------------------------------------------------------------------------
// 读投影与写面
// ---------------------------------------------------------------------------

/// `trust` 意图（两臂恰一由 methods 层形状闸保证）。
pub(crate) enum TrustIntent {
    /// verified 臂：三形态码比对闸，通过 → TRUSTED_VERIFIED。
    Verified { code: String },
    /// clear 臂：无数字闸（官方 setUnverified/setVerifiedDefault 语义）。
    SetState(TrustState),
}

/// trust 写面错误（methods 层折叠：UnknownAccount/Io → -32603，
/// Failed/InvalidCode → -1 原文位两码）。
#[derive(Debug)]
pub(crate) enum TrustError {
    /// 账号不指向已链接账号。
    UnknownAccount,
    /// 比对失败 / 对端 identity 未知 / store 读取失败。
    Failed,
    /// verified 臂码格式非法。
    InvalidCode,
    /// sidecar 本地写失败。
    Io,
}

/// verified 臂码格式非法错误原文（pinned signal-cli TrustCommand.java
/// UserErrorException 逐字）。
pub(crate) const TRUST_INVALID_CODE_MESSAGE: &str = "Safety number has invalid format, either specify the old hex fingerprint or the new safety number";

/// verified 臂比对失败/identity 未知错误原文（pinned 同上：trustIdentity
/// Verified 返回 false 臂）。
pub(crate) const TRUST_FAILED_MESSAGE: &str = "Failed to set the trust for this number, make sure the number and the fingerprint/safety number are correct.";

/// 写结局（verified 同步自发面所需材料）。
#[derive(Debug)]
pub(crate) struct TrustOutcome {
    /// 对端 identity key 33B 序列。
    pub(crate) peer_key: Vec<u8>,
    pub(crate) state: TrustState,
}

/// `listIdentities` 读投影（单引用窄读；对端 identity 未知 → 空表 =
/// SafetyNumberNotReady 态非错误）。首读建档：sidecar 缺条目以 `default`
/// 态 + now 建（addedTimestamp 语义见模块注释）。
pub(crate) async fn identity_records<K: IdentityKeyStore>(
    store: &K,
    own_aci: &ServiceId,
    peer: ServiceId,
    peer_number: Option<&str>,
    account_dir: &Path,
    now: u64,
) -> Vec<serde_json::Value> {
    let Ok(Some(peer_key)) = store
        .get_identity(&ProtocolAddress::new(
            peer.service_id_string(),
            presage::libsignal_service::protocol::DeviceId::new(1).expect("primary device"),
        ))
        .await
    else {
        return Vec::new();
    };
    let Ok(own_pair) = store.get_identity_key_pair().await else {
        return Vec::new();
    };
    let Some(fingerprint) = compute_fingerprint(own_aci, own_pair.identity_key(), &peer, &peer_key)
    else {
        return Vec::new();
    };

    let sid = peer.service_id_string();
    let (sidecar_state, added_at) = match sidecar_entry(account_dir, &sid) {
        Some((word, added)) => (Some(word), added),
        None => {
            save_trust_state(account_dir, &sid, TrustState::Default, now, now);
            (None, now)
        }
    };

    let mut object = serde_json::Map::new();
    if let Some(number) = peer_number {
        object.insert(
            "number".into(),
            serde_json::Value::String(number.to_string()),
        );
    }
    object.insert("uuid".into(), serde_json::Value::String(sid));
    object.insert(
        "fingerprint".into(),
        serde_json::Value::String(crate::service::hex_encode(&peer_key.serialize())),
    );
    // display/scannable 理论上不可失败（prost encode into Vec 不失败；
    // 长度畸形不可能——SHA512 截断定长），失败键缺席诚实降级。
    if let Ok(display) = fingerprint.display_string() {
        object.insert(
            "safetyNumber".into(),
            serde_json::Value::String(format_safety_number(&display)),
        );
    }
    if let Ok(scannable) = fingerprint.scannable.serialize() {
        use base64::Engine as _;
        object.insert(
            "scannableSafetyNumber".into(),
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(scannable)),
        );
    }
    object.insert(
        "trustLevel".into(),
        serde_json::Value::String(trust_level_word(sidecar_state.as_deref()).to_string()),
    );
    object.insert("addedTimestamp".into(), serde_json::json!(added_at));
    vec![serde_json::Value::Object(object)]
}

/// `trust` 写面（对端 identity 必须已知——两臂同要求；verified 臂比对
/// 不过即不落任何态）。sidecar 权威突变；verified 同步自发归 service 层
/// best-effort（不回滚不报错）。
pub(crate) async fn write_trust<K: IdentityKeyStore>(
    store: &K,
    own_aci: &ServiceId,
    peer: ServiceId,
    intent: &TrustIntent,
    account_dir: &Path,
    now: u64,
) -> Result<TrustOutcome, TrustError> {
    let sid = peer.service_id_string();
    let peer_key = store
        .get_identity(&ProtocolAddress::new(
            sid.clone(),
            presage::libsignal_service::protocol::DeviceId::new(1).expect("primary device"),
        ))
        .await
        .map_err(|_| TrustError::Failed)?
        .ok_or(TrustError::Failed)?;
    let state = match intent {
        TrustIntent::SetState(state) => *state,
        TrustIntent::Verified { code } => {
            let code = parse_verification_code(code).ok_or(TrustError::InvalidCode)?;
            let own_pair = store
                .get_identity_key_pair()
                .await
                .map_err(|_| TrustError::Failed)?;
            let fingerprint =
                compute_fingerprint(own_aci, own_pair.identity_key(), &peer, &peer_key)
                    .ok_or(TrustError::Failed)?;
            if !code_matches(&fingerprint, &peer_key.serialize(), &code) {
                return Err(TrustError::Failed);
            }
            TrustState::Verified
        }
    };
    // addedAtMs 跨态迁移保留（首次建档 = now）。
    let added_at = sidecar_entry(account_dir, &sid)
        .map(|(_, added)| added)
        .unwrap_or(now);
    if !save_trust_state(account_dir, &sid, state, added_at, now) {
        return Err(TrustError::Io);
    }
    Ok(TrustOutcome {
        peer_key: peer_key.serialize().into_vec(),
        state,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::prelude::Uuid;

    // 官方 testVectorsVersion2（libsignal fingerprint.rs 测试常量同源钉定，
    // AGPL 同源只读参照）：钉 Fingerprint::new 参数序（version/iterations/
    // local_id/local_key/remote_id/remote_key——Rust 序与 TS 序 iterations
    // 在前不同）与 V2 scannable 序列化面，防参数漂移。
    const ALICE_IDENTITY_HEX: &str =
        "0506863bc66d02b40d27b8d49ca7c09e9239236f9d7d25d6fcca5ce13c7064d868";
    const BOB_IDENTITY_HEX: &str =
        "05f781b6fb32fed9ba1cf2de978d4d5da28dc34046ae814402b5c0dbd96fda907b";
    const ALICE_STABLE_ID: &str = "+14152222222";
    const BOB_STABLE_ID: &str = "+14153333333";
    const DISPLAYABLE_FINGERPRINT: &str =
        "300354477692869396892869876765458257569162576843440918079131";
    const ALICE_SCANNABLE_V2_HEX: &str = "080212220a201e301a0353dce3dbe7684cb8336e85136cdc0ee96219494ada305d62a7bd61df1a220a20d62cbf73a11592015b6b9f1682ac306fea3aaf3885b84d12bca631e9d4fb3a4d";
    const BOB_SCANNABLE_V2_HEX: &str = "080212220a20d62cbf73a11592015b6b9f1682ac306fea3aaf3885b84d12bca631e9d4fb3a4d1a220a201e301a0353dce3dbe7684cb8336e85136cdc0ee96219494ada305d62a7bd61df";

    // 引擎生产参数链钉样（uuid ServiceId binary 16B + 上列 ALICE/BOB key）：
    // 60 位结果现算后钉死，防版本/迭代/字节序静默漂移。
    const TEST_OWN_UUID: &str = "00000000-0000-0000-0000-000000000001";
    const TEST_PEER_UUID: &str = "00000000-0000-0000-0000-000000000002";
    const TEST_UUID_PAIR_DISPLAY: &str =
        "256971653603451292057831397131962629315236480928702681296151";

    fn key_of(hex: &str) -> IdentityKey {
        IdentityKey::decode(&crate::service::hex_decode(hex).expect("hex")).expect("identity key")
    }

    fn service_id(uuid: &str) -> ServiceId {
        ServiceId::Aci(Uuid::parse_str(uuid).expect("uuid").into())
    }

    fn uuid_pair_fingerprint(
        own: &ServiceId,
        own_key: &IdentityKey,
        peer: &ServiceId,
        peer_key: &IdentityKey,
    ) -> Fingerprint {
        compute_fingerprint(own, own_key, peer, peer_key).expect("fingerprint")
    }

    #[test]
    fn fingerprint_official_vector_v2_pins_parameter_order() {
        let alice = key_of(ALICE_IDENTITY_HEX);
        let bob = key_of(BOB_IDENTITY_HEX);
        let alice_fp = Fingerprint::new(
            FINGERPRINT_VERSION,
            FINGERPRINT_ITERATIONS,
            ALICE_STABLE_ID.as_bytes(),
            &alice,
            BOB_STABLE_ID.as_bytes(),
            &bob,
        )
        .expect("fingerprint");
        // display：官方期望 60 位（V1/V2 display 同值）。
        let display = alice_fp.display_string().expect("display");
        assert_eq!(display, DISPLAYABLE_FINGERPRINT);
        assert_eq!(display.len(), 60);
        // scannable：官方 V2 序列化逐字节（CombinedFingerprints protobuf =
        // 官方 QR 码内容）。
        assert_eq!(
            crate::service::hex_encode(&alice_fp.scannable.serialize().expect("serialize")),
            ALICE_SCANNABLE_V2_HEX
        );
        // 双方互换 digits 相等（对称面）。
        let bob_fp = Fingerprint::new(
            FINGERPRINT_VERSION,
            FINGERPRINT_ITERATIONS,
            BOB_STABLE_ID.as_bytes(),
            &bob,
            ALICE_STABLE_ID.as_bytes(),
            &alice,
        )
        .expect("fingerprint");
        assert_eq!(
            bob_fp.display_string().expect("display"),
            DISPLAYABLE_FINGERPRINT
        );
        assert_eq!(
            crate::service::hex_encode(&bob_fp.scannable.serialize().expect("serialize")),
            BOB_SCANNABLE_V2_HEX
        );
        // 交叉 compare：对端序列化 → true（扫码语义）；自比 → false。
        assert!(alice_fp
            .scannable
            .compare(&bob_fp.scannable.serialize().expect("serialize"))
            .expect("compare"));
        assert!(!alice_fp
            .scannable
            .compare(&alice_fp.scannable.serialize().expect("serialize"))
            .expect("compare"));
    }

    #[test]
    fn compute_fingerprint_uuid_pair_pins_display_and_format() {
        let own = service_id(TEST_OWN_UUID);
        let peer = service_id(TEST_PEER_UUID);
        let own_key = key_of(ALICE_IDENTITY_HEX);
        let peer_key = key_of(BOB_IDENTITY_HEX);
        let fingerprint = uuid_pair_fingerprint(&own, &own_key, &peer, &peer_key);
        let digits = fingerprint.display_string().expect("display");
        assert_eq!(digits.len(), 60);
        assert_eq!(digits, TEST_UUID_PAIR_DISPLAY);
        // 互换双方 digits 相等。
        let swapped = uuid_pair_fingerprint(&peer, &peer_key, &own, &own_key);
        assert_eq!(
            swapped.display_string().expect("display"),
            TEST_UUID_PAIR_DISPLAY
        );
        // 格式化排版：71 字符 = 60 位 + 11 空格，12 组×5 位（signal-cli
        // Util.formatSafetyNumber 同排版）。
        let formatted = format_safety_number(&digits);
        assert_eq!(formatted.len(), 71);
        let groups: Vec<&str> = formatted.split(' ').collect();
        assert_eq!(groups.len(), 12);
        assert!(groups.iter().all(|group| group.len() == 5));
        assert_eq!(formatted.split(' ').collect::<String>(), digits);
    }

    #[test]
    fn verification_code_three_forms_match() {
        let own = service_id(TEST_OWN_UUID);
        let peer = service_id(TEST_PEER_UUID);
        let own_key = key_of(ALICE_IDENTITY_HEX);
        let peer_key = key_of(BOB_IDENTITY_HEX);
        let fingerprint = uuid_pair_fingerprint(&own, &own_key, &peer, &peer_key);
        let digits = fingerprint.display_string().expect("display");
        let peer_bytes = peer_key.serialize().to_vec();
        assert_eq!(peer_bytes.len(), 33);
        // QR 内容 = 对端设备视角的 scannable（对端为 local、本端为 remote）
        // 序列化——本端 compare 对其判 true；本端自序列化自比恒 false
        // （防截图重放本人码的安全属性）。
        let peer_view = Fingerprint::new(
            FINGERPRINT_VERSION,
            FINGERPRINT_ITERATIONS,
            &peer.service_id_binary(),
            &peer_key,
            &own.service_id_binary(),
            &own_key,
        )
        .expect("peer view");
        use base64::Engine as _;
        let scannable_b64 = base64::engine::general_purpose::STANDARD
            .encode(peer_view.scannable.serialize().expect("serialize"));
        let self_b64 = base64::engine::general_purpose::STANDARD
            .encode(fingerprint.scannable.serialize().expect("serialize"));

        // 数字臂：未格式化原串与空格排版两形态同过（signal-cli 比未格式化
        // 60 位原串，空格剥离后同串）。
        let code = parse_verification_code(&digits).expect("digits form");
        assert!(code_matches(&fingerprint, &peer_bytes, &code));
        let formatted = parse_verification_code(&format_safety_number(&digits)).expect("formatted");
        assert!(code_matches(&fingerprint, &peer_bytes, &formatted));
        // 指纹臂：66 位小写 hex = 对端 identity key 33B 序列。
        let fingerprint_hex = crate::service::hex_encode(&peer_bytes);
        assert_eq!(fingerprint_hex.len(), 66);
        let code = parse_verification_code(&fingerprint_hex).expect("hex form");
        assert!(matches!(code, VerificationCode::FingerprintKey(_)));
        assert!(code_matches(&fingerprint, &peer_bytes, &code));
        // scannable 臂：QR base64（CombinedFingermarks protobuf 内容）。
        let code = parse_verification_code(&scannable_b64).expect("base64 form");
        assert!(matches!(code, VerificationCode::Scannable(_)));
        assert!(code_matches(&fingerprint, &peer_bytes, &code));
        // 本端自序列化 → compare false（自比拒绝面）。
        let code = parse_verification_code(&self_b64).expect("self base64 form");
        assert!(!code_matches(&fingerprint, &peer_bytes, &code));
        // 错码逐臂拒；格式非法（非 base64/非 hex/非 60 位数字）→ None。
        let wrong_digits: String = digits
            .chars()
            .map(|c| if c == '0' { '1' } else { '0' })
            .collect();
        let wrong = parse_verification_code(&wrong_digits).expect("60 digits parse");
        assert!(!code_matches(&fingerprint, &peer_bytes, &wrong));
        let wrong_hex = crate::service::hex_encode(&[0u8; 33]);
        let wrong = parse_verification_code(&wrong_hex).expect("hex parse");
        assert!(!code_matches(&fingerprint, &peer_bytes, &wrong));
        assert!(parse_verification_code("short").is_none());
        assert!(parse_verification_code("!!!!").is_none());
        // 剥空格口径：内部空格剥离（仅 ' '），其余字符原样。
        assert!(parse_verification_code(&format!(
            "{} {}",
            &fingerprint_hex[..33],
            &fingerprint_hex[33..]
        ))
        .is_some());
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kt-engine-identity-{tag}-{}-{n}",
            std::process::id()
        ))
    }

    #[test]
    fn trust_sidecar_roundtrip_state_migration_and_isolation() {
        let dir = temp_dir("sidecar");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let sid = service_id(TEST_PEER_UUID).service_id_string();
        let other = service_id(TEST_OWN_UUID).service_id_string();

        // 缺条目 → None（读面 default 兜底口径）。
        assert!(sidecar_entry(&dir, &sid).is_none());
        // 建档 default → 读回 state/addedAt。
        assert!(save_trust_state(
            &dir,
            &sid,
            TrustState::Default,
            1000,
            1000
        ));
        assert_eq!(sidecar_entry(&dir, &sid), Some(("default".into(), 1000)));
        // 迁移 verified：addedAt 保留（write_trust 迁移语义）、changed 更新。
        assert!(save_trust_state(
            &dir,
            &sid,
            TrustState::Verified,
            1000,
            2000
        ));
        assert_eq!(sidecar_entry(&dir, &sid), Some(("verified".into(), 1000)));
        // 再迁 unverified；同文件第二 sid 条目互不干扰（read-modify-write
        // 保其余条目原样）。
        assert!(save_trust_state(
            &dir,
            &other,
            TrustState::Unverified,
            3000,
            3000
        ));
        assert_eq!(sidecar_entry(&dir, &sid), Some(("verified".into(), 1000)));
        assert_eq!(
            sidecar_entry(&dir, &other),
            Some(("unverified".into(), 3000))
        );
        // 原子写后无 tmp 残留。
        assert!(!dir.join("identity-trust.json.tmp").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn trust_sidecar_corrupt_file_tolerated() {
        let dir = temp_dir("sidecar-corrupt");
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(dir.join("identity-trust.json"), "not-json{").expect("write");
        // 坏文件读 → None（容错）；写 → 整体重建不炸。
        assert!(sidecar_entry(&dir, "whatever").is_none());
        assert!(save_trust_state(&dir, "sid", TrustState::Verified, 1, 1));
        assert_eq!(sidecar_entry(&dir, "sid"), Some(("verified".into(), 1)));
        // 根非对象（数组）→ 写面拒绝不动原文件（不静默毁档）。
        std::fs::write(dir.join("identity-trust.json"), "[1,2]").expect("write");
        assert!(!save_trust_state(&dir, "sid", TrustState::Default, 5, 5));
        assert_eq!(sidecar_entry(&dir, "sid"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn trust_level_word_mapping() {
        assert_eq!(trust_level_word(Some("verified")), "TRUSTED_VERIFIED");
        assert_eq!(trust_level_word(Some("unverified")), "UNTRUSTED");
        assert_eq!(trust_level_word(Some("default")), "TRUSTED_UNVERIFIED");
        assert_eq!(trust_level_word(None), "TRUSTED_UNVERIFIED");
        assert_eq!(trust_level_word(Some("bogus")), "TRUSTED_UNVERIFIED");
    }

    // 端到端（store 面）：libsignal 自带 InMemIdentityKeyStore 播种对端
    // identity → identity_records 投影全字段 + write_trust 比对闸/迁移闸。
    fn inmem_store(
        own_key_hex: &str,
        peer_name: &str,
        peer_key_hex: &str,
    ) -> presage::libsignal_service::protocol::InMemIdentityKeyStore {
        use presage::libsignal_service::protocol::{InMemIdentityKeyStore, PrivateKey};
        // identity key 序列 33B 的私钥位不影响指纹（指纹只用公钥位）；
        // 私钥取 32 字节常量保证 own pair 公钥 = 传入 identity key。
        let own_key = key_of(own_key_hex);
        let private = PrivateKey::deserialize(&[7u8; 32]).expect("private key");
        let mut store = InMemIdentityKeyStore::new(
            presage::libsignal_service::protocol::IdentityKeyPair::new(own_key, private),
            1,
        );
        let device = presage::libsignal_service::protocol::DeviceId::new(1).expect("device");
        let peer = key_of(peer_key_hex);
        futures::executor::block_on(
            store.save_identity(&ProtocolAddress::new(peer_name.to_string(), device), &peer),
        )
        .expect("seed peer identity");
        store
    }

    #[test]
    fn identity_records_projects_full_face_and_archives_added_at() {
        let own = service_id(TEST_OWN_UUID);
        let peer = service_id(TEST_PEER_UUID);
        let dir = temp_dir("records");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let store = inmem_store(
            ALICE_IDENTITY_HEX,
            &peer.service_id_string(),
            BOB_IDENTITY_HEX,
        );

        // 首读：全字段投影 + sidecar 首读建档（default 态）。
        let records = futures::executor::block_on(identity_records(
            &store,
            &own,
            peer,
            Some("+15551234567"),
            &dir,
            5000,
        ));
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record["number"], "+15551234567");
        assert_eq!(record["uuid"], peer.service_id_string());
        assert_eq!(
            record["fingerprint"],
            crate::service::hex_encode(&key_of(BOB_IDENTITY_HEX).serialize())
        );
        assert_eq!(
            record["trustLevel"], "TRUSTED_UNVERIFIED",
            "缺条目 default 兜底"
        );
        assert_eq!(record["addedTimestamp"], 5000, "首读建档时刻");
        let safety = record["safetyNumber"].as_str().expect("safetyNumber");
        assert_eq!(safety.len(), 71);
        // scannable 与 Fingerprint 序列化 base64 同值。
        let scannable = fingerprint_of(&store, &own, &peer);
        use base64::Engine as _;
        assert_eq!(
            record["scannableSafetyNumber"],
            base64::engine::general_purpose::STANDARD.encode(scannable)
        );
        assert_eq!(
            sidecar_entry(&dir, &peer.service_id_string()),
            Some(("default".into(), 5000)),
            "首读建档"
        );

        // trust 迁移 verified 后再读：trustLevel/addedTimestamp 跟随。
        assert!(save_trust_state(
            &dir,
            &peer.service_id_string(),
            TrustState::Verified,
            5000,
            6000
        ));
        let records =
            futures::executor::block_on(identity_records(&store, &own, peer, None, &dir, 7000));
        let record = &records[0];
        assert_eq!(record["trustLevel"], "TRUSTED_VERIFIED");
        assert_eq!(record["addedTimestamp"], 5000, "addedAt 跨读保留");
        assert!(record.get("number").is_none(), "无号码形态 number 键不在");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn fingerprint_of(
        store: &presage::libsignal_service::protocol::InMemIdentityKeyStore,
        own: &ServiceId,
        peer: &ServiceId,
    ) -> Vec<u8> {
        let peer_key = futures::executor::block_on(store.get_identity(&ProtocolAddress::new(
            peer.service_id_string(),
            presage::libsignal_service::protocol::DeviceId::new(1).expect("device"),
        )))
        .expect("identity")
        .expect("peer key");
        let own_pair = futures::executor::block_on(store.get_identity_key_pair()).expect("own");
        uuid_pair_fingerprint(own, own_pair.identity_key(), peer, &peer_key)
            .scannable
            .serialize()
            .expect("serialize")
    }

    #[test]
    fn write_trust_verified_gate_and_clear_arm() {
        let own = service_id(TEST_OWN_UUID);
        let peer = service_id(TEST_PEER_UUID);
        let dir = temp_dir("write-trust");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let store = inmem_store(
            ALICE_IDENTITY_HEX,
            &peer.service_id_string(),
            BOB_IDENTITY_HEX,
        );
        let own_key_pair =
            futures::executor::block_on(store.get_identity_key_pair()).expect("own pair");
        let digits = uuid_pair_fingerprint(
            &own,
            own_key_pair.identity_key(),
            &peer,
            &key_of(BOB_IDENTITY_HEX),
        )
        .display_string()
        .expect("display");
        let peer_key_bytes = key_of(BOB_IDENTITY_HEX).serialize().to_vec();

        // verified 臂：比对通过 → 落 verified（addedAt = now）。
        let outcome = match futures::executor::block_on(write_trust(
            &store,
            &own,
            peer,
            &TrustIntent::Verified {
                code: format_safety_number(&digits),
            },
            &dir,
            1000,
        )) {
            Ok(outcome) => outcome,
            Err(error) => panic!("verified write failed: {error:?}"),
        };
        assert_eq!(outcome.peer_key, peer_key_bytes);
        assert_eq!(outcome.state, TrustState::Verified);
        assert_eq!(
            sidecar_entry(&dir, &peer.service_id_string()),
            Some(("verified".into(), 1000))
        );

        // 再次 verified（排版形态）：addedAt 跨态保留。
        futures::executor::block_on(write_trust(
            &store,
            &own,
            peer,
            &TrustIntent::Verified {
                code: digits.clone(),
            },
            &dir,
            2000,
        ))
        .unwrap_or_else(|error| panic!("rewrite failed: {error:?}"));
        assert_eq!(
            sidecar_entry(&dir, &peer.service_id_string()),
            Some(("verified".into(), 1000)),
            "addedAt 迁移保留"
        );

        // 比对失败（他人码）→ Failed 且不落任何态。
        let bob = service_id("00000000-0000-0000-0000-000000000003");
        let bob_pair_own = uuid_pair_fingerprint(
            &bob,
            &key_of(BOB_IDENTITY_HEX),
            &peer,
            &key_of(BOB_IDENTITY_HEX),
        )
        .display_string()
        .expect("display");
        let error = match futures::executor::block_on(write_trust(
            &store,
            &own,
            peer,
            &TrustIntent::Verified { code: bob_pair_own },
            &dir,
            3000,
        )) {
            Ok(outcome) => panic!("mismatch must fail, got {outcome:?}"),
            Err(error) => error,
        };
        assert!(matches!(error, TrustError::Failed));
        assert_eq!(
            sidecar_entry(&dir, &peer.service_id_string()),
            Some(("verified".into(), 1000)),
            "比对失败不改既有态"
        );

        // 格式非法 → InvalidCode。
        let error = futures::executor::block_on(write_trust(
            &store,
            &own,
            peer,
            &TrustIntent::Verified {
                code: "not-a-code".to_string(),
            },
            &dir,
            3000,
        ))
        .expect_err("bad format");
        assert!(matches!(error, TrustError::InvalidCode));

        // clear 臂（default）：无数字闸，态迁移、addedAt 保留。
        let outcome = futures::executor::block_on(write_trust(
            &store,
            &own,
            peer,
            &TrustIntent::SetState(TrustState::Default),
            &dir,
            4000,
        ))
        .expect("clear");
        assert_eq!(outcome.state, TrustState::Default);
        assert_eq!(
            sidecar_entry(&dir, &peer.service_id_string()),
            Some(("default".into(), 1000))
        );

        // 对端 identity 未知（未播种）→ Failed（clear 臂同闸）。
        let stranger = service_id("00000000-0000-0000-0000-000000000004");
        let error = futures::executor::block_on(write_trust(
            &store,
            &own,
            stranger,
            &TrustIntent::SetState(TrustState::Unverified),
            &dir,
            5000,
        ))
        .expect_err("unknown identity");
        assert!(matches!(error, TrustError::Failed));

        // identity_records 对未播种对端 → 空表（非错误）。
        let records = futures::executor::block_on(identity_records(
            &store,
            &own,
            service_id("00000000-0000-0000-0000-000000000004"),
            Some("+15550000000"),
            &dir,
            6000,
        ));
        assert!(records.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
