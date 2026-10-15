// SPDX-License-Identifier: AGPL-3.0-only

//! 按手机号查找面（KT 扩展方法 `phoneLookup`，E 批）：CDSI（Contact
//! Discovery Service）号码 → 账号解析。
//!
//! 官方行为锚（AGPL 仓行为对照，零抄码，引用只到文件:行）：
//! - CDSI 通路全在 vendored fork/上游原生面：libsignal-service
//!   `src/websocket/directory.rs`（`GET /v2/directory/auth` 自动取凭据 +
//!   `discover_contacts` 经 libsignal-net SGX attested WS 送达查询）、
//!   presage `src/manager/registered.rs` `discover_contacts_by_phone_
//!   number`（Manager 包装，本批随 `cdsi` feature 开启启用）。官方桌面
//!   PhoneNumberLookup（ts/types/PhoneNumber.*）同走 CDSI，限速由服务器
//!   裁决（`retry_after` 秒数诚实回传，调用方自行退避）。
//! - 隐私与联系人纪律（AGENTS.md）：查询结果**只做内存投影即弃**——
//!   不写引擎库、不产生任何 receive/联系人事件；号码到账号的本地映射仍
//!   归 connector 的 contacts 同步链路，本方法只是在线解析器。
//! - 降级语义与 `usernameLookup` 同款：未命中/未注册条目诚实降级
//!   `isRegistered: false` 不炸整批；批级失败只有限速（PHONE_RATE_LIMITED
//!   族）与服务端/连接失败两臂。
//!
//! 出口代理：CDSI 建连走 vendored `directory.rs` 的 ALL_PROXY 注入
//! （2026-10-15 批），与进程其余 reqwest 传输面同源。

use presage::libsignal_service::content::ServiceError;
use presage::libsignal_service::prelude::phonenumber::Mode;
use presage::libsignal_service::prelude::phonenumber::PhoneNumber;
use presage::libsignal_service::protocol::ServiceId;
use presage::manager::{Manager, Registered};
use presage_store_sqlite::SqliteStore;

/// phoneLookup 单批条目上限（usernameLookup 20 条/批帧界同源镜像：CDSI
/// 单请求面同样该收敛——服务器限速阈值不公开，批小是限速礼貌）。
pub(crate) const PHONE_LOOKUP_MAX_ENTRIES: usize = 20;

/// E.164 fail-closed 语法闸：`^\+\d{7,15}$`（+ 打头，7..=15 位纯数字）。
/// 引擎侧唯一权威校验面（正则语义手写展开，零 regex 依赖）。
pub(crate) fn is_valid_e164(number: &str) -> bool {
    let digits = number.strip_prefix('+').unwrap_or("");
    let shape_ok =
        (7..=15).contains(&digits.len()) && digits.bytes().all(|byte| byte.is_ascii_digit());
    shape_ok && digits.len() + 1 == number.len()
}

/// phoneLookup 批级失败族：账号解析梯未命中（-32603，与一切 account
/// 参数面同口径）、限速（附服务器 retry_after 秒数）与服务端/连接失败
/// （细节不外泄）。E.164 非法在参数面即拒（methods.rs），不入本错误族。
#[derive(Debug)]
pub(crate) enum PhoneLookupError {
    UnknownAccount,
    RateLimited { retry_after_seconds: u32 },
    ServerFailed,
}

/// 单条 CDSI 命中：ACI/PNI 二选一（官方 directory.rs 同语义——双命中
/// 罕见且取 ACI，warn 在 vendored 面已记）。
pub(crate) struct ResolvedNumber {
    pub(crate) number: String,
    pub(crate) service_id: Option<ServiceId>,
}

/// phoneLookup 编排：已注册 Manager 的 identified 通路走 presage 官方
/// `discover_contacts_by_phone_number`（CDSI 全链在 vendor/上游原生面），
/// 结果按输入序投影（E.164 文本对齐输入条目，不按返回序）。零落库。
pub(crate) async fn lookup(
    manager: &mut Manager<SqliteStore, Registered>,
    numbers: &[String],
) -> Result<Vec<ResolvedNumber>, PhoneLookupError> {
    let rows = manager
        .discover_contacts_by_phone_number(numbers.iter().map(String::as_str))
        .await
        .map_err(|error| match error {
            presage::Error::ServiceError(ServiceError::CdsiLookupError(
                libsignal_net::cdsi::LookupError::RateLimited(retry_later),
            )) => PhoneLookupError::RateLimited {
                retry_after_seconds: retry_later.retry_after_seconds,
            },
            _ => PhoneLookupError::ServerFailed,
        })?;
    // 返回条目按 E.164 文本对回输入序（内部 TryIntoE164 过滤不会发生——
    // 参数面已闸，防御性对齐而非位置对齐）。
    let mut resolved = Vec::with_capacity(numbers.len());
    for number in numbers {
        let service_id = rows
            .iter()
            .find(|(phone, _)| e164_text(phone) == *number)
            .and_then(|(_, service_id)| *service_id);
        resolved.push(ResolvedNumber {
            number: number.clone(),
            service_id,
        });
    }
    Ok(resolved)
}

/// PhoneNumber → E.164 文本（`+15551234567`；mod.rs e164_string 同构，
/// 本模块自带避免环形依赖面）。
fn e164_text(phone: &PhoneNumber) -> String {
    phone.format().mode(Mode::E164).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// E.164 fail-closed 语法闸逐形状：合法 7..=15 位、越界/缺 +/非数字/
    /// 空段/内嵌空格/超长一律拒。
    #[test]
    fn e164_gate_faces() {
        assert!(is_valid_e164("+15551234567"));
        assert!(is_valid_e164("+1234567")); // 恰 7 位下界
        assert!(is_valid_e164(&format!("+{}", "1".repeat(15)))); // 恰 15 位上界
                                                                 // 缺 + / + 缺数字 / 6 位下界外 / 16 位上界外 / 非数字 / 空串 /
                                                                 // 内嵌空白 / 双 +。
        assert!(!is_valid_e164("15551234567"));
        assert!(!is_valid_e164("+"));
        assert!(!is_valid_e164("+123456"));
        assert!(!is_valid_e164(&format!("+{}", "1".repeat(16))));
        assert!(!is_valid_e164("+1555abc23456"));
        assert!(!is_valid_e164(""));
        assert!(!is_valid_e164("+1 555 123 4567"));
        assert!(!is_valid_e164("++15551234567"));
        assert!(!is_valid_e164("+1555123456７")); // 全角数字非 ASCII
    }
}
