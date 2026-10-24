// SPDX-License-Identifier: AGPL-3.0-only

//! CDN0 multipart 头像上行共享件（官方 WebAPI makePutParams/uploadAvatar
//! 行为对齐，不抄码）：上行属性七字段校验（verifyAttributes 同构）+ CDN0
//! 根 multipart POST。profile 面（setProfile）与群面（updateGroup avatar）
//! 共用同一形状——官方两链调用同一 makePutParams，字段序逐一同构。

/// 头像上行属性（官方 AvatarUploadAttributes；profile 面来自 `PUT /v1/profile`
/// JSON 响应、群面来自 `GET v2/groups/avatar/form` protobuf 响应，形状一致）。
#[derive(Debug, Clone)]
pub(crate) struct AvatarUploadFields {
    pub(crate) key: String,
    pub(crate) credential: String,
    pub(crate) acl: String,
    pub(crate) algorithm: String,
    pub(crate) date: String,
    pub(crate) policy: String,
    pub(crate) signature: String,
}

impl AvatarUploadFields {
    /// 官方 verifyAttributes 同构：七字段全非空才可用（缺任一 fail-closed）。
    pub(crate) fn verified(self) -> Option<Self> {
        let empty = [
            self.key.is_empty(),
            self.credential.is_empty(),
            self.acl.is_empty(),
            self.algorithm.is_empty(),
            self.date.is_empty(),
            self.policy.is_empty(),
            self.signature.is_empty(),
        ];
        (!empty.iter().any(|field| *field)).then_some(self)
    }
}

/// CDN multipart body（官方 WebAPI makePutParams 逐字节同构）：字段序
/// key / x-amz-credential / acl / x-amz-algorithm / x-amz-date / policy /
/// x-amz-signature / Content-Type（表单字段值 application/octet-stream）+
/// file part（Content-Type: application/octet-stream）；段间 CRLF；结尾
/// `\r\n--{boundary}--\r\n`。
pub(crate) fn multipart_body(
    attrs: &AvatarUploadFields,
    payload: &[u8],
    boundary: &str,
) -> Vec<u8> {
    let section = |name: &str, value: &str| {
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}")
    };
    let mut body = Vec::new();
    body.extend_from_slice(section("key", &attrs.key).as_bytes());
    let fields = [
        ("x-amz-credential", attrs.credential.as_str()),
        ("acl", attrs.acl.as_str()),
        ("x-amz-algorithm", attrs.algorithm.as_str()),
        ("x-amz-date", attrs.date.as_str()),
        ("policy", attrs.policy.as_str()),
        ("x-amz-signature", attrs.signature.as_str()),
        ("Content-Type", "application/octet-stream"),
    ];
    for (name, value) in fields {
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(section(name, value).as_bytes());
    }
    body.extend_from_slice(
        format!("\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"\r\nContent-Type: application/octet-stream\r\n\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(payload);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

/// CDN0 根 POST（官方 uploadAvatar/uploadGroupAvatar 同路径同形状）：成功
/// 任意 2xx，返回头像 path（= 上行属性 `key`）；其余（属性校验失败/网络/
/// 非 2xx）= Err。匿名 CDN（无 basic auth）。payload 由调用方加密（profile
/// 面 iv||AES-GCM(profileKey)、群面 GroupAttributeBlob 密文）。
pub(crate) async fn post_to_cdn0(attrs: AvatarUploadFields, payload: &[u8]) -> Result<String, ()> {
    let fields = attrs.verified().ok_or(())?;
    let mut seed = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut seed);
    let boundary = format!(
        "----------------{}",
        presage::libsignal_service::prelude::Uuid::from_bytes(seed).simple()
    );
    let body = multipart_body(&fields, payload, &boundary);
    let push = presage::libsignal_service::push_service::PushService::new(
        presage::libsignal_service::configuration::SignalServers::Production,
        None,
        crate::service::USER_AGENT,
    );
    let response = push
        .request(
            reqwest::Method::POST,
            presage::libsignal_service::configuration::Endpoint::cdn(0, "/"),
            presage::libsignal_service::push_service::HttpAuthOverride::Unidentified,
        )
        .map_err(|_| ())?
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .map_err(|_| ())?;
    if !response.status().is_success() {
        return Err(());
    }
    Ok(fields.key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> AvatarUploadFields {
        AvatarUploadFields {
            key: "avatars/ABCDEF".to_owned(),
            credential: "cred/20261005/".to_owned(),
            acl: "public-read".to_owned(),
            algorithm: "AWS4-HMAC-SHA256".to_owned(),
            date: "20261005T000000Z".to_owned(),
            policy: "policy-b64".to_owned(),
            signature: "sig-hex".to_owned(),
        }
    }

    /// CDN multipart body 官方 WebAPI makePutParams 逐字节同构钉定（profile
    /// 面先例测试随共享件迁入）：字段序 key/x-amz-credential/acl/
    /// x-amz-algorithm/x-amz-date/policy/x-amz-signature/Content-Type +
    /// file part（application/octet-stream），段间 CRLF、结尾
    /// `--{boundary}--\r\n`。
    #[test]
    fn multipart_body_matches_official_shape() {
        let boundary = "----------------abc123";
        let body = multipart_body(&fields(), b"\x01\x02\x03", boundary);
        let expected = "--{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\navatars/ABCDEF\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-credential\"\r\n\r\ncred/20261005/\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"acl\"\r\n\r\npublic-read\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-algorithm\"\r\n\r\nAWS4-HMAC-SHA256\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-date\"\r\n\r\n20261005T000000Z\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"policy\"\r\n\r\npolicy-b64\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-signature\"\r\n\r\nsig-hex\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"Content-Type\"\r\n\r\napplication/octet-stream\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"file\"\r\nContent-Type: application/octet-stream\r\n\r\n\x01\x02\x03\
\r\n--{b}--\r\n"
            .replace("{b}", boundary);
        assert_eq!(String::from_utf8(body).expect("utf8"), expected);
    }

    /// 上行属性校验（官方 verifyAttributes 同构）：七字段全非空才可用；
    /// 任一字段空 = 拒（逐字段面）。
    #[test]
    fn avatar_upload_fields_require_all_seven() {
        let full = fields();
        assert!(full.clone().verified().is_some());
        for emptied in [
            "key",
            "credential",
            "acl",
            "algorithm",
            "date",
            "policy",
            "signature",
        ] {
            let mut broken = fields();
            match emptied {
                "key" => broken.key = String::new(),
                "credential" => broken.credential = String::new(),
                "acl" => broken.acl = String::new(),
                "algorithm" => broken.algorithm = String::new(),
                "date" => broken.date = String::new(),
                "policy" => broken.policy = String::new(),
                "signature" => broken.signature = String::new(),
                _ => unreachable!(),
            }
            assert!(broken.verified().is_none(), "{emptied} emptied rejects");
        }
    }
}
