// SPDX-License-Identifier: AGPL-3.0-only

//! F 链路入站观测面：Storage 合并驱动的会话三 flag（mutedUntilTimestamp/
//! archived/markedUnread）变更通知——`receive.conversationState`（wire
//! additive，引擎通知词表第五词；connector 缺口记档 implementation-plan
//! §4.113 的引擎腿）。
//!
//! 背景：引擎的 Storage 面全是按需请求/应答（§4.113 实证），服务器 manifest
//! 修订从不主动到达；入站 merge 路径（storage_merge 的 mergeContacts/
//! importGroups 两族）是远端三 flag 唯一会「路过」引擎的点。本模块把
//! 路过变成观测：merge 读到的每条会话记录与 per-account 簿（进程内存，
//! 不落盘不跨重启）差分，真实翻转合成一条通知。
//!
//! 语义钉定（取舍全量记档，compat §5 F 链路块同源）：
//! - **簿 = 「消费方最后被告知的态」**。三面登记：入站 merge 观测（本模块
//!   observe）、本机写（setConversationState / storage.write 三类记录臂
//!   写后登记，防回声——写方已知，不自我通知）、本机读投影
//!   （getConversationState 应答 = 消费方已知态，登记防冗余通知）。
//! - **首观测 = 种子静默**。引擎重启簿空，首次 merge 全量登记不发通知
//!   （启动风暴闸）；消费方启动期收敛走既有按需读面，不靠通知补齐。
//! - **远端删记录不清簿**：记录消失不产事件（本面不建模记录生命周期；
//!   重建时与最后已知态差分，收敛正确——重建值 ≠ 最后已知才通知）。
//! - **值未变不发**（合并但无翻转 = 零通知）；一次 merge 批的多条翻转
//!   合成一条通知。跨调用时间窗合流**不做**：merge 是 connector 驱动的
//!   离散批（启动期 mergeContacts + importGroups 至多两帧），定时器合流
//!   引入后台任务与投递延迟，换不来的批量收益。
//! - 零内容纪律：通知只携会话标识 + 三位值，无消息内容；零 stderr
//!   （引擎面无日志设施，会话标识亦不落日志）。
//!
//! 有界性（双帽）：
//! - 簿帽 `MAX_TRACKED_CONVERSATIONS`：超帽新会话不登记（fail-open 盲点
//!   ——该会话后续翻转静默，现实账号量级不可及；已登记条目照常差分）。
//! - 批帽 `MAX_FLIPS_PER_NOTIFICATION`：单条通知条目截断 + `overflow:
//!   true` 尾标。取舍（对「合并成全量失效信号」）：截断保留已差分出的
//!   真值立即落消费方簿，尾标保持溢出诚实（消费方要全集可走
//!   getConversationState 逐会话重读）；失效信号把已知真值一并丢弃、
//!   消费代价同为全量重读，故取截断 + 尾标。

use std::collections::HashMap;

use serde_json::{json, Value};

use super::storage_sync::{ConversationState, EngineConversationId};

/// 簿帽（每账号跟踪会话数硬上限）：8192 条目 ×（标识串 + 24B 态 + map
/// 开销）≈ 1MB/账号内，现实账号不可及；超帽 fail-open（模块头注）。
pub(crate) const MAX_TRACKED_CONVERSATIONS: usize = 8192;

/// 批帽（单条通知条目硬上限）：connector 侧列表帽同族量级（pinned 128 /
/// sticker 256，connector §5），256 条约 31KB 单行，远低于连接器 stdout
/// 行长硬闸；超帽截断 + overflow 尾标（模块头注取舍）。
pub(crate) const MAX_FLIPS_PER_NOTIFICATION: usize = 256;

/// 一条真实翻转（观测差分产出）：会话标识 + 变更后三 flag（消费方拿到即
/// 落本机簿，不需要回读）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversationFlagFlip {
    pub(crate) id: EngineConversationId,
    pub(crate) state: ConversationState,
}

/// per-account 会话三 flag 簿（进程内存）：「消费方最后被告知的态」。
/// 键 = 引擎会话标识 canonical 形（观测面 conversation_observation_of 与
/// 写面 canonical 同口径）；值 = 三 flag。
#[derive(Default)]
pub(crate) struct ConversationStateBook {
    seen: HashMap<EngineConversationId, ConversationState>,
}

impl ConversationStateBook {
    /// 入站观测单条：已跟踪且值翻转 → 更新簿 + Some(新态)（= 通知条目）；
    /// 已跟踪同值 → None（值未变不发）；未跟踪 → 帽内登记种子，静默 None
    /// （首观测静默纪律）；帽满未跟踪 → 不登记 None（fail-open 盲点）。
    fn observe(
        &mut self,
        id: &EngineConversationId,
        state: ConversationState,
    ) -> Option<ConversationState> {
        match self.seen.get_mut(id) {
            Some(known) => {
                if *known == state {
                    None
                } else {
                    *known = state;
                    Some(state)
                }
            }
            None => {
                if self.seen.len() < MAX_TRACKED_CONVERSATIONS {
                    self.seen.insert(id.clone(), state);
                }
                None
            }
        }
    }

    /// 消费方已知态登记（本机写后 / 读投影应答）：覆盖或帽内登记，永不
    /// 产翻转（防回声/防冗余的单点入口）。
    pub(crate) fn note_known(&mut self, id: EngineConversationId, state: ConversationState) {
        match self.seen.get_mut(&id) {
            Some(known) => *known = state,
            None => {
                if self.seen.len() < MAX_TRACKED_CONVERSATIONS {
                    self.seen.insert(id, state);
                }
            }
        }
    }
}

/// 一次 merge 批的观测差分：逐条过簿，真实翻转按观测序收进返回vec
/// （簿随批更新——同批重复会话后者与前者的更新后态差分，不自激）。
pub(crate) fn observe_batch(
    book: &mut ConversationStateBook,
    observed: &[(EngineConversationId, ConversationState)],
) -> Vec<ConversationFlagFlip> {
    observed
        .iter()
        .filter_map(|(id, state)| {
            book.observe(id, *state).map(|state| ConversationFlagFlip {
                id: id.clone(),
                state,
            })
        })
        .collect()
}

/// `receive.conversationState` 通知构造（wire additive；receive 车道族
/// 独立方法，account 域，receive.emptied/receive.clockskew 同族同纪律）。
/// 条目形状：conversationId/kind（§4.97 投影口径，与 getConversationState
/// 参数同名直通）+ 三 flag 键恒在（读投影同形状）；overflow 键恒在布尔，
/// 批帽截断时 true（尾标纪律，模块头注取舍）。
pub(crate) fn conversation_state_notification(
    account: &str,
    flips: &[ConversationFlagFlip],
) -> Value {
    let overflow = flips.len() > MAX_FLIPS_PER_NOTIFICATION;
    let shown = &flips[..flips.len().min(MAX_FLIPS_PER_NOTIFICATION)];
    json!({
        "jsonrpc": "2.0",
        "method": "receive.conversationState",
        "params": {
            "account": account,
            "conversations": shown
                .iter()
                .map(|flip| json!({
                    "conversationId": flip.id.id_string(),
                    "kind": flip.id.kind(),
                    "mutedUntilTimestamp": flip.state.muted_until_timestamp,
                    "archived": flip.state.archived,
                    "markedUnread": flip.state.marked_unread,
                }))
                .collect::<Vec<_>>(),
            "overflow": overflow,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contact(id: &str) -> EngineConversationId {
        EngineConversationId::Contact(id.to_owned())
    }

    fn state(muted: u64, archived: bool, unread: bool) -> ConversationState {
        ConversationState {
            muted_until_timestamp: muted,
            archived,
            marked_unread: unread,
        }
    }

    /// 簿语义：首观测种子静默；同值不发；真实翻转产出新态并更新簿（翻转
    /// 后再观测同值不再发）；本机已知态登记防回声（登记后观测同值静默、
    /// 观测异值照常翻转）。
    #[test]
    fn book_seed_silent_flip_once_and_note_suppresses_echo() {
        let mut book = ConversationStateBook::default();
        let id = contact("11111111-2222-4333-8444-555555555555");

        // 首观测 = 种子静默（含非缺省态——重启后首批不炸通知）。
        assert_eq!(book.observe(&id, state(0, true, false)), None);
        // 同值不发。
        assert_eq!(book.observe(&id, state(0, true, false)), None);
        // 真实翻转 → 产新态；簿已更新（再观测同值静默）。
        assert_eq!(
            book.observe(&id, state(9, true, false)),
            Some(state(9, true, false))
        );
        assert_eq!(book.observe(&id, state(9, true, false)), None);

        // 本机写登记（防回声）：登记 X 后观测 X 静默；观测 Y ≠ X 照常翻转
        //（远端在他设备真改的收敛面）。
        let other = contact("22222222-2222-4333-8444-555555555555");
        book.note_known(other.clone(), state(0, false, true));
        assert_eq!(book.observe(&other, state(0, false, true)), None);
        assert_eq!(
            book.observe(&other, state(0, false, false)),
            Some(state(0, false, false))
        );

        // 远端删记录不清簿：重建值 = 最后已知 → 静默；重建值 ≠ 最后已知 →
        // 翻转通知（收敛正确）。
        assert_eq!(book.observe(&id, state(9, true, false)), None);
    }

    /// 批差分：一批多会话合成翻转集（观测序），种子与未变条目不进集。
    #[test]
    fn observe_batch_collects_only_real_flips_in_order() {
        let mut book = ConversationStateBook::default();
        let a = contact("aaaaaaaa-2222-4333-8444-555555555555");
        let b = EngineConversationId::Group("b".repeat(43));
        // 种子批：全静默。
        assert!(observe_batch(
            &mut book,
            &[
                (a.clone(), state(0, false, false)),
                (b.clone(), state(1, false, false))
            ],
        )
        .is_empty());
        // 翻转批：a 翻转 + b 未变 + c 新种子（静默）→ 仅 a 进集。
        let c = contact("cccccccc-2222-4333-8444-555555555555");
        let flips = observe_batch(
            &mut book,
            &[
                (a.clone(), state(7, true, false)),
                (b.clone(), state(1, false, false)),
                (c.clone(), state(0, false, true)),
            ],
        );
        assert_eq!(
            flips,
            vec![ConversationFlagFlip {
                id: a,
                state: state(7, true, false),
            }]
        );
    }

    /// 簿帽：帽满后新会话不登记（翻转静默 = fail-open 盲点）；已登记条目
    /// 照常差分不受帽影响。
    #[test]
    fn book_cap_stops_tracking_new_but_keeps_existing() {
        let mut book = ConversationStateBook::default();
        for index in 0..MAX_TRACKED_CONVERSATIONS {
            book.note_known(
                contact(&format!("{index:08x}-2222-4333-8444-555555555555")),
                state(0, false, false),
            );
        }
        // 已登记条目照常翻转。
        let tracked = contact("00000000-2222-4333-8444-555555555555");
        assert_eq!(
            book.observe(&tracked, state(0, true, false)),
            Some(state(0, true, false))
        );
        // 帽满：新会话首观测即非缺省也静默（不登记不通知）。
        let late = contact("ffffffff-2222-4333-8444-555555555555");
        assert_eq!(book.observe(&late, state(5, true, true)), None);
        assert_eq!(book.observe(&late, state(6, true, true)), None);
    }

    /// 通知形状钉定（词表第五词注册面）：独立方法名 + account 域 +
    /// conversations 条目五键恒在（conversationId/kind/三 flag）+
    /// overflow 恒在；条目序 = 翻转序。
    #[test]
    fn notification_shape_registers_fifth_word() {
        let flips = vec![
            ConversationFlagFlip {
                id: contact("11111111-2222-4333-8444-555555555555"),
                state: state(0, true, false),
            },
            ConversationFlagFlip {
                id: EngineConversationId::Group("b".repeat(43)),
                state: state(1_700_000_000_000, false, true),
            },
        ];
        let notification = conversation_state_notification("acct", &flips);
        assert_eq!(notification["jsonrpc"], json!("2.0"));
        assert_eq!(notification["method"], json!("receive.conversationState"));
        assert_eq!(notification["params"]["account"], json!("acct"));
        assert_eq!(notification["params"]["overflow"], json!(false));
        let conversations = notification["params"]["conversations"]
            .as_array()
            .expect("array");
        assert_eq!(conversations.len(), 2);
        assert_eq!(
            conversations[0]["conversationId"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(conversations[0]["kind"], json!("contact"));
        assert_eq!(conversations[0]["mutedUntilTimestamp"], json!(0));
        assert_eq!(conversations[0]["archived"], json!(true));
        assert_eq!(conversations[0]["markedUnread"], json!(false));
        assert_eq!(conversations[1]["kind"], json!("group"));
        assert_eq!(conversations[1]["conversationId"], json!("b".repeat(43)));
        assert_eq!(
            conversations[1]["mutedUntilTimestamp"],
            json!(1_700_000_000_000_u64)
        );
        // 无信封载体（流态/存储态事件纪律同 receive.emptied）。
        assert!(notification["params"].get("envelope").is_none());
    }

    /// 批帽溢出：条目截断在帽 + overflow=true 尾标；帽内条目不损。
    #[test]
    fn notification_truncates_at_cap_with_overflow_marker() {
        let flips: Vec<ConversationFlagFlip> = (0..MAX_FLIPS_PER_NOTIFICATION + 3)
            .map(|index| ConversationFlagFlip {
                id: contact(&format!("{index:08x}-2222-4333-8444-555555555555")),
                state: state(0, true, false),
            })
            .collect();
        let notification = conversation_state_notification("acct", &flips);
        assert_eq!(notification["params"]["overflow"], json!(true));
        let conversations = notification["params"]["conversations"]
            .as_array()
            .expect("array");
        assert_eq!(conversations.len(), MAX_FLIPS_PER_NOTIFICATION);
        assert_eq!(
            conversations[0]["conversationId"],
            json!("00000000-2222-4333-8444-555555555555")
        );
    }

    /// 消费方测试替身（词表注册验收面）：按钉定契约严格解析通知——方法
    /// 名入词表、params 五面形状逐键验型，坏形状/词表外整拒。
    #[test]
    fn consumer_test_double_parses_notification_strictly() {
        #[derive(Debug, PartialEq, Eq)]
        struct ParsedEntry {
            id: String,
            kind: String,
            muted: u64,
            archived: bool,
            unread: bool,
        }
        fn strict_parse(value: &Value) -> Option<Vec<ParsedEntry>> {
            if value["method"] != "receive.conversationState" {
                return None;
            }
            let params = value["params"].as_object()?;
            if !params["account"].is_string() || !params["overflow"].is_boolean() {
                return None;
            }
            params["conversations"]
                .as_array()?
                .iter()
                .map(|entry| {
                    Some(ParsedEntry {
                        id: entry["conversationId"].as_str()?.to_owned(),
                        kind: entry["kind"].as_str()?.to_owned(),
                        muted: entry["mutedUntilTimestamp"].as_u64()?,
                        archived: entry["archived"].as_bool()?,
                        unread: entry["markedUnread"].as_bool()?,
                    })
                })
                .collect()
        }

        let flips = vec![ConversationFlagFlip {
            id: contact("11111111-2222-4333-8444-555555555555"),
            state: state(42, true, true),
        }];
        let notification = conversation_state_notification("acct", &flips);
        let parsed = strict_parse(&notification).expect("consumer parses");
        assert_eq!(
            parsed,
            vec![ParsedEntry {
                id: "11111111-2222-4333-8444-555555555555".to_owned(),
                kind: "contact".to_owned(),
                muted: 42,
                archived: true,
                unread: true,
            }]
        );
        // 词表外方法名整拒（consumer 侧 default 臂语义）。
        assert!(strict_parse(&json!({"method": "receive.other", "params": {}})).is_none());
    }
}
