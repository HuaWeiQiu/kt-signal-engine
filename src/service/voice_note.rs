// SPDX-License-Identifier: AGPL-3.0-only

//! 出向语音笔记波形+时长生成（send `;voice=true` 面，compat §5 2026-10-14 批）。
//!
//! 官方 wire 形状（Signal-Android `lib/libsignal-service/src/main/protowire/
//! SignalService.proto`，`AttachmentPointer.audioWaveform=21`/
//! `audioDurationSeconds=22`；本仓 vendor proto 已同步）：波形恒
//! [`BAR_COUNT`] 桶、每字节一桶、0=静音 255=最响；时长 float 秒。官方
//! Android 发送侧生成（`AudioWaveFormGenerator`，行为对齐非抄码）：音频
//! 解码为 PCM 后按时长均分 100 桶，逐桶样本均方按 16-bit 满幅（32768²）
//! 归一，再经 dB 尺度映射桶值：`db = 10·log10(meanSquare)`，−60 dB 噪声床
//! 以下归零，`bar = round(255·(db+60)/60)` 截到 [0,255]，空桶 0。
//!
//! 失败姿态（官方 `GenerateAudioWaveFormJob` catch-all 同构）：无法解码 →
//! `None` → 发送面两字段全不带（官方 wire 恒成对，不产「有时长无波形」的
//! 非官方形状），消息照发。解码覆盖矩阵：OGG/Opus（ogg + opus-decoder，
//! 依赖报备见根 Cargo.toml）与 WAV/PCM（仓内自解析）；其余容器（mp3/m4a/
//! ogg-vorbis 等）落 None 姿态。

use std::io::{Cursor, Read as _};

/// 官方桶数（Android `AudioWaveFormGenerator.BAR_COUNT`；proto 注释
/// 「At most 100 bytes/bars」同源）。
const BAR_COUNT: usize = 100;

/// 噪声床 −60 dB（官方 `NOISE_BED_DB`）：低于此均方的桶归 0。
const NOISE_BED_DB: f64 = -60.0;

/// Opus 全带解码原生素率（RFC 7845：输出恒 48 kHz）。
const OPUS_SAMPLE_RATE: u32 = 48_000;

/// Opus 单包最大每声道样本数（120 ms 帧 @ 48 kHz，RFC 6716 上限）。
const OPUS_MAX_FRAME_SAMPLES: usize = 5760;

pub(crate) struct VoiceAudioMetadata {
    pub(crate) waveform: Vec<u8>,
    pub(crate) duration_seconds: f32,
}

/// 附件字节 → 官方形状波形+时长；不可解码容器/坏流 → None（失败姿态）。
pub(crate) fn generate(bytes: &[u8]) -> Option<VoiceAudioMetadata> {
    if bytes.len() >= 4 && &bytes[0..4] == b"OggS" {
        return generate_ogg_opus(bytes);
    }
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        return generate_wav(bytes);
    }
    None
}

/// 官方桶值映射（行为对齐 `meanSquareToBar`）：均方 → dB → 噪声床归零 →
/// 255 满幅 → round + coerce [0,255]。
fn mean_square_to_bar(mean_square: f64) -> u8 {
    if mean_square <= 0.0 {
        return 0;
    }
    let db = 10.0 * mean_square.log10();
    let normalized = ((db - NOISE_BED_DB) / -NOISE_BED_DB).max(0.0);
    (255.0 * normalized).round().clamp(0.0, 255.0) as u8
}

/// 逐桶累加器（行为对齐官方 `BarAccumulator`）：样本按其帧时间戳落桶，
/// 桶内累平方和与样本计数（全声道逐样本计入）。样本以满幅归一值 [−1,1)
/// 进入（等价官方「raw short 平方和 ÷ count ÷ 32768²」，f64 域同值）。
struct BarAccumulator {
    sum_of_squares: [f64; BAR_COUNT],
    sample_counts: [u64; BAR_COUNT],
    total_duration_us: u64,
    sample_rate: u32,
}

impl BarAccumulator {
    /// `total_frames` = 播放域总帧数（时长基），`sample_rate` > 0。
    fn new(total_frames: u64, sample_rate: u32) -> Self {
        Self {
            sum_of_squares: [0.0; BAR_COUNT],
            sample_counts: [0; BAR_COUNT],
            total_duration_us: total_frames * 1_000_000 / u64::from(sample_rate),
            sample_rate,
        }
    }

    /// `frame_index` 为播放域帧号（0 起），`value` 为该样本满幅归一值。
    /// 桶号官方公式（帧时刻 µs × 桶数 ÷ 总时长 µs）；尾部溢出桶 clamp 到
    /// 末桶（官方此处提前 return 弃尾样本——解码样本数与容器时长微差时
    /// 引擎选择全量计入，字节形状差异限最后一桶一两个样本的均方差）。
    fn add_sample(&mut self, frame_index: u64, value: f64) {
        let frame_time_us = frame_index * 1_000_000 / u64::from(self.sample_rate);
        let bar_index = ((frame_time_us * BAR_COUNT as u64) / self.total_duration_us) as usize;
        let bar_index = bar_index.min(BAR_COUNT - 1);
        self.sum_of_squares[bar_index] += value * value;
        self.sample_counts[bar_index] += 1;
    }

    /// 官方 `toWaveForm` 同构：空桶 0，非空桶 meanSquareToBar。
    fn finish(self) -> Vec<u8> {
        (0..BAR_COUNT)
            .map(|bar| {
                if self.sample_counts[bar] == 0 {
                    0
                } else {
                    mean_square_to_bar(self.sum_of_squares[bar] / self.sample_counts[bar] as f64)
                }
            })
            .collect()
    }
}

struct OpusStreamHead {
    serial: u32,
    channels: u8,
    preskip: u16,
}

/// 一遍扫：定位首个 OpusHead 逻辑流（ID 头魔数 `0x7F"OpusHead"`，RFC 7845
/// §4.2）并取该流最大绝对 granule 位置（官方容器时长同源——granule 即
/// 含 pre-skip 的累计帧数）。无 OpusHead / granule 全零（无 EOS 页）→ None。
fn scan_ogg_opus_head(bytes: &[u8]) -> Option<(OpusStreamHead, u64)> {
    let mut reader = ogg::PacketReader::new(Cursor::new(bytes));
    let mut head: Option<OpusStreamHead> = None;
    let mut last_granule = 0u64;
    loop {
        // 坏流 Err 显式失败（None），与解码错误同姿态；Ok(None) = 干净流尾。
        let packet = match reader.read_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(_) => return None,
        };
        match &head {
            Some(head) => {
                if packet.stream_serial() == head.serial {
                    last_granule = last_granule.max(packet.absgp_page());
                }
            }
            None => {
                let data = packet.data.as_slice();
                // OGG 封装 ID 头 = 0x7F 前缀 + 19 字节 OpusHead（RFC 7845
                // §5.1：magic[1..9]/version[9]/channels[10]/pre-skip[11..13]/
                // input rate[13..17]/gain[17..19]/mapping[19]）。
                if data.len() >= 20 && data[0] == 0x7F && &data[1..9] == b"OpusHead" {
                    // 声道闸 1..=2：语音笔记恒单声道/立体声，多声道
                    // multistream 非本单流解码器面（fail-closed）。
                    let channels = data[10];
                    if !(1..=2).contains(&channels) {
                        return None;
                    }
                    let preskip = u16::from_le_bytes([data[11], data[12]]);
                    head = Some(OpusStreamHead {
                        serial: packet.stream_serial(),
                        channels,
                        preskip,
                    });
                }
            }
        }
    }
    let head = head?;
    Some((head, last_granule))
}

/// OGG/Opus 路径：granule − pre-skip = 播放域帧数（时长基，官方容器时长
/// 同源）；二遍解码计桶，头部 pre-skip 帧按 RFC 7845 §4.6 从播放域剔除。
/// 解码中途失败 → None（整段放弃，官方失败姿态恒两字段全不带）。
fn generate_ogg_opus(bytes: &[u8]) -> Option<VoiceAudioMetadata> {
    let (head, last_granule) = scan_ogg_opus_head(bytes)?;
    let total_frames = last_granule.saturating_sub(u64::from(head.preskip));
    if total_frames == 0 {
        // 官方「Zero duration」IOException 同姿态。
        return None;
    }

    let mut decoder =
        opus_decoder::OpusDecoder::new(OPUS_SAMPLE_RATE, usize::from(head.channels)).ok()?;
    let mut accumulator = BarAccumulator::new(total_frames, OPUS_SAMPLE_RATE);
    let mut reader = ogg::PacketReader::new(Cursor::new(bytes));
    let mut pcm = vec![0i16; OPUS_MAX_FRAME_SAMPLES * usize::from(head.channels)];
    let mut preskip_left = u64::from(head.preskip);
    let mut frame_index = 0u64;
    loop {
        // 坏流 Err 显式失败（None）；Ok(None) = 干净流尾。
        let packet = match reader.read_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(_) => return None,
        };
        if packet.stream_serial() != head.serial {
            continue;
        }
        let data = packet.data.as_slice();
        // RFC 7845 头部两包（ID `0x7F"OpusHead"` / 注释 `0x81"OpusTags"`）
        // 不进解码器。
        let is_opus_header = data.len() >= 9
            && (data[0] == 0x7F || data[0] == 0x81)
            && (&data[1..9] == b"OpusHead" || &data[1..9] == b"OpusTags");
        if is_opus_header {
            continue;
        }
        let samples_per_channel = decoder.decode(data, &mut pcm, false).ok()?;
        for sample in 0..samples_per_channel {
            if preskip_left > 0 {
                preskip_left -= 1;
                continue;
            }
            for channel in 0..usize::from(head.channels) {
                let value = f64::from(pcm[sample * usize::from(head.channels) + channel]) / 32768.0;
                accumulator.add_sample(frame_index, value);
            }
            frame_index += 1;
        }
    }
    Some(VoiceAudioMetadata {
        waveform: accumulator.finish(),
        duration_seconds: total_frames as f32 / OPUS_SAMPLE_RATE as f32,
    })
}

/// WAV/PCM 路径（仓内自解析，零依赖）：RIFF 块遍历取 `fmt `/`data`，支持
/// PCM 8/16/24/32-bit 与 IEEE float32（官方 MediaCodec 解码输出 16-bit
/// PCM，各深度按自身满幅归一后与其 32768² 路径同域等值）；截断/零帧/
/// 零采样率/未知格式 → None。
fn generate_wav(bytes: &[u8]) -> Option<VoiceAudioMetadata> {
    let mut fmt: Option<(u16, u16, u32, u16)> = None; // (format, channels, sample_rate, bits)
    let mut data: Option<&[u8]> = None;
    let mut cursor = Cursor::new(bytes);
    cursor.set_position(12); // RIFF 头 + "WAVE"
    while cursor.position() + 8 <= bytes.len() as u64 {
        let mut id = [0u8; 4];
        let mut size_buf = [0u8; 4];
        cursor.read_exact(&mut id).ok()?;
        cursor.read_exact(&mut size_buf).ok()?;
        let size = u32::from_le_bytes(size_buf) as usize;
        let start = cursor.position() as usize;
        let end = start.checked_add(size)?;
        if end > bytes.len() {
            return None;
        }
        match (&id, size) {
            (b"fmt ", _) if size >= 16 => {
                let fields = &bytes[start..end];
                fmt = Some((
                    u16::from_le_bytes([fields[0], fields[1]]),
                    u16::from_le_bytes([fields[2], fields[3]]),
                    u32::from_le_bytes([fields[4], fields[5], fields[6], fields[7]]),
                    u16::from_le_bytes([fields[14], fields[15]]),
                ));
            }
            (b"data", _) => data = Some(&bytes[start..end]),
            _ => {}
        }
        // RIFF 块按 2 字节对齐：奇数长度块带 1 字节 pad。
        cursor.set_position((end + usize::from(size % 2 == 1)) as u64);
    }
    let (format, channels, sample_rate, bits) = fmt?;
    let data = data?;
    let bytes_per_sample = usize::from(bits / 8);
    if sample_rate == 0 || channels == 0 || bytes_per_sample == 0 {
        return None;
    }
    let block_align = usize::from(channels) * bytes_per_sample;
    let frames = data.len() / block_align;
    if frames == 0 {
        return None;
    }
    let decoded = WavSampleDecoder::new(format, bits)?;
    let mut accumulator = BarAccumulator::new(frames as u64, sample_rate);
    for frame in 0..frames {
        for channel in 0..usize::from(channels) {
            let offset = frame * block_align + channel * bytes_per_sample;
            let value = decoded.decode(&data[offset..offset + bytes_per_sample])?;
            accumulator.add_sample(frame as u64, value);
        }
    }
    Some(VoiceAudioMetadata {
        waveform: accumulator.finish(),
        duration_seconds: frames as f32 / sample_rate as f32,
    })
}

/// WAV 样本解码臂（格式×位深白名单；越界/未知臂 None fail-closed）。
struct WavSampleDecoder {
    format: u16,
    bytes_per_sample: usize,
}

impl WavSampleDecoder {
    fn new(format: u16, bits: u16) -> Option<Self> {
        match (format, bits) {
            (1, 8) | (1, 16) | (1, 24) | (1, 32) | (3, 32) => Some(Self {
                format,
                bytes_per_sample: usize::from(bits / 8),
            }),
            _ => None,
        }
    }

    fn decode(&self, raw: &[u8]) -> Option<f64> {
        if raw.len() != self.bytes_per_sample {
            return None;
        }
        match (self.format, self.bytes_per_sample) {
            (1, 1) => Some((f64::from(raw[0]) - 128.0) / 128.0),
            (1, 2) => Some(f64::from(i16::from_le_bytes([raw[0], raw[1]])) / 32768.0),
            (1, 3) => {
                let extended = i32::from_le_bytes([0, raw[0], raw[1], raw[2]]) >> 8; // 符号扩展到 24-bit
                Some(f64::from(extended) / 8_388_608.0)
            }
            (1, 4) => Some(f64::from(i32::from_le_bytes(raw.try_into().ok()?)) / 2_147_483_648.0),
            (3, 4) => Some(f64::from(f32::from_le_bytes(raw.try_into().ok()?))),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 官方桶值映射向量（dB 边界钉形）：0 dB 满幅 255、−60 dB 噪声床 0、
    /// −30 dB 半幅 127.5 → 128（round half away from zero）、静音/超噪床下
    /// 归 0。
    #[test]
    fn mean_square_to_bar_matches_official_db_scale() {
        assert_eq!(mean_square_to_bar(0.0), 0);
        assert_eq!(mean_square_to_bar(1.0), 255); // 0 dB
        assert_eq!(mean_square_to_bar(10f64.powi(-6)), 0); // −60 dB
        assert_eq!(mean_square_to_bar(10f64.powi(-3)), 128); // −30 dB → 127.5
        assert_eq!(mean_square_to_bar(10f64.powi(-2)), 170); // −20 dB → 170
        assert_eq!(mean_square_to_bar(10f64.powi(-1)), 213); // −10 dB → 212.5
        assert_eq!(mean_square_to_bar(10f64.powi(-30)), 0); // −300 dB 远低于噪声床
    }

    /// WAV 全链：合成 PCM16 单声道 8 kHz——前 800 帧 0.5 振幅（均方 0.25
    /// → −6.02 dB → 229），后 800 帧静音（0）；恒 100 桶，前半 229 后半 0，
    /// 时长 0.2 s。
    #[test]
    fn wav_sine_like_pattern_maps_to_expected_bars_and_duration() {
        let sample_rate = 8_000u32;
        let frames = 1_600u32;
        let mut pcm = Vec::with_capacity(frames as usize * 2);
        for frame in 0..frames {
            let value = if frame < 800 { 16384i16 } else { 0 };
            pcm.extend_from_slice(&value.to_le_bytes());
        }
        let bytes = synth_wav(1, 16, sample_rate, &pcm);
        let meta = generate(&bytes).expect("decodes");

        assert_eq!(meta.waveform.len(), 100);
        let expected = mean_square_to_bar(0.25);
        assert_eq!(expected, 229);
        for (bar, value) in meta.waveform.iter().enumerate() {
            if bar < 50 {
                assert_eq!(*value, expected, "bar {bar}");
            } else {
                assert_eq!(*value, 0, "bar {bar}");
            }
        }
        assert!((meta.duration_seconds - 0.2).abs() < 1e-6);
    }

    /// WAV 立体声：两声道同振幅 = 单声道同值（官方逐声道计入后均方同域）。
    #[test]
    fn wav_stereo_channels_counted_like_official() {
        let frames = 800u32;
        let mut pcm = Vec::new();
        for _ in 0..frames {
            pcm.extend_from_slice(&24576i16.to_le_bytes());
            pcm.extend_from_slice(&24576i16.to_le_bytes());
        }
        let bytes = synth_wav(2, 16, 8_000, &pcm);
        let meta = generate(&bytes).expect("decodes");
        assert_eq!(meta.waveform.len(), 100);
        assert_eq!(meta.waveform[0], mean_square_to_bar(0.5625));
        assert!((meta.duration_seconds - 0.1).abs() < 1e-6);
    }

    /// OGG/Opus 全链（管线钉形）：合法 OggS 页结构 + OpusHead + 空音频包
    ///（解码器 PLC 路径，输出静音）——恒 100 桶、时长取 granule−preskip
    /// 基、全 0 桶值。
    #[test]
    fn ogg_opus_stream_produces_full_bar_count_and_granule_duration() {
        let bytes = synth_ogg_opus(96_000, 48_000, 1);
        let meta = generate(&bytes).expect("decodes");
        assert_eq!(meta.waveform.len(), 100);
        assert!(meta.waveform.iter().all(|bar| *bar == 0));
        assert!((meta.duration_seconds - 2.0).abs() < 1e-6);
    }

    /// 失败姿态矩阵：非音频字节、无 OpusHead 的 OGG、截断 WAV、零时长
    /// granule——全 None（发送面两字段全不带）。
    #[test]
    fn undecodable_payloads_fail_closed_to_none() {
        assert!(generate(b"hello world, not audio").is_none());
        assert!(generate(&[]).is_none());
        // OGG 容器但流内无 OpusHead（如纯垃圾页）。
        assert!(generate(b"OggS\x00\x02garbage-truncated-page").is_none());
        // 截断 WAV：fmt 声明 1600 字节 data 只剩 10。
        let mut wav = synth_wav(
            1,
            16,
            8_000,
            &[0i16; 800]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<u8>>(),
        );
        wav.truncate(wav.len() - 10);
        assert!(generate(&wav).is_none());
        // granule 0（无 EOS 页）。
        let bytes = synth_ogg_opus(0, 48_000, 1);
        assert!(generate(&bytes).is_none());
    }

    /// 测试夹具：最小合法 WAV（RIFF/fmt/data 三块，PCM 整型）。
    fn synth_wav(channels: u16, bits: u16, sample_rate: u32, data: &[u8]) -> Vec<u8> {
        let block_align = channels * bits / 8;
        let byte_rate = sample_rate * u32::from(block_align);
        let mut out = Vec::new();
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&((36 + data.len() as u32).to_le_bytes()));
        out.extend_from_slice(b"WAVE");
        out.extend_from_slice(b"fmt ");
        out.extend_from_slice(&16u32.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes()); // PCM
        out.extend_from_slice(&channels.to_le_bytes());
        out.extend_from_slice(&sample_rate.to_le_bytes());
        out.extend_from_slice(&byte_rate.to_le_bytes());
        out.extend_from_slice(&block_align.to_le_bytes());
        out.extend_from_slice(&bits.to_le_bytes());
        out.extend_from_slice(b"data");
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        out
    }

    /// 测试夹具：最小合法 OGG/Opus 流（ogg crate 写出面）——OpusHead
    /// 独页 + `frames` 帧时长的空音频包（PLC 静音），EOS 页 granule =
    /// preskip + `frames`。
    fn synth_ogg_opus(frames: u64, _rate: u32, channels: u8) -> Vec<u8> {
        let preskip = 312u16;
        let mut head = Vec::new();
        head.push(0x7F);
        head.extend_from_slice(b"OpusHead");
        head.push(1); // version
        head.push(channels);
        head.extend_from_slice(&preskip.to_le_bytes());
        head.extend_from_slice(&48_000u32.to_le_bytes()); // input sample rate
        head.extend_from_slice(&0i16.to_le_bytes()); // gain
        head.push(0); // mapping family

        let mut out = Vec::new();
        let mut writer = ogg::PacketWriter::new(&mut out);
        let serial = 0x4B_54_56_4E;
        writer
            .write_packet(head, serial, ogg::PacketWriteEndInfo::EndPage, 0)
            .expect("writes head page");

        // 每 PLC 包 960 帧 @48 kHz = 20 ms； granule = preskip + frames。
        let packet_frames = 960u64;
        let packets = frames.div_ceil(packet_frames);
        let final_granule = u64::from(preskip) + frames;
        for index in 0..packets {
            let info = if index + 1 == packets {
                ogg::PacketWriteEndInfo::EndStream
            } else {
                ogg::PacketWriteEndInfo::NormalPacket
            };
            writer
                .write_packet(Vec::new(), serial, info, final_granule)
                .expect("writes audio packet");
        }
        out
    }
}
