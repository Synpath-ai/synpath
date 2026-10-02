// Times decoding the recorded Polymarket transcript: `cargo run --release --example decode_timing`.
use std::str::FromStr;
use std::time::Instant;

fn time(label: &str, frames: &[String], f: impl Fn(&str)) {
    let rounds = 2000;
    let start = Instant::now();
    for _ in 0..rounds {
        for frame in frames {
            f(frame);
        }
    }
    let per = start.elapsed().as_secs_f64() / (rounds * frames.len()) as f64;
    println!("{label}: {:.2} us per frame", per * 1e6);
}

fn main() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/samples/ws/polymarket_market_transcript.jsonl"
    );
    let text = std::fs::read_to_string(path).unwrap();
    let frames: Vec<String> = text
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|row| row["dir"] == "in" && row["msg"].to_string().contains("price_change"))
        .map(|row| row["msg"].to_string())
        .collect();
    println!(
        "{} price_change frames, {} bytes avg",
        frames.len(),
        frames.iter().map(|f| f.len()).sum::<usize>() / frames.len()
    );
    time("full decode", &frames, |f| {
        std::hint::black_box(synpath_core::polymarket::decode(f).unwrap());
    });
    time("serde_json::Value", &frames, |f| {
        std::hint::black_box(serde_json::from_str::<serde_json::Value>(f).unwrap());
    });
    time("IgnoredAny", &frames, |f| {
        std::hint::black_box(serde_json::from_str::<serde::de::IgnoredAny>(f).unwrap());
    });
    time("4x Decimal::from_str", &frames, |_| {
        for t in ["0.123", "45.5", "0.5", "1000"] {
            std::hint::black_box(rust_decimal::Decimal::from_str(t).unwrap());
        }
    });
}
