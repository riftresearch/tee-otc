use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use clap::{Parser, Subcommand};
use otc_models::constants::CB_BTC_CONTRACT_ADDRESS;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const COINBASE_CANDLES_URL: &str = "https://api.exchange.coinbase.com/products/BTC-USD/candles";
const COINBASE_CANDLE_GRANULARITY_SECONDS: i64 = 60;
const COINBASE_MAX_CANDLES_PER_REQUEST: i64 = 200;
#[derive(Parser, Debug)]
#[command(name = "revenue-export")]
#[command(about = "Export monthly BTC/cbBTC revenue rollups from the OTC replica")]
struct Cli {
    /// Read-only OTC replica Postgres URL.
    #[arg(long, env = "OTC_REPLICA_DATABASE_URL")]
    database_url: String,

    /// Optional legacy OTC Postgres URL for pre-cutover history.
    #[arg(long, env = "OTC_LEGACY_DATABASE_URL")]
    legacy_database_url: Option<String>,

    /// Allow a request to start before the earliest available swap coverage.
    #[arg(long, default_value_t = false)]
    allow_partial_coverage: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Export one monthly revenue CSV row.
    Monthly {
        /// Month to export in YYYY-MM format.
        #[arg(long)]
        month: String,

        /// Timezone used for month boundaries.
        #[arg(long, default_value = "UTC")]
        timezone: String,

        /// Write CSV to a file instead of stdout.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Export one CSV row per month for an inclusive month range.
    Range {
        /// Start month in YYYY-MM format.
        #[arg(long)]
        from_month: String,

        /// End month in YYYY-MM format.
        #[arg(long)]
        to_month: String,

        /// Timezone used for month boundaries.
        #[arg(long, default_value = "UTC")]
        timezone: String,

        /// Write CSV to a file instead of stdout.
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeeAsset {
    Btc,
    CbBtc,
}

#[derive(Debug, Clone)]
struct ConfirmedSwapFeeRow {
    swap_id: String,
    confirmed_at: DateTime<Utc>,
    fee_asset: FeeAsset,
    protocol_fee_sats: i64,
}

#[derive(Debug, Clone, Copy)]
enum SourceSchema {
    Current,
    Legacy,
}

#[derive(Debug)]
struct MonthlySummary {
    month: String,
    timezone: String,
    settled_swap_count: usize,
    btc_fee_sats: i64,
    cbbtc_fee_sats: i64,
    usd_total_cents_at_swap_time: i64,
}

impl MonthlySummary {
    fn new(month: String, timezone: String) -> Self {
        Self {
            month,
            timezone,
            settled_swap_count: 0,
            btc_fee_sats: 0,
            cbbtc_fee_sats: 0,
            usd_total_cents_at_swap_time: 0,
        }
    }

    fn write_csv_header<W: Write>(mut writer: W) -> Result<()> {
        writeln!(
            writer,
            "month,timezone,settled_swap_count,btc_fee_sats,btc_fee_btc,cbbtc_fee_sats,cbbtc_fee_cbbtc,usd_total_at_swap_time"
        )
        .context("failed to write CSV header")?;
        Ok(())
    }

    fn write_csv_row<W: Write>(&self, mut writer: W) -> Result<()> {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{}",
            self.month,
            self.timezone,
            self.settled_swap_count,
            self.btc_fee_sats,
            sats_to_btc_string(self.btc_fee_sats),
            self.cbbtc_fee_sats,
            sats_to_btc_string(self.cbbtc_fee_sats),
            cents_to_usd_string(self.usd_total_cents_at_swap_time),
        )
        .with_context(|| format!("failed to write CSV row for month {}", self.month))?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let primary_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cli.database_url)
        .await
        .context("failed to connect to OTC replica database")?;
    let legacy_pool = match &cli.legacy_database_url {
        Some(url) => Some(
            PgPoolOptions::new()
                .max_connections(5)
                .connect(url)
                .await
                .context("failed to connect to legacy OTC database")?,
        ),
        None => None,
    };

    match cli.command {
        Command::Monthly {
            month,
            timezone,
            output,
        } => {
            let summaries = export_month_range(
                &primary_pool,
                legacy_pool.as_ref(),
                &month,
                &month,
                &timezone,
                cli.allow_partial_coverage,
            )
            .await?;
            write_summaries(&summaries, output)?;
        }
        Command::Range {
            from_month,
            to_month,
            timezone,
            output,
        } => {
            let summaries = export_month_range(
                &primary_pool,
                legacy_pool.as_ref(),
                &from_month,
                &to_month,
                &timezone,
                cli.allow_partial_coverage,
            )
            .await?;
            write_summaries(&summaries, output)?;
        }
    }

    Ok(())
}

async fn export_month_range(
    primary_pool: &PgPool,
    legacy_pool: Option<&PgPool>,
    from_month: &str,
    to_month: &str,
    timezone: &str,
    allow_partial_coverage: bool,
) -> Result<Vec<MonthlySummary>> {
    let tz: Tz = timezone
        .parse()
        .with_context(|| format!("invalid timezone: {timezone}"))?;
    let (start_utc, end_utc, month_labels) = month_range_utc(from_month, to_month, tz)?;
    let primary_coverage_start = load_coverage_start(primary_pool, SourceSchema::Current)
        .await
        .context("failed to determine primary replica coverage")?;
    let legacy_coverage_start = match legacy_pool {
        Some(pool) => Some(
            load_coverage_start(pool, SourceSchema::Legacy)
                .await
                .context("failed to determine legacy replica coverage")?,
        ),
        None => None,
    }
    .flatten();

    let earliest_available = [primary_coverage_start, legacy_coverage_start]
        .into_iter()
        .flatten()
        .min();
    if !allow_partial_coverage {
        if let Some(earliest_available) = earliest_available {
            if start_utc < earliest_available {
                if legacy_pool.is_none() && primary_coverage_start.is_some() {
                    bail!(
                        "requested range starts at {}, but primary replica coverage begins at {}. Provide --legacy-database-url for older history.",
                        start_utc,
                        primary_coverage_start.expect("checked is_some")
                    );
                }

                bail!(
                    "requested range starts at {}, but available coverage begins at {}",
                    start_utc,
                    earliest_available
                );
            }
        }
    }

    let mut summaries = BTreeMap::new();
    for month_label in month_labels {
        summaries.insert(
            month_label.clone(),
            MonthlySummary::new(month_label, timezone.to_string()),
        );
    }

    let mut swaps =
        load_confirmed_swap_fees(primary_pool, SourceSchema::Current, start_utc, end_utc)
            .await
            .context("failed to load current replica swaps")?;
    if let Some(pool) = legacy_pool {
        let mut legacy_swaps =
            load_confirmed_swap_fees(pool, SourceSchema::Legacy, start_utc, end_utc)
                .await
                .context("failed to load legacy replica swaps")?;
        swaps.append(&mut legacy_swaps);
        swaps = dedupe_swaps(swaps)?;
    }

    if swaps.is_empty() {
        return Ok(summaries.into_values().collect());
    }

    let http = reqwest::Client::builder()
        .user_agent("rift-revenue-export/1.0")
        .build()
        .context("failed to build HTTP client")?;

    let price_start = swaps
        .first()
        .map(|swap| floor_to_minute(swap.confirmed_at))
        .expect("swaps is not empty");
    let price_end = swaps
        .last()
        .map(|swap| floor_to_minute(swap.confirmed_at + Duration::minutes(1)))
        .expect("swaps is not empty");
    let price_map = fetch_btc_prices_by_minute(&http, price_start, price_end).await?;

    for swap in swaps {
        let month_label = month_label_for_timestamp(swap.confirmed_at, tz);
        let summary = summaries
            .get_mut(&month_label)
            .with_context(|| format!("missing summary bucket for month {month_label}"))?;
        let minute = floor_to_minute(swap.confirmed_at);
        let price = exact_price(&price_map, minute)
            .with_context(|| format!("missing exact BTC/USD candle for {}", swap.confirmed_at))?;
        let fee_usd_cents = sats_and_price_to_cents(swap.protocol_fee_sats, price)?;

        summary.settled_swap_count += 1;
        summary.usd_total_cents_at_swap_time += fee_usd_cents;

        match swap.fee_asset {
            FeeAsset::Btc => summary.btc_fee_sats += swap.protocol_fee_sats,
            FeeAsset::CbBtc => summary.cbbtc_fee_sats += swap.protocol_fee_sats,
        }
    }

    Ok(summaries.into_values().collect())
}

async fn load_confirmed_swap_fees(
    pool: &PgPool,
    source_schema: SourceSchema,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
) -> Result<Vec<ConfirmedSwapFeeRow>> {
    let query = match source_schema {
        SourceSchema::Current => {
            r#"
            SELECT
                s.id::text AS swap_id,
                (s.user_deposit_status->>'confirmed_at')::timestamptz AS confirmed_at,
                q.from_chain,
                q.from_token->>'type' AS from_token_type,
                lower(q.from_token->>'data') AS from_token_data,
                COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0) AS protocol_fee_sats
            FROM swaps s
            JOIN quotes q ON q.id = s.quote_id
            WHERE s.status = 'settled'
              AND s.realized_swap IS NOT NULL
              AND COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0) > 0
              AND (s.user_deposit_status->>'confirmed_at')::timestamptz >= $1
              AND (s.user_deposit_status->>'confirmed_at')::timestamptz < $2
            ORDER BY confirmed_at ASC
            "#
        }
        SourceSchema::Legacy => {
            r#"
            SELECT
                s.id::text AS swap_id,
                (s.user_deposit_status->>'confirmed_at')::timestamptz AS confirmed_at,
                q.from_chain,
                q.from_token->>'type' AS from_token_type,
                lower(q.from_token->>'data') AS from_token_data,
                COALESCE((q.fee_schedule->>'protocol_fee_sats')::numeric::bigint, 0) AS protocol_fee_sats
            FROM swaps s
            JOIN quotes q ON q.id = s.quote_id
            WHERE s.status = 'settled'
              AND COALESCE((q.fee_schedule->>'protocol_fee_sats')::numeric::bigint, 0) > 0
              AND (s.user_deposit_status->>'confirmed_at')::timestamptz >= $1
              AND (s.user_deposit_status->>'confirmed_at')::timestamptz < $2
            ORDER BY confirmed_at ASC
            "#
        }
    };

    let rows = sqlx::query(query)
        .bind(start_utc)
        .bind(end_utc)
        .fetch_all(pool)
        .await
        .context("failed to query settled swap fees")?;

    let mut swaps = Vec::with_capacity(rows.len());
    for row in rows {
        let swap_id: String = row.try_get("swap_id")?;
        let confirmed_at: DateTime<Utc> = row
            .try_get("confirmed_at")
            .context("missing confirmed_at for settled swap")?;
        let from_chain: String = row.try_get("from_chain")?;
        let from_token_type: String = row.try_get("from_token_type")?;
        let from_token_data: Option<String> = row.try_get("from_token_data")?;
        let protocol_fee_sats: i64 = row.try_get("protocol_fee_sats")?;
        let fee_asset = classify_fee_asset(&from_chain, &from_token_type, from_token_data.as_deref())
            .with_context(|| {
                format!(
                    "failed to classify fee asset for from_chain={from_chain}, from_token_type={from_token_type}"
                )
            })?;

        swaps.push(ConfirmedSwapFeeRow {
            swap_id,
            confirmed_at,
            fee_asset,
            protocol_fee_sats,
        });
    }

    Ok(swaps)
}

async fn load_coverage_start(
    pool: &PgPool,
    source_schema: SourceSchema,
) -> Result<Option<DateTime<Utc>>> {
    let query = match source_schema {
        SourceSchema::Current => {
            r#"
            SELECT min((s.user_deposit_status->>'confirmed_at')::timestamptz) AS confirmed_at
            FROM swaps s
            WHERE s.status = 'settled'
              AND s.realized_swap IS NOT NULL
              AND COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0) > 0
              AND (s.user_deposit_status->>'confirmed_at') IS NOT NULL
            "#
        }
        SourceSchema::Legacy => {
            r#"
            SELECT min((s.user_deposit_status->>'confirmed_at')::timestamptz) AS confirmed_at
            FROM swaps s
            JOIN quotes q ON q.id = s.quote_id
            WHERE s.status = 'settled'
              AND COALESCE((q.fee_schedule->>'protocol_fee_sats')::numeric::bigint, 0) > 0
              AND (s.user_deposit_status->>'confirmed_at') IS NOT NULL
            "#
        }
    };

    let row = sqlx::query(query)
        .fetch_one(pool)
        .await
        .context("failed to query coverage start")?;
    row.try_get("confirmed_at")
        .context("failed to decode coverage start")
}

fn dedupe_swaps(swaps: Vec<ConfirmedSwapFeeRow>) -> Result<Vec<ConfirmedSwapFeeRow>> {
    let mut deduped = BTreeMap::new();
    for swap in swaps {
        match deduped.entry(swap.swap_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(swap);
            }
            Entry::Occupied(entry) => {
                let existing = entry.get();
                if existing.confirmed_at != swap.confirmed_at
                    || existing.fee_asset != swap.fee_asset
                    || existing.protocol_fee_sats != swap.protocol_fee_sats
                {
                    bail!(
                        "duplicate swap {} had conflicting data across sources",
                        swap.swap_id
                    );
                }
            }
        }
    }

    let mut deduped: Vec<_> = deduped.into_values().collect();
    deduped.sort_by_key(|swap| swap.confirmed_at);
    Ok(deduped)
}

fn classify_fee_asset(
    from_chain: &str,
    from_token_type: &str,
    from_token_data: Option<&str>,
) -> Result<FeeAsset> {
    match (from_chain, from_token_type) {
        ("bitcoin", "Native") => Ok(FeeAsset::Btc),
        ("ethereum", "Address") | ("base", "Address") => {
            let token = from_token_data.ok_or_else(|| anyhow!("missing token address"))?;
            if token.eq_ignore_ascii_case(CB_BTC_CONTRACT_ADDRESS) {
                Ok(FeeAsset::CbBtc)
            } else {
                bail!("unexpected token address: {token}");
            }
        }
        _ => bail!("unsupported fee asset source: {from_chain}/{from_token_type}"),
    }
}

async fn fetch_btc_prices_by_minute(
    client: &reqwest::Client,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
) -> Result<BTreeMap<i64, Decimal>> {
    let mut prices = BTreeMap::new();
    let mut chunk_start = floor_to_minute(start_utc);
    let end_minute = floor_to_minute(end_utc);

    while chunk_start < end_minute {
        let chunk_end = std::cmp::min(
            chunk_start + Duration::minutes(COINBASE_MAX_CANDLES_PER_REQUEST),
            end_minute,
        );
        let candles = fetch_coinbase_candle_chunk(client, chunk_start, chunk_end).await?;

        for candle in candles {
            prices.insert(candle.minute.timestamp(), candle.price_usd);
        }

        chunk_start = chunk_end;
    }

    if prices.is_empty() {
        bail!(
            "no BTC/USD candles returned for {}..{}",
            floor_to_minute(start_utc),
            floor_to_minute(end_utc)
        );
    }

    Ok(prices)
}

#[derive(Debug)]
struct Candle {
    minute: DateTime<Utc>,
    price_usd: Decimal,
}

async fn fetch_coinbase_candle_chunk(
    client: &reqwest::Client,
    chunk_start: DateTime<Utc>,
    chunk_end: DateTime<Utc>,
) -> Result<Vec<Candle>> {
    let span_minutes = (chunk_end - chunk_start).num_minutes();
    match fetch_coinbase_candle_chunk_once(client, chunk_start, chunk_end).await {
        Ok(candles) => Ok(candles),
        Err(_err) if span_minutes > 60 => {
            let mut candles = Vec::new();
            let mut subchunk_start = chunk_start;

            while subchunk_start < chunk_end {
                let subchunk_end = std::cmp::min(subchunk_start + Duration::minutes(60), chunk_end);
                let mut subchunk =
                    fetch_coinbase_candle_chunk_once(client, subchunk_start, subchunk_end)
                        .await
                        .with_context(|| {
                            format!(
                                "failed split Coinbase fetch for {}..{} after parent failure",
                                subchunk_start, subchunk_end
                            )
                        })?;
                candles.append(&mut subchunk);
                subchunk_start = subchunk_end;
            }

            Ok(candles)
        }
        Err(err) => Err(err),
    }
}

async fn fetch_coinbase_candle_chunk_once(
    client: &reqwest::Client,
    chunk_start: DateTime<Utc>,
    chunk_end: DateTime<Utc>,
) -> Result<Vec<Candle>> {
    let response = client
        .get(COINBASE_CANDLES_URL)
        .query(&[
            ("start", chunk_start.to_rfc3339()),
            ("end", chunk_end.to_rfc3339()),
            (
                "granularity",
                COINBASE_CANDLE_GRANULARITY_SECONDS.to_string(),
            ),
        ])
        .send()
        .await
        .with_context(|| {
            format!("failed to fetch Coinbase candles for {chunk_start}..{chunk_end}")
        })?;

    if !response.status().is_success() {
        bail!(
            "Coinbase candles request failed for {}..{}: {}",
            chunk_start,
            chunk_end,
            response.status()
        );
    }

    let rows = response
        .json::<Vec<Vec<Value>>>()
        .await
        .context("failed to decode Coinbase candles response")?;

    let mut candles = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(candle) = parse_coinbase_candle_row(&row)? {
            candles.push(candle);
        }
    }

    Ok(candles)
}

fn parse_coinbase_candle_row(row: &[Value]) -> Result<Option<Candle>> {
    if row.len() < 5 {
        return Ok(None);
    }

    let Some(timestamp) = row[0].as_i64() else {
        return Ok(None);
    };
    let Some(minute) = DateTime::from_timestamp(timestamp, 0) else {
        return Ok(None);
    };

    // Coinbase candle rows are [time, low, high, open, close, volume].
    // We use the minute close instead of the low, which understates value.
    let price_usd = decimal_from_json_number(&row[4]).context("invalid Coinbase close price")?;
    Ok(Some(Candle { minute, price_usd }))
}

fn decimal_from_json_number(value: &Value) -> Result<Decimal> {
    match value {
        Value::Number(number) => number
            .to_string()
            .parse::<Decimal>()
            .with_context(|| format!("failed to parse decimal from JSON number {number}")),
        _ => bail!("expected JSON number, got {value}"),
    }
}

fn exact_price(prices: &BTreeMap<i64, Decimal>, minute: DateTime<Utc>) -> Option<Decimal> {
    prices.get(&minute.timestamp()).copied()
}

fn floor_to_minute(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    let seconds = timestamp.timestamp();
    let floored = seconds - seconds.rem_euclid(COINBASE_CANDLE_GRANULARITY_SECONDS);
    DateTime::from_timestamp(floored, 0).expect("rounded timestamp should be valid")
}

fn sats_and_price_to_cents(sats: i64, btc_usd: Decimal) -> Result<i64> {
    let btc_amount = Decimal::from_i128_with_scale(sats as i128, 8);
    let usd_amount = btc_amount * btc_usd;
    (usd_amount * Decimal::from(100))
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .ok_or_else(|| anyhow!("USD cents overflow for sats={sats}, btc_usd={btc_usd}"))
}

fn sats_to_btc_string(sats: i64) -> String {
    let mut btc = Decimal::from_i128_with_scale(sats as i128, 8);
    btc.rescale(8);
    btc.to_string()
}

fn cents_to_usd_string(cents: i64) -> String {
    let mut usd = Decimal::from_i128_with_scale(cents as i128, 2);
    usd.rescale(2);
    usd.to_string()
}

fn month_range_utc(
    from_month: &str,
    to_month: &str,
    tz: Tz,
) -> Result<(DateTime<Utc>, DateTime<Utc>, Vec<String>)> {
    let (start_year, start_month) = parse_year_month(from_month)?;
    let (end_year, end_month) = parse_year_month(to_month)?;

    if (start_year, start_month) > (end_year, end_month) {
        bail!("from-month must be <= to-month");
    }

    let after_end = next_month(end_year, end_month);

    let local_start = tz
        .with_ymd_and_hms(start_year, start_month, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid month start for {from_month} in {tz}"))?;
    let local_end = tz
        .with_ymd_and_hms(after_end.0, after_end.1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid month end for {to_month} in {tz}"))?;

    Ok((
        local_start.with_timezone(&Utc),
        local_end.with_timezone(&Utc),
        month_labels_inclusive(start_year, start_month, end_year, end_month),
    ))
}

fn parse_year_month(input: &str) -> Result<(i32, u32)> {
    let date = NaiveDate::parse_from_str(&format!("{input}-01"), "%Y-%m-%d")
        .with_context(|| format!("invalid month format, expected YYYY-MM: {input}"))?;
    Ok((date.year(), date.month()))
}

fn next_month(year: i32, month: u32) -> (i32, u32) {
    if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    }
}

fn month_labels_inclusive(
    start_year: i32,
    start_month: u32,
    end_year: i32,
    end_month: u32,
) -> Vec<String> {
    let mut labels = Vec::new();
    let mut year = start_year;
    let mut month = start_month;

    loop {
        labels.push(format!("{year:04}-{month:02}"));
        if (year, month) == (end_year, end_month) {
            break;
        }
        (year, month) = next_month(year, month);
    }

    labels
}

fn month_label_for_timestamp(timestamp: DateTime<Utc>, tz: Tz) -> String {
    let local = timestamp.with_timezone(&tz);
    format!("{:04}-{:02}", local.year(), local.month())
}

fn write_summaries(summaries: &[MonthlySummary], output: Option<PathBuf>) -> Result<()> {
    match output {
        Some(path) => {
            let file = File::create(&path)
                .with_context(|| format!("failed to create output file {}", path.display()))?;
            let mut writer = io::BufWriter::new(file);
            MonthlySummary::write_csv_header(&mut writer)?;
            for summary in summaries {
                summary.write_csv_row(&mut writer)?;
            }
            Ok(())
        }
        None => {
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            MonthlySummary::write_csv_header(&mut handle)?;
            for summary in summaries {
                summary.write_csv_row(&mut handle)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_coinbase_candle_close_price() {
        let candle = parse_coinbase_candle_row(&[
            Value::from(1_710_000_000_i64),
            Value::from(62_900.12),
            Value::from(63_100.45),
            Value::from(63_050.00),
            Value::from(63_025.55),
            Value::from(12.34),
        ])
        .expect("parse should succeed")
        .expect("candle should exist");

        assert_eq!(candle.minute.timestamp(), 1_710_000_000);
        assert_eq!(
            candle.price_usd,
            Decimal::from_str_exact("63025.55").unwrap()
        );
    }

    #[test]
    fn rounds_usd_cents_away_from_zero() {
        let cents = sats_and_price_to_cents(300, Decimal::from_str_exact("85000.123456").unwrap())
            .expect("calculation should succeed");

        assert_eq!(cents, 26);
    }

    #[test]
    fn dedupes_identical_swap_rows() {
        let swap = ConfirmedSwapFeeRow {
            swap_id: "swap-1".to_string(),
            confirmed_at: DateTime::from_timestamp(1_710_000_000, 0).unwrap(),
            fee_asset: FeeAsset::Btc,
            protocol_fee_sats: 300,
        };

        let deduped = dedupe_swaps(vec![swap.clone(), swap]).expect("dedupe should succeed");
        assert_eq!(deduped.len(), 1);
    }
}
