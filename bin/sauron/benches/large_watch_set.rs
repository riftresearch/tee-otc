use std::{hint::black_box, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use otc_models::ChainType;
use sauron::{
    benchmarks::{
        benchmark_runtime, make_mixed_watch_entries, make_single_chain_shared_watch_entries,
        populate_watch_store, DEFAULT_BITCOIN_INDEXED_LOOKUP_CONCURRENCY,
        DEFAULT_EVM_INDEXED_LOOKUP_CONCURRENCY, LARGE_WATCH_SET_SIZES,
    },
    discovery::{
        benchmark_schedule_initial_indexed_lookups, benchmark_seed_indexed_lookup_backfill,
    },
    watch::WatchStore,
};

fn bench_watch_store_replace_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("sauron_watch_store_replace_all");
    let runtime = benchmark_runtime();

    for size in LARGE_WATCH_SET_SIZES {
        let entries = make_mixed_watch_entries(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &entries, |b, entries| {
            b.iter_batched(
                || (WatchStore::default(), (*entries).clone()),
                |(store, entries)| {
                    runtime.block_on(async move {
                        store.replace_all(entries).await;
                        black_box(store.len().await)
                    });
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_watch_store_snapshot_for_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("sauron_watch_store_snapshot_for_chain");
    let runtime = benchmark_runtime();

    for size in LARGE_WATCH_SET_SIZES {
        let store = runtime.block_on(populate_watch_store(make_mixed_watch_entries(size)));

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("bitcoin", size), &store, |b, store| {
            b.iter(|| {
                let snapshot = runtime.block_on(store.snapshot_for_chain(ChainType::Bitcoin));
                black_box(snapshot.len())
            });
        });
    }

    group.finish();
}

fn bench_indexed_lookup_backfill_seed(c: &mut Criterion) {
    let mut group = c.benchmark_group("sauron_indexed_lookup_backfill_seed");

    for size in LARGE_WATCH_SET_SIZES {
        let watches = make_single_chain_shared_watch_entries(size, ChainType::Bitcoin);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &watches, |b, watches| {
            b.iter(|| {
                black_box(benchmark_seed_indexed_lookup_backfill(black_box(
                    watches.as_slice(),
                )))
            });
        });
    }

    group.finish();
}

fn bench_indexed_lookup_backfill_schedule(c: &mut Criterion) {
    let mut group = c.benchmark_group("sauron_indexed_lookup_backfill_schedule");

    for size in LARGE_WATCH_SET_SIZES {
        let bitcoin_watches = make_single_chain_shared_watch_entries(size, ChainType::Bitcoin);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("bitcoin_concurrency_32", size),
            &bitcoin_watches,
            |b, watches| {
                b.iter(|| {
                    black_box(benchmark_schedule_initial_indexed_lookups(
                        black_box(watches.as_slice()),
                        DEFAULT_BITCOIN_INDEXED_LOOKUP_CONCURRENCY,
                    ))
                });
            },
        );

        let ethereum_watches = make_single_chain_shared_watch_entries(size, ChainType::Ethereum);
        group.bench_with_input(
            BenchmarkId::new("evm_concurrency_8", size),
            &ethereum_watches,
            |b, watches| {
                b.iter(|| {
                    black_box(benchmark_schedule_initial_indexed_lookups(
                        black_box(watches.as_slice()),
                        DEFAULT_EVM_INDEXED_LOOKUP_CONCURRENCY,
                    ))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1));
    targets =
        bench_watch_store_replace_all,
        bench_watch_store_snapshot_for_chain,
        bench_indexed_lookup_backfill_seed,
        bench_indexed_lookup_backfill_schedule
);
criterion_main!(benches);
