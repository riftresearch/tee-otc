use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

use otc_models::ChainType;
use sauron::{
    benchmarks::{
        benchmark_runtime, make_mixed_watch_entries, make_single_chain_shared_watch_entries,
        populate_watch_store, DEFAULT_BITCOIN_INDEXED_LOOKUP_CONCURRENCY,
        DEFAULT_EVM_INDEXED_LOOKUP_CONCURRENCY, LARGE_WATCH_SET_SIZES,
    },
    discovery::{
        benchmark_prepare_indexed_lookup_backfill, benchmark_prepare_initial_indexed_lookups,
    },
};

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

#[derive(Debug, Clone, Copy)]
struct MeasurementStart {
    baseline_current_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct AllocationReport {
    current_bytes: usize,
    peak_bytes: usize,
    total_allocated_bytes: usize,
    total_deallocated_bytes: usize,
    allocations: usize,
    deallocations: usize,
    reallocations: usize,
}

#[derive(Debug, Clone)]
struct ScenarioRow {
    scenario: &'static str,
    size: usize,
    retained_mib: f64,
    peak_mib: f64,
    total_allocated_mib: f64,
    after_drop_mib: f64,
    allocations: usize,
    note: String,
}

struct TrackingAllocator {
    current_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    total_allocated_bytes: AtomicUsize,
    total_deallocated_bytes: AtomicUsize,
    allocations: AtomicUsize,
    deallocations: AtomicUsize,
    reallocations: AtomicUsize,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            current_bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
            total_allocated_bytes: AtomicUsize::new(0),
            total_deallocated_bytes: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0),
            reallocations: AtomicUsize::new(0),
        }
    }

    fn begin(&self) -> MeasurementStart {
        let baseline_current_bytes = self.current_bytes.load(Ordering::SeqCst);
        self.peak_bytes
            .store(baseline_current_bytes, Ordering::SeqCst);
        self.total_allocated_bytes.store(0, Ordering::SeqCst);
        self.total_deallocated_bytes.store(0, Ordering::SeqCst);
        self.allocations.store(0, Ordering::SeqCst);
        self.deallocations.store(0, Ordering::SeqCst);
        self.reallocations.store(0, Ordering::SeqCst);

        MeasurementStart {
            baseline_current_bytes,
        }
    }

    fn snapshot(&self, start: MeasurementStart) -> AllocationReport {
        let current_bytes = self
            .current_bytes
            .load(Ordering::SeqCst)
            .saturating_sub(start.baseline_current_bytes);
        let peak_bytes = self
            .peak_bytes
            .load(Ordering::SeqCst)
            .saturating_sub(start.baseline_current_bytes);

        AllocationReport {
            current_bytes,
            peak_bytes,
            total_allocated_bytes: self.total_allocated_bytes.load(Ordering::SeqCst),
            total_deallocated_bytes: self.total_deallocated_bytes.load(Ordering::SeqCst),
            allocations: self.allocations.load(Ordering::SeqCst),
            deallocations: self.deallocations.load(Ordering::SeqCst),
            reallocations: self.reallocations.load(Ordering::SeqCst),
        }
    }

    fn observe_peak(&self, current_bytes: usize) {
        let mut peak_bytes = self.peak_bytes.load(Ordering::Relaxed);
        while current_bytes > peak_bytes {
            match self.peak_bytes.compare_exchange_weak(
                peak_bytes,
                current_bytes,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => peak_bytes = observed,
            }
        }
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = layout.size();
            let current_bytes = self.current_bytes.fetch_add(size, Ordering::SeqCst) + size;
            self.total_allocated_bytes.fetch_add(size, Ordering::SeqCst);
            self.allocations.fetch_add(1, Ordering::SeqCst);
            self.observe_peak(current_bytes);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            let size = layout.size();
            let current_bytes = self.current_bytes.fetch_add(size, Ordering::SeqCst) + size;
            self.total_allocated_bytes.fetch_add(size, Ordering::SeqCst);
            self.allocations.fetch_add(1, Ordering::SeqCst);
            self.observe_peak(current_bytes);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        let size = layout.size();
        self.current_bytes.fetch_sub(size, Ordering::SeqCst);
        self.total_deallocated_bytes
            .fetch_add(size, Ordering::SeqCst);
        self.deallocations.fetch_add(1, Ordering::SeqCst);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let old_size = layout.size();
            self.reallocations.fetch_add(1, Ordering::SeqCst);
            if new_size >= old_size {
                let growth = new_size - old_size;
                let current_bytes = self.current_bytes.fetch_add(growth, Ordering::SeqCst) + growth;
                self.total_allocated_bytes
                    .fetch_add(new_size, Ordering::SeqCst);
                self.observe_peak(current_bytes);
            } else {
                let shrink = old_size - new_size;
                self.current_bytes.fetch_sub(shrink, Ordering::SeqCst);
                self.total_deallocated_bytes
                    .fetch_add(shrink, Ordering::SeqCst);
            }
        }
        new_ptr
    }
}

fn main() {
    let runtime = benchmark_runtime();
    let mut rows = Vec::new();

    for size in LARGE_WATCH_SET_SIZES {
        rows.push(measure_replace_all(size, &runtime));
        rows.push(measure_snapshot_for_chain(size, &runtime));
        rows.push(measure_indexed_lookup_backfill_seed(size));
        rows.push(measure_indexed_lookup_backfill_schedule(
            size,
            ChainType::Bitcoin,
            DEFAULT_BITCOIN_INDEXED_LOOKUP_CONCURRENCY,
        ));
        rows.push(measure_indexed_lookup_backfill_schedule(
            size,
            ChainType::Ethereum,
            DEFAULT_EVM_INDEXED_LOOKUP_CONCURRENCY,
        ));
    }

    print_rows(&rows);
}

fn measure_replace_all(size: usize, runtime: &tokio::runtime::Runtime) -> ScenarioRow {
    let start = ALLOCATOR.begin();
    let store = runtime.block_on(async {
        let entries = make_mixed_watch_entries(size);
        populate_watch_store(entries).await
    });
    let watch_count = runtime.block_on(store.len());
    let live = ALLOCATOR.snapshot(start);
    drop(store);
    let dropped = ALLOCATOR.snapshot(start);

    ScenarioRow {
        scenario: "replace_all",
        size,
        retained_mib: mib(live.current_bytes),
        peak_mib: mib(live.peak_bytes),
        total_allocated_mib: mib(live.total_allocated_bytes),
        after_drop_mib: mib(dropped.current_bytes),
        allocations: live.allocations,
        note: format!(
            "watches={watch_count}, deallocated_mib={:.2}, deallocations={}, reallocations={}",
            mib(live.total_deallocated_bytes),
            live.deallocations,
            live.reallocations
        ),
    }
}

fn measure_snapshot_for_chain(size: usize, runtime: &tokio::runtime::Runtime) -> ScenarioRow {
    let store = runtime.block_on(populate_watch_store(make_mixed_watch_entries(size)));
    let start = ALLOCATOR.begin();
    let snapshot = runtime.block_on(store.snapshot_for_chain(ChainType::Bitcoin));
    let matched = snapshot.len();
    let live = ALLOCATOR.snapshot(start);
    drop(snapshot);
    let dropped = ALLOCATOR.snapshot(start);
    drop(store);

    ScenarioRow {
        scenario: "snapshot_for_chain_bitcoin",
        size,
        retained_mib: mib(live.current_bytes),
        peak_mib: mib(live.peak_bytes),
        total_allocated_mib: mib(live.total_allocated_bytes),
        after_drop_mib: mib(dropped.current_bytes),
        allocations: live.allocations,
        note: format!(
            "matches={matched}, deallocated_mib={:.4}, deallocations={}, reallocations={}",
            mib(live.total_deallocated_bytes),
            live.deallocations,
            live.reallocations
        ),
    }
}

fn measure_indexed_lookup_backfill_seed(size: usize) -> ScenarioRow {
    let watches = make_single_chain_shared_watch_entries(size, ChainType::Bitcoin);
    let start = ALLOCATOR.begin();
    let scenario = benchmark_prepare_indexed_lookup_backfill(&watches);
    let stats = scenario.stats();
    let watch_count = scenario.watch_count();
    let version_count = scenario.current_watch_versions_count();
    let active_count = scenario.active_watch_ids_count();
    let live = ALLOCATOR.snapshot(start);
    drop(scenario);
    let dropped = ALLOCATOR.snapshot(start);

    ScenarioRow {
        scenario: "indexed_lookup_backfill_seed",
        size,
        retained_mib: mib(live.current_bytes),
        peak_mib: mib(live.peak_bytes),
        total_allocated_mib: mib(live.total_allocated_bytes),
        after_drop_mib: mib(dropped.current_bytes),
        allocations: live.allocations,
        note: format!(
            "watch_map={watch_count}, versions={version_count}, active={active_count}, queued={}, inflight={}, ready={}",
            stats.queued, stats.inflight, stats.ready
        ),
    }
}

fn measure_indexed_lookup_backfill_schedule(
    size: usize,
    chain: ChainType,
    concurrency: usize,
) -> ScenarioRow {
    let watches = make_single_chain_shared_watch_entries(size, chain);
    let start = ALLOCATOR.begin();
    let scenario = benchmark_prepare_initial_indexed_lookups(&watches, concurrency);
    let stats = scenario.stats();
    let live = ALLOCATOR.snapshot(start);
    drop(scenario);
    let dropped = ALLOCATOR.snapshot(start);

    ScenarioRow {
        scenario: match chain {
            ChainType::Bitcoin => "indexed_lookup_schedule_bitcoin",
            ChainType::Ethereum => "indexed_lookup_schedule_ethereum",
            ChainType::Base => "indexed_lookup_schedule_base",
        },
        size,
        retained_mib: mib(live.current_bytes),
        peak_mib: mib(live.peak_bytes),
        total_allocated_mib: mib(live.total_allocated_bytes),
        after_drop_mib: mib(dropped.current_bytes),
        allocations: live.allocations,
        note: format!(
            "concurrency={concurrency}, queued={}, inflight={}, ready={}",
            stats.queued, stats.inflight, stats.ready
        ),
    }
}

fn print_rows(rows: &[ScenarioRow]) {
    println!(
        "{:<34} {:>8} {:>14} {:>11} {:>17} {:>15} {:>10}  Note",
        "Scenario",
        "Size",
        "Retained MiB",
        "Peak MiB",
        "Total Alloc MiB",
        "After Drop MiB",
        "Allocs",
    );

    for row in rows {
        println!(
            "{:<34} {:>8} {:>14.2} {:>11.2} {:>17.2} {:>15.2} {:>10}  {}",
            row.scenario,
            row.size,
            row.retained_mib,
            row.peak_mib,
            row.total_allocated_mib,
            row.after_drop_mib,
            row.allocations,
            row.note
        );
    }
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}
