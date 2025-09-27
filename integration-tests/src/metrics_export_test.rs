use market_maker::install_metrics_recorder;

#[tokio::test]
async fn metrics_exporter_renders_payload() {
    let handle = install_metrics_recorder().expect("metrics recorder installs");

    metrics::gauge!("mm_inventory_ratio_bps").set(42.0);
    handle.run_upkeep();

    let body = handle.render();
    assert!(
        body.contains("mm_metrics_exporter_up"),
        "expected bootstrap gauge in payload: {body:?}"
    );
    assert!(
        body.contains("mm_inventory_ratio_bps"),
        "expected inventory ratio metric in payload: {body:?}"
    );
}
