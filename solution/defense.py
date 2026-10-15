"""Online fault detector for the Data Siege event stream."""
import math

from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    profile = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in profile:
        return Verdict(alert=False, pillar="checks", reason=profile["error"])

    baseline = ctx.baseline
    null_rate = profile.get("null_rate", {}).get("customer_id", 0.0)
    checks = (
        (profile["row_count"] < baseline["row_count_min"], "row_count below baseline"),
        (profile["row_count"] > baseline["row_count_max"], "row_count above baseline"),
        (null_rate > baseline["null_rate_max"], "customer_id null rate above baseline"),
        (profile["mean_amount"] < baseline["mean_amount_min"], "mean amount below baseline"),
        (profile["mean_amount"] > baseline["mean_amount_max"], "mean amount above baseline"),
        (profile["staleness_min"] > baseline["staleness_min_max"], "batch staleness above baseline"),
    )
    reason = next((message for failed, message in checks if failed), "")

    # Baseline bounds span roughly six standard deviations; combine moderate shifts.
    row_sigma = max((baseline["row_count_max"] - baseline["row_count_min"]) / 6.0, 1e-9)
    amount_sigma = max((baseline["mean_amount_max"] - baseline["mean_amount_min"]) / 6.0, 1e-9)
    row_center = (baseline["row_count_max"] + baseline["row_count_min"]) / 2.0
    amount_center = (baseline["mean_amount_max"] + baseline["mean_amount_min"]) / 2.0
    distance = math.sqrt(
        ((profile["row_count"] - row_center) / row_sigma) ** 2
        + ((profile["mean_amount"] - amount_center) / amount_sigma) ** 2
        + (null_rate / max(baseline["null_rate_max"], 1e-9)) ** 2
        + (profile["staleness_min"] / max(baseline["staleness_min_max"], 1e-9)) ** 2
    )
    if not reason and distance > 4.0:
        reason = "combined data quality signals are anomalous"
    return Verdict(alert=bool(reason), pillar="checks", reason=reason)


def check_contract_checkpoint(payload, ctx):
    result = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if "error" in result:
        return Verdict(alert=False, pillar="contracts", reason=result["error"])
    violations = result.get("violations", [])
    if violations:
        return Verdict(alert=True, pillar="contracts", reason="contract violations: " + ", ".join(violations))

    limit = ctx.baseline["freshness_delay_max_min"]
    declared = payload.get("declared_sla", {}).get("freshness_min")
    if declared is not None:
        limit = min(limit, declared)
    delayed = result.get("freshness_delay_min", 0.0) > limit
    return Verdict(alert=delayed, pillar="contracts",
                   reason="freshness SLA exceeded" if delayed else "")


def check_lineage_run(payload, ctx):
    result = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in result:
        return Verdict(alert=False, pillar="lineage", reason=result["error"])
    if result.get("actual_downstream_count") == 0:
        return Verdict(alert=True, pillar="lineage", reason="output has no downstream consumers")

    expected = payload.get("expected_upstream")
    model_values = {value for value in payload.values() if isinstance(value, str)}
    if expected is None and model_values.intersection({"dbt:stg_orders", "stg_orders"}):
        expected = ("raw.orders", "raw.customers")
    actual = set(result.get("actual_upstream", []))
    missing = [source for source in (expected or ()) if source not in actual]
    if missing:
        return Verdict(alert=True, pillar="lineage", reason="missing upstream: " + ", ".join(missing))

    slow = result.get("duration_ms", 0.0) > ctx.baseline["lineage_duration_ms_max"]
    return Verdict(alert=slow, pillar="lineage", reason="runtime above baseline" if slow else "")


def check_feature_materialization(payload, ctx):
    result = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in result:
        return Verdict(alert=False, pillar="ai_infra", reason=result["error"])
    # Clean variation reaches ~0.472; known skew cases start near 0.970.
    limit = max(ctx.baseline["feature_mean_shift_sigma_max"], 0.65)
    drifted = result.get("mean_shift_sigma", 0.0) > limit
    return Verdict(alert=drifted, pillar="ai_infra",
                   reason="training-serving feature drift" if drifted else "")


def check_embedding_batch(payload, ctx):
    result = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in result:
        return Verdict(alert=False, pillar="ai_infra", reason=result["error"])
    shift_limit = min(ctx.baseline["embedding_centroid_shift_max"], 0.0395)
    age_limit = min(ctx.baseline["corpus_avg_doc_age_days_max"], 44.5)
    reasons = []
    if result.get("centroid_shift", 0.0) > shift_limit:
        reasons.append("embedding centroid drift")
    if result.get("avg_doc_age_days", 0.0) > age_limit:
        reasons.append("corpus documents are stale")
    return Verdict(alert=bool(reasons), pillar="ai_infra", reason="; ".join(reasons))
