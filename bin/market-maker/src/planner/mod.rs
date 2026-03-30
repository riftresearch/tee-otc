use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PlannerAsset {
    AssetA,
    AssetB,
}

impl PlannerAsset {
    pub const ALL: [Self; 2] = [Self::AssetA, Self::AssetB];

    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::AssetA => Self::AssetB,
            Self::AssetB => Self::AssetA,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationState {
    Open,
    Outbound,
    PendingSettlement,
    Done,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Obligation {
    pub id: u64,
    pub payout_asset: PlannerAsset,
    pub payout_amount: u64,
    pub settlement_asset: PlannerAsset,
    pub settlement_amount: u64,
    pub created_at: u64,
    pub state: ObligationState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundBatch {
    pub payout_asset: PlannerAsset,
    pub payout_amount: u64,
    pub settlement_asset: PlannerAsset,
    pub settlement_amount: u64,
    pub obligation_ids: BTreeSet<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSettlement {
    pub asset: PlannerAsset,
    pub amount: u64,
    pub obligation_ids: BTreeSet<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rebalance {
    pub src_asset: PlannerAsset,
    pub dst_asset: PlannerAsset,
    pub amount: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerPolicy {
    pub rebalance_trigger_bps: u16,
    pub rebalance_target_bps: u16,
}

impl PlannerPolicy {
    #[must_use]
    pub fn new(rebalance_trigger_bps: u16, rebalance_target_bps: u16) -> Self {
        assert!(rebalance_trigger_bps <= 10_000, "trigger bps must be <= 10000");
        assert!(rebalance_target_bps <= 10_000, "target bps must be <= 10000");
        assert!(
            rebalance_target_bps <= rebalance_trigger_bps,
            "target bps must be <= trigger bps"
        );

        Self {
            rebalance_trigger_bps,
            rebalance_target_bps,
        }
    }
}

impl Default for PlannerPolicy {
    fn default() -> Self {
        Self::new(7_000, 5_000)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlannerActions {
    pub outbound_batches: Vec<OutboundBatch>,
    pub rebalance: Option<Rebalance>,
}

impl PlannerActions {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outbound_batches.is_empty() && self.rebalance.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerState {
    pub obligations: BTreeMap<u64, Obligation>,
    pub available_inventory: BTreeMap<PlannerAsset, u64>,
    pub outbound_batches: BTreeMap<PlannerAsset, OutboundBatch>,
    pub pending_settlements: BTreeMap<BTreeSet<u64>, PendingSettlement>,
    pub rebalance: Option<Rebalance>,
}

impl PlannerState {
    #[must_use]
    pub fn new(asset_a_inventory: u64, asset_b_inventory: u64) -> Self {
        Self {
            obligations: BTreeMap::new(),
            available_inventory: BTreeMap::from([
                (PlannerAsset::AssetA, asset_a_inventory),
                (PlannerAsset::AssetB, asset_b_inventory),
            ]),
            outbound_batches: BTreeMap::new(),
            pending_settlements: BTreeMap::new(),
            rebalance: None,
        }
    }

    pub fn insert_obligation(&mut self, obligation: Obligation) {
        let previous = self.obligations.insert(obligation.id, obligation);
        assert!(previous.is_none(), "duplicate obligation id");
    }

    #[must_use]
    pub fn available_inventory(&self, asset: PlannerAsset) -> u64 {
        self.available_inventory
            .get(&asset)
            .copied()
            .unwrap_or_default()
    }

    pub fn set_available_inventory(&mut self, asset: PlannerAsset, amount: u64) {
        self.available_inventory.insert(asset, amount);
    }

    #[must_use]
    pub fn next_actions(&self, policy: PlannerPolicy) -> PlannerActions {
        let outbound_batches = PlannerAsset::ALL
            .into_iter()
            .filter_map(|asset| self.fillable_prefix(asset))
            .collect::<Vec<_>>();

        let started_outbound_ids = outbound_batches
            .iter()
            .flat_map(|batch| batch.obligation_ids.iter().copied())
            .collect::<BTreeSet<_>>();

        let mut inventory_after_outbound_batches = self.available_inventory.clone();
        for batch in &outbound_batches {
            let available = inventory_after_outbound_batches
                .get(&batch.payout_asset)
                .copied()
                .unwrap_or_default();
            inventory_after_outbound_batches
                .insert(batch.payout_asset, available.saturating_sub(batch.payout_amount));
        }

        let rebalance =
            self.next_rebalance(policy, &started_outbound_ids, &inventory_after_outbound_batches);

        PlannerActions {
            outbound_batches,
            rebalance,
        }
    }

    pub fn commit_next_actions(&mut self, policy: PlannerPolicy) -> PlannerActions {
        let actions = self.next_actions(policy);

        for batch in &actions.outbound_batches {
            let available_before = self.available_inventory(batch.payout_asset);
            assert!(
                available_before >= batch.payout_amount,
                "outbound batch exceeds available inventory"
            );
            self.set_available_inventory(batch.payout_asset, available_before - batch.payout_amount);

            for obligation_id in &batch.obligation_ids {
                let obligation = self
                    .obligations
                    .get_mut(obligation_id)
                    .expect("outbound batch references missing obligation");
                obligation.state = ObligationState::Outbound;
            }

            let previous = self
                .outbound_batches
                .insert(batch.payout_asset, batch.clone());
            assert!(previous.is_none(), "outbound batch already active for asset");
        }

        if let Some(rebalance) = actions.rebalance {
            let available_before = self.available_inventory(rebalance.src_asset);
            assert!(
                available_before >= rebalance.amount,
                "rebalance exceeds available inventory"
            );
            self.set_available_inventory(rebalance.src_asset, available_before - rebalance.amount);

            let previous = self.rebalance.replace(rebalance);
            assert!(previous.is_none(), "rebalance already active");
        }

        actions
    }

    pub fn mark_outbound_batch_succeeded(
        &mut self,
        asset: PlannerAsset,
    ) -> Option<PendingSettlement> {
        let batch = self.outbound_batches.remove(&asset)?;

        for obligation_id in &batch.obligation_ids {
            let obligation = self
                .obligations
                .get_mut(obligation_id)
                .expect("outbound batch references missing obligation");
            obligation.state = ObligationState::PendingSettlement;
        }

        let settlement = PendingSettlement {
            asset: batch.settlement_asset,
            amount: batch.settlement_amount,
            obligation_ids: batch.obligation_ids.clone(),
        };

        let key = settlement.obligation_ids.clone();
        let previous = self.pending_settlements.insert(key, settlement.clone());
        assert!(previous.is_none(), "pending settlement already exists");

        Some(settlement)
    }

    pub fn mark_outbound_batch_failed(&mut self, asset: PlannerAsset) -> Option<OutboundBatch> {
        let batch = self.outbound_batches.remove(&asset)?;

        self.set_available_inventory(
            batch.payout_asset,
            self.available_inventory(batch.payout_asset)
                .saturating_add(batch.payout_amount),
        );

        for obligation_id in &batch.obligation_ids {
            let obligation = self
                .obligations
                .get_mut(obligation_id)
                .expect("outbound batch references missing obligation");
            obligation.state = ObligationState::Open;
        }

        Some(batch)
    }

    pub fn complete_pending_settlement(
        &mut self,
        obligation_ids: &BTreeSet<u64>,
    ) -> Option<PendingSettlement> {
        let settlement = self.pending_settlements.remove(obligation_ids)?;

        self.set_available_inventory(
            settlement.asset,
            self.available_inventory(settlement.asset)
                .saturating_add(settlement.amount),
        );

        for obligation_id in &settlement.obligation_ids {
            let obligation = self
                .obligations
                .get_mut(obligation_id)
                .expect("pending settlement references missing obligation");
            obligation.state = ObligationState::Done;
        }

        Some(settlement)
    }

    pub fn mark_rebalance_succeeded(&mut self) -> Option<Rebalance> {
        let rebalance = self.rebalance.take()?;
        let dst_inventory = self.available_inventory(rebalance.dst_asset);
        self.set_available_inventory(
            rebalance.dst_asset,
            dst_inventory.saturating_add(rebalance.amount),
        );
        Some(rebalance)
    }

    pub fn mark_rebalance_failed(&mut self) -> Option<Rebalance> {
        let rebalance = self.rebalance.take()?;
        let src_inventory = self.available_inventory(rebalance.src_asset);
        self.set_available_inventory(
            rebalance.src_asset,
            src_inventory.saturating_add(rebalance.amount),
        );
        Some(rebalance)
    }

    pub fn drop_open_obligation(&mut self, obligation_id: u64) -> bool {
        match self.obligations.get(&obligation_id) {
            Some(obligation) if obligation.state == ObligationState::Open => {
                self.obligations.remove(&obligation_id);
                true
            }
            _ => false,
        }
    }

    #[must_use]
    pub fn drain_plan_assuming_success(
        &self,
        policy: PlannerPolicy,
        max_rounds: usize,
    ) -> Vec<PlannerActions> {
        let mut state = self.clone();
        let mut rounds = Vec::new();

        for _ in 0..max_rounds {
            let actions = state.commit_next_actions(policy);
            if actions.is_empty() {
                break;
            }

            let outbound_assets = actions
                .outbound_batches
                .iter()
                .map(|batch| batch.payout_asset)
                .collect::<Vec<_>>();
            let has_rebalance = actions.rebalance.is_some();

            for asset in outbound_assets {
                state.mark_outbound_batch_succeeded(asset);
            }

            let settlement_keys = state
                .pending_settlements
                .keys()
                .cloned()
                .collect::<Vec<BTreeSet<u64>>>();
            for settlement_key in settlement_keys {
                state.complete_pending_settlement(&settlement_key);
            }

            if has_rebalance {
                state.mark_rebalance_succeeded();
            }

            rounds.push(actions);
        }

        rounds
    }

    fn next_rebalance(
        &self,
        policy: PlannerPolicy,
        started_outbound_ids: &BTreeSet<u64>,
        inventory_after_outbound_batches: &BTreeMap<PlannerAsset, u64>,
    ) -> Option<Rebalance> {
        if self.rebalance.is_some() {
            return None;
        }

        let available_a = inventory_after_outbound_batches
            .get(&PlannerAsset::AssetA)
            .copied()
            .unwrap_or_default();
        let available_b = inventory_after_outbound_batches
            .get(&PlannerAsset::AssetB)
            .copied()
            .unwrap_or_default();
        let total_available = available_a.saturating_add(available_b);
        if total_available == 0 {
            return None;
        }

        let majority_asset = if available_a >= available_b {
            PlannerAsset::AssetA
        } else {
            PlannerAsset::AssetB
        };
        let minority_asset = majority_asset.other();
        let majority_available = inventory_after_outbound_batches
            .get(&majority_asset)
            .copied()
            .unwrap_or_default();

        let trigger_reached = (majority_available as u128) * 10_000u128
            >= (policy.rebalance_trigger_bps as u128) * (total_available as u128);
        if !trigger_reached {
            return None;
        }

        let demand_a = self.open_payout_demand_after_outbound_batches(
            PlannerAsset::AssetA,
            started_outbound_ids,
        );
        let demand_b = self.open_payout_demand_after_outbound_batches(
            PlannerAsset::AssetB,
            started_outbound_ids,
        );

        let deficit_a = demand_a.saturating_sub(available_a);
        let deficit_b = demand_b.saturating_sub(available_b);
        let surplus_a = available_a.saturating_sub(demand_a);
        let surplus_b = available_b.saturating_sub(demand_b);

        let minority_blocked = match minority_asset {
            PlannerAsset::AssetA => deficit_a > 0,
            PlannerAsset::AssetB => deficit_b > 0,
        };
        if !minority_blocked {
            return None;
        }

        let (src_asset, dst_asset, source_surplus, target_deficit) =
            if surplus_a > 0 && deficit_b > 0 {
                (PlannerAsset::AssetA, PlannerAsset::AssetB, surplus_a, deficit_b)
            } else if surplus_b > 0 && deficit_a > 0 {
                (PlannerAsset::AssetB, PlannerAsset::AssetA, surplus_b, deficit_a)
            } else {
                return None;
            };

        let target_majority_cap =
            div_ceil_u64((policy.rebalance_target_bps as u128) * (total_available as u128), 10_000);
        let amount_to_target = majority_available.saturating_sub(target_majority_cap);
        if amount_to_target == 0 {
            return None;
        }

        let amount = amount_to_target.min(source_surplus).min(target_deficit);
        if amount == 0 {
            return None;
        }

        Some(Rebalance {
            src_asset,
            dst_asset,
            amount,
        })
    }

    fn fillable_prefix(&self, asset: PlannerAsset) -> Option<OutboundBatch> {
        if self.outbound_batches.contains_key(&asset) {
            return None;
        }

        let mut obligation_ids = BTreeSet::new();
        let mut payout_amount = 0u64;
        let mut settlement_amount = 0u64;
        let available = self.available_inventory(asset);

        for obligation in self.open_lane(asset) {
            let next_payout_amount = payout_amount.saturating_add(obligation.payout_amount);
            if next_payout_amount > available {
                break;
            }

            payout_amount = next_payout_amount;
            settlement_amount =
                settlement_amount.saturating_add(obligation.settlement_amount);
            obligation_ids.insert(obligation.id);
        }

        if obligation_ids.is_empty() {
            None
        } else {
            Some(OutboundBatch {
                payout_asset: asset,
                payout_amount,
                settlement_asset: asset.other(),
                settlement_amount,
                obligation_ids,
            })
        }
    }

    fn open_lane(&self, asset: PlannerAsset) -> Vec<&Obligation> {
        let mut obligations = self
            .obligations
            .values()
            .filter(|obligation| {
                obligation.state == ObligationState::Open && obligation.payout_asset == asset
            })
            .collect::<Vec<_>>();

        obligations.sort_by_key(|obligation| (obligation.created_at, obligation.id));
        obligations
    }

    fn open_payout_demand_after_outbound_batches(
        &self,
        asset: PlannerAsset,
        started_outbound_ids: &BTreeSet<u64>,
    ) -> u64 {
        self.obligations
            .values()
            .filter(|obligation| {
                obligation.state == ObligationState::Open
                    && obligation.payout_asset == asset
                    && !started_outbound_ids.contains(&obligation.id)
            })
            .fold(0u64, |sum, obligation| {
                sum.saturating_add(obligation.payout_amount)
            })
    }
}

fn div_ceil_u64(numerator: u128, denominator: u128) -> u64 {
    ((numerator + denominator - 1) / denominator) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_obligation(
        id: u64,
        payout_asset: PlannerAsset,
        payout_amount: u64,
        settlement_amount: u64,
        created_at: u64,
    ) -> Obligation {
        Obligation {
            id,
            payout_asset,
            payout_amount,
            settlement_asset: payout_asset.other(),
            settlement_amount,
            created_at,
            state: ObligationState::Open,
        }
    }

    #[test]
    fn commits_maximal_oldest_first_outbound_prefix() {
        let mut state = PlannerState::new(5, 0);
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetA, 2, 2, 1));
        state.insert_obligation(open_obligation(2, PlannerAsset::AssetA, 3, 3, 2));
        state.insert_obligation(open_obligation(3, PlannerAsset::AssetA, 1, 1, 3));

        let actions = state.commit_next_actions(PlannerPolicy::default());

        assert_eq!(actions.outbound_batches.len(), 1);
        assert_eq!(actions.outbound_batches[0].payout_asset, PlannerAsset::AssetA);
        assert_eq!(actions.outbound_batches[0].payout_amount, 5);
        assert_eq!(
            actions.outbound_batches[0].obligation_ids,
            BTreeSet::from([1, 2])
        );
        assert_eq!(state.available_inventory(PlannerAsset::AssetA), 0);
        assert_eq!(
            state.obligations.get(&1).map(|obligation| obligation.state),
            Some(ObligationState::Outbound)
        );
        assert_eq!(
            state.obligations.get(&2).map(|obligation| obligation.state),
            Some(ObligationState::Outbound)
        );
        assert_eq!(
            state.obligations.get(&3).map(|obligation| obligation.state),
            Some(ObligationState::Open)
        );
    }

    #[test]
    fn does_not_skip_blocked_oldest_obligation_within_an_asset_lane() {
        let mut state = PlannerState::new(3, 0);
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetA, 4, 4, 1));
        state.insert_obligation(open_obligation(2, PlannerAsset::AssetA, 1, 1, 2));

        let actions = state.next_actions(PlannerPolicy::default());

        assert!(actions.outbound_batches.is_empty());
        assert!(actions.rebalance.is_none());
    }

    #[test]
    fn starts_thresholded_rebalance_only_for_blocked_minority_demand() {
        let mut state = PlannerState::new(7, 3);
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetB, 4, 4, 1));

        let actions = state.next_actions(PlannerPolicy::default());

        assert!(actions.outbound_batches.is_empty());
        assert_eq!(
            actions.rebalance,
            Some(Rebalance {
                src_asset: PlannerAsset::AssetA,
                dst_asset: PlannerAsset::AssetB,
                amount: 1,
            })
        );
    }

    #[test]
    fn does_not_rebalance_inside_trigger_band() {
        let mut state = PlannerState::new(6, 4);
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetB, 5, 5, 1));

        let actions = state.next_actions(PlannerPolicy::default());

        assert!(actions.rebalance.is_none());
    }

    #[test]
    fn open_obligations_can_be_dropped_but_outbound_ones_cannot() {
        let mut state = PlannerState::new(1, 0);
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetA, 1, 1, 1));

        assert!(state.drop_open_obligation(1));
        assert!(!state.obligations.contains_key(&1));

        state.insert_obligation(open_obligation(2, PlannerAsset::AssetA, 1, 1, 2));
        state.commit_next_actions(PlannerPolicy::default());

        assert!(!state.drop_open_obligation(2));
        assert!(state.obligations.contains_key(&2));
        assert_eq!(
            state.obligations.get(&2).map(|obligation| obligation.state),
            Some(ObligationState::Outbound)
        );
    }

    #[test]
    fn outbound_batch_success_creates_pending_settlement_then_credits_inventory() {
        let mut state = PlannerState::new(2, 0);
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetA, 2, 3, 1));
        state.commit_next_actions(PlannerPolicy::default());

        let pending_settlement = state
            .mark_outbound_batch_succeeded(PlannerAsset::AssetA)
            .expect("expected pending settlement");

        assert_eq!(pending_settlement.asset, PlannerAsset::AssetB);
        assert_eq!(pending_settlement.amount, 3);
        assert_eq!(
            state.obligations.get(&1).map(|obligation| obligation.state),
            Some(ObligationState::PendingSettlement)
        );

        let settlement_key = pending_settlement.obligation_ids.clone();
        state.complete_pending_settlement(&settlement_key);

        assert_eq!(state.available_inventory(PlannerAsset::AssetB), 3);
        assert_eq!(
            state.obligations.get(&1).map(|obligation| obligation.state),
            Some(ObligationState::Done)
        );
    }

    #[test]
    fn outbound_batch_can_start_while_a_rebalance_is_already_active() {
        let mut state = PlannerState::new(1, 0);
        state.rebalance = Some(Rebalance {
            src_asset: PlannerAsset::AssetA,
            dst_asset: PlannerAsset::AssetB,
            amount: 2,
        });
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetB, 2, 2, 1));
        state.insert_obligation(open_obligation(2, PlannerAsset::AssetA, 1, 1, 1));

        let actions = state.commit_next_actions(PlannerPolicy::default());

        assert_eq!(actions.outbound_batches.len(), 1);
        assert_eq!(actions.outbound_batches[0].payout_asset, PlannerAsset::AssetA);
        assert_eq!(actions.outbound_batches[0].obligation_ids, BTreeSet::from([2]));
        assert!(actions.rebalance.is_none());
        assert!(state.rebalance.is_some());
    }

    #[test]
    fn drain_plan_assuming_success_uses_pending_settlement_transition() {
        let mut state = PlannerState::new(7, 3);
        state.insert_obligation(open_obligation(1, PlannerAsset::AssetB, 4, 4, 1));

        let rounds = state.drain_plan_assuming_success(PlannerPolicy::default(), 8);

        assert_eq!(rounds.len(), 2);
        assert_eq!(
            rounds[0].rebalance,
            Some(Rebalance {
                src_asset: PlannerAsset::AssetA,
                dst_asset: PlannerAsset::AssetB,
                amount: 1,
            })
        );
        assert_eq!(rounds[1].outbound_batches.len(), 1);
        assert_eq!(rounds[1].outbound_batches[0].payout_asset, PlannerAsset::AssetB);
        assert_eq!(
            rounds[1].outbound_batches[0].obligation_ids,
            BTreeSet::from([1])
        );
    }
}
