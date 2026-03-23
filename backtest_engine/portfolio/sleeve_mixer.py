"""
Sleeve Mixer — combines multiple sleeve portfolios into a final portfolio.

Three mixing modes:
- fixed_mix: weighted sum of sleeve weights with optional normalization
- regime_switch: select sleeve weights based on regime predicates
- score_based_mix: placeholder (interface ready for future implementation)
"""

from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd
import numpy as np

from backtest_engine.strategy_ir.models import (
    PortfolioAggregation,
    SleeveMixMethod,
    RegimeBranch,
)


class SleeveMixer:
    """
    Mixes sleeve-level weight vectors into a single portfolio weight vector.
    """

    def mix(
        self,
        sleeve_weights: Dict[str, pd.Series],
        config: PortfolioAggregation,
        regime_predicates: Optional[Dict[str, bool]] = None,
    ) -> pd.Series:
        """
        Combine sleeve weights according to the aggregation config.

        Parameters
        ----------
        sleeve_weights : dict
            sleeve_id -> pd.Series (ticker -> weight).
        config : PortfolioAggregation
        regime_predicates : dict, optional
            condition_node -> bool. For regime_switch mode.

        Returns
        -------
        pd.Series : Combined ticker -> weight, normalized.
        """
        if not sleeve_weights:
            return pd.Series(dtype=float)

        if config.method == SleeveMixMethod.FIXED_MIX:
            return self._fixed_mix(sleeve_weights, config)
        elif config.method == SleeveMixMethod.REGIME_SWITCH:
            return self._regime_switch(sleeve_weights, config, regime_predicates or {})
        elif config.method == SleeveMixMethod.SCORE_BASED_MIX:
            return self._score_based_mix(sleeve_weights, config)
        else:
            raise ValueError(f"Unknown sleeve mix method: {config.method}")

    def _fixed_mix(
        self,
        sleeve_weights: Dict[str, pd.Series],
        config: PortfolioAggregation,
    ) -> pd.Series:
        """Weighted sum of sleeve portfolios."""
        # Determine mix weights
        if config.sleeve_weights:
            mix_w = {k: v for k, v in config.sleeve_weights.items() if k in sleeve_weights}
        else:
            # Equal weights across active sleeves
            n = len(sleeve_weights)
            mix_w = {k: 1.0 / n for k in sleeve_weights}

        # Normalize mix weights
        total_mix = sum(mix_w.values())
        if total_mix < 1e-10:
            n = len(sleeve_weights)
            mix_w = {k: 1.0 / n for k in sleeve_weights}
            total_mix = 1.0

        if config.normalize:
            mix_w = {k: v / total_mix for k, v in mix_w.items()}

        # Aggregate
        all_tickers = set()
        for sw in sleeve_weights.values():
            all_tickers.update(sw.index)

        combined = pd.Series(0.0, index=sorted(all_tickers))
        for sleeve_id, sw in sleeve_weights.items():
            w = mix_w.get(sleeve_id, 0.0)
            if w > 0:
                combined = combined.add(sw * w, fill_value=0.0)

        return combined[combined > 1e-8]

    def _regime_switch(
        self,
        sleeve_weights: Dict[str, pd.Series],
        config: PortfolioAggregation,
        regime_predicates: Dict[str, bool],
    ) -> pd.Series:
        """
        Select mix weights based on regime predicates.
        First True branch wins.
        """
        for branch in config.regime_branches:
            cond = regime_predicates.get(branch.condition_node, False)
            if cond:
                active_weights = {
                    k: v for k, v in branch.weights.items()
                    if k in sleeve_weights
                }
                return self._fixed_mix(
                    sleeve_weights,
                    PortfolioAggregation(
                        method=SleeveMixMethod.FIXED_MIX,
                        sleeve_weights=active_weights,
                        normalize=config.normalize,
                    )
                )

        # No branch matched — use default weights
        if config.default_weights:
            active_weights = {
                k: v for k, v in config.default_weights.items()
                if k in sleeve_weights
            }
            return self._fixed_mix(
                sleeve_weights,
                PortfolioAggregation(
                    method=SleeveMixMethod.FIXED_MIX,
                    sleeve_weights=active_weights,
                    normalize=config.normalize,
                )
            )

        # Final fallback: equal weight
        return self._fixed_mix(sleeve_weights, config)

    def _score_based_mix(
        self,
        sleeve_weights: Dict[str, pd.Series],
        config: PortfolioAggregation,
    ) -> pd.Series:
        """
        Placeholder for score-based mixing.
        Current behavior: equal weight (full implementation requires runtime scores).
        """
        return self._fixed_mix(sleeve_weights, config)
