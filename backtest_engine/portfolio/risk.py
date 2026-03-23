"""
Covariance estimation models for portfolio optimization.

Supports:
- diagonal_vol: diagonal matrix using only variance (ignores correlations)
- sample_cov: standard sample covariance matrix
- shrinkage_cov: Ledoit-Wolf shrinkage (via scipy)

All return an (n x n) numpy array.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import List, Optional

from backtest_engine.strategy_ir.models import CovarianceModel


def estimate_covariance(
    returns: pd.DataFrame,
    model: CovarianceModel = CovarianceModel.SHRINKAGE_COV,
    annualize: bool = True,
    annualization_factor: int = 252,
) -> np.ndarray:
    """
    Estimate covariance matrix from a (T x N) returns DataFrame.

    Parameters
    ----------
    returns : pd.DataFrame
        Rows = dates, columns = tickers. NaN-forward-filled, then dropped.
    model : CovarianceModel
    annualize : bool
        If True, multiply by annualization_factor.
    annualization_factor : int

    Returns
    -------
    np.ndarray : (N x N) covariance matrix.
    """
    # Clean: drop rows with all NaN, then forward-fill
    r = returns.ffill().dropna(how="all")
    # Drop tickers with > 30% missing
    threshold = int(len(r) * 0.7)
    r = r.dropna(axis=1, thresh=threshold).fillna(0.0)

    n = r.shape[1]
    if n == 0:
        return np.eye(1)

    scale = annualization_factor if annualize else 1.0

    if model == CovarianceModel.DIAGONAL_VOL:
        vol = r.std(ddof=1).values
        cov = np.diag(vol ** 2) * scale

    elif model == CovarianceModel.SAMPLE_COV:
        cov = np.cov(r.T) * scale
        if cov.ndim == 0:
            cov = np.array([[float(cov) * scale]])

    elif model == CovarianceModel.SHRINKAGE_COV:
        cov = _ledoit_wolf_shrinkage(r.values) * scale

    else:
        cov = np.cov(r.T) * scale

    # Ensure positive semi-definite (numerical noise fix)
    cov = _nearestPD(cov)
    return cov


def _ledoit_wolf_shrinkage(X: np.ndarray) -> np.ndarray:
    """
    Ledoit-Wolf analytical shrinkage estimator.
    Shrinks sample covariance toward scaled identity.
    """
    T, N = X.shape
    if T < N + 2:
        # Not enough samples — fall back to diagonal
        return np.diag(X.var(axis=0))

    # Sample covariance (unbiased)
    mu = X.mean(axis=0)
    X_centered = X - mu
    S = X_centered.T @ X_centered / T

    # Ledoit-Wolf shrinkage target: scaled identity
    mu_hat = np.trace(S) / N
    target = mu_hat * np.eye(N)

    # Frobenius norm components
    delta = 0.0
    for t in range(T):
        x = X_centered[t, :, np.newaxis]
        outer = x @ x.T / T
        delta += np.sum((outer - S) ** 2)

    # Shrinkage intensity
    num = delta / T
    denom = np.sum((S - target) ** 2)
    alpha = min(1.0, max(0.0, num / (denom + 1e-12)))

    return (1 - alpha) * S + alpha * target


def _nearestPD(A: np.ndarray) -> np.ndarray:
    """Find the nearest positive definite matrix using eigenvalue clipping."""
    # Symmetrize
    B = (A + A.T) / 2
    eigvals, eigvecs = np.linalg.eigh(B)
    # Clip to min eigenvalue
    eigvals = np.maximum(eigvals, 1e-8)
    return eigvecs @ np.diag(eigvals) @ eigvecs.T


def get_tickers_from_returns(returns: pd.DataFrame) -> List[str]:
    """Return list of tickers present in the returns DataFrame."""
    r = returns.ffill().dropna(how="all")
    threshold = int(len(r) * 0.7)
    r = r.dropna(axis=1, thresh=threshold)
    return list(r.columns)
