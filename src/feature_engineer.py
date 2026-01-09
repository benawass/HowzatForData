"""
Cricket Feature Engineer

Creates ML-ready features from preprocessed cricket data.
Separate from preprocessing to maintain single responsibility.
"""
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CricketFeatureEngineer:
    """Creates ML features from preprocessed cricket data."""

    def __init__(self, df: pd.DataFrame):
        """
        Initialize with preprocessed DataFrame.

        Args:
            df: Preprocessed cricket data from CricketDataPreprocessor.
        """
        self.df = df

    def create_all_features(self) -> pd.DataFrame:
        """
        Run the full feature engineering pipeline.

        Returns:
            DataFrame with all features added.
        """
        df = self.df.copy()

        # Add feature groups here as they're implemented
        # df = self._create_batting_features(df)
        # df = self._create_bowling_features(df)
        # df = self._create_match_context_features(df)
        # df = self._create_player_form_features(df)

        return df

    # -------------------------------------------------------------------------
    # Batting Features
    # -------------------------------------------------------------------------

    def _create_batting_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create batting-related features.

        Examples:
            - Strike rate (runs per 100 balls)
            - Batting average
            - Boundary percentage
            - Rolling form (last N innings average)
        """
        raise NotImplementedError

    # -------------------------------------------------------------------------
    # Bowling Features
    # -------------------------------------------------------------------------

    def _create_bowling_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create bowling-related features.

        Examples:
            - Economy rate (runs per over)
            - Bowling average (runs per wicket)
            - Dot ball percentage
            - Wickets per spell
        """
        raise NotImplementedError

    # -------------------------------------------------------------------------
    # Match Context Features
    # -------------------------------------------------------------------------

    def _create_match_context_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create match situation features.

        Examples:
            - Required run rate
            - Runs remaining to win
            - Wickets in hand
            - Overs remaining
            - Innings number
        """
        raise NotImplementedError

    # -------------------------------------------------------------------------
    # Player Form Features
    # -------------------------------------------------------------------------

    def _create_player_form_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create player historical form features.

        Examples:
            - Last 5/10 innings average
            - Performance vs specific opposition
            - Home/away performance split
            - Performance in match phase (powerplay, middle, death)
        """
        raise NotImplementedError

    # -------------------------------------------------------------------------
    # Aggregation Helpers
    # -------------------------------------------------------------------------

    def _rolling_average(
        self, 
        df: pd.DataFrame, 
        column: str, 
        window: int,
        groupby_cols: list[str]
    ) -> pd.Series:
        """Calculate rolling average over last N observations per group."""
        raise NotImplementedError

    def _exponential_weighted_average(
        self,
        df: pd.DataFrame,
        column: str,
        span: int,
        groupby_cols: list[str]
    ) -> pd.Series:
        """Calculate exponentially weighted average (more weight to recent)."""
        raise NotImplementedError
