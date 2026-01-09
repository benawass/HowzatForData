"""
Cricket Data Preprocessor

Preprocesses cricket data from ingested form to a cleaned database with validity checks. 
"""
import logging
import warnings
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class CricketDataPreprocessor:
    """Preprocesses cricket data."""

    def __init__(self, df: pd.DataFrame, modern_data_filter_year: int = 2015):
        self.df = df
        self.modern_data_filter_year = modern_data_filter_year

    def clean_data(self) -> pd.DataFrame:
        """Standardize names, drop incomplete matches, fix dates."""
        df = self.df.copy()

        # 1. Modern Data Filter (e.g., year >= 2015)
        df = self._modern_data_filter(df)

        # 2. Validity Check: Log any matches with outcome discrepancies
        discrepancies = self.validate_match_outcomes()
        if not discrepancies.empty:
            logger.warning(
                f"Found {len(discrepancies)} matches with outcome discrepancies"
            )

        return df

    def _modern_data_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out data older than the modern_data_filter_year."""
        df["start_date"] = pd.to_datetime(df["start_date"])
        return df[df["start_date"].dt.year >= self.modern_data_filter_year]

    def validate_match_outcomes(self) -> pd.DataFrame:
        """
        Validate that the recorded outcome_winner matches calculated winner.

        **Test Cricket Winner Logic:**
        1. Each team bats twice (up to 4 innings total)
        2. Sum total runs for each team across all their innings
        3. The team with MORE TOTAL RUNS wins

        **Outcome types:**
        - Won by runs: Batting-first team scored more total runs
        - Won by wickets: Chasing team reached target with wickets in hand
        - Won by innings: Team won by an innings + runs (only batted once)
        - Draw: Match ended without a result (time ran out)

        Returns:
            DataFrame of matches where calculated winner != recorded winner
        """
        # Calculate totals per team per match
        match_summary = self._calculate_match_totals()

        # Compare calculated winner to recorded winner
        discrepancies = self._find_outcome_discrepancies(match_summary)

        if discrepancies.empty:
            logger.info("All match outcomes validated successfully")
        else:
            logger.warning(
                f"Found {len(discrepancies)} matches with outcome discrepancies:"
            )
            for _, row in discrepancies.iterrows():
                logger.warning(
                    f"  Match {row['match_id']}: "
                    f"Recorded={row['outcome_winner']}, "
                    f"Calculated={row['calculated_winner']}, "
                    f"Team A ({row['team_a']})={row['team_a_runs']}, "
                    f"Team B ({row['team_b']})={row['team_b_runs']}"
                )

        return discrepancies

    def _calculate_match_totals(self) -> pd.DataFrame:
        """
        Calculate total runs and wickets for each team in each match.

        Returns:
            DataFrame with one row per match containing:
            - team_a, team_b: Team names
            - team_a_runs, team_b_runs: Total runs scored by each team
            - team_a_wickets, team_b_wickets: Total wickets lost by each team
            - outcome_winner: Recorded winner from the data
        """
        df = self.df.copy()

        # Get runs scored by each batting team per match
        runs_by_batting_team = (
            df.groupby(["match_id", "batting_team"])
            .agg(
                total_runs=("runs_total", "sum"),
                wickets_lost=("is_wicket", "sum"),
            )
            .reset_index()
        )

        # Get match metadata (team_a, team_b, outcome_winner)
        match_meta = (
            df.groupby("match_id")
            .agg(
                team_a=("team_a", "first"),
                team_b=("team_b", "first"),
                outcome_winner=("outcome_winner", "first"),
                outcome_result=("outcome_result", "first"),
            )
            .reset_index()
        )

        # Pivot to get team_a and team_b runs/wickets as columns
        match_summary = match_meta.copy()

        for _, row in match_summary.iterrows():
            match_id = row["match_id"]
            team_a = row["team_a"]
            team_b = row["team_b"]

            # Get team A's runs
            team_a_data = runs_by_batting_team[
                (runs_by_batting_team["match_id"] == match_id)
                & (runs_by_batting_team["batting_team"] == team_a)
            ]
            match_summary.loc[
                match_summary["match_id"] == match_id, "team_a_runs"
            ] = team_a_data["total_runs"].sum() if not team_a_data.empty else 0

            match_summary.loc[
                match_summary["match_id"] == match_id, "team_a_wickets"
            ] = team_a_data["wickets_lost"].sum() if not team_a_data.empty else 0

            # Get team B's runs
            team_b_data = runs_by_batting_team[
                (runs_by_batting_team["match_id"] == match_id)
                & (runs_by_batting_team["batting_team"] == team_b)
            ]
            match_summary.loc[
                match_summary["match_id"] == match_id, "team_b_runs"
            ] = team_b_data["total_runs"].sum() if not team_b_data.empty else 0

            match_summary.loc[
                match_summary["match_id"] == match_id, "team_b_wickets"
            ] = team_b_data["wickets_lost"].sum() if not team_b_data.empty else 0

        return match_summary

    def _find_outcome_discrepancies(self, match_summary: pd.DataFrame) -> pd.DataFrame:
        """
        Compare calculated winner (by runs) to recorded outcome_winner.

        Args:
            match_summary: DataFrame from _calculate_match_totals()

        Returns:
            DataFrame of matches where calculated != recorded winner
        """
        discrepancies = []

        for _, row in match_summary.iterrows():
            # Skip draws/no results - no winner to validate
            if pd.isna(row["outcome_winner"]) or row["outcome_result"] in [
                "draw",
                "tie",
                "no result",
            ]:
                continue

            team_a = row["team_a"]
            team_b = row["team_b"]
            team_a_runs = row["team_a_runs"]
            team_b_runs = row["team_b_runs"]
            recorded_winner = row["outcome_winner"]

            # Calculate winner: team with more runs wins
            if team_a_runs > team_b_runs:
                calculated_winner = team_a
            elif team_b_runs > team_a_runs:
                calculated_winner = team_b
            else:
                # Tie scenario (very rare)
                calculated_winner = None

            # Check for discrepancy
            if calculated_winner != recorded_winner:
                discrepancies.append(
                    {
                        "match_id": row["match_id"],
                        "team_a": team_a,
                        "team_b": team_b,
                        "team_a_runs": team_a_runs,
                        "team_b_runs": team_b_runs,
                        "calculated_winner": calculated_winner,
                        "outcome_winner": recorded_winner,
                        "outcome_result": row["outcome_result"],
                    }
                )

        return pd.DataFrame(discrepancies)