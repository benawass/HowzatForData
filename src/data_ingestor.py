"""
Cricket Data Ingestor

Parses ball-by-ball cricket JSON files into a flat DataFrame format,
extracting all available match metadata and delivery-level details.
"""
import json
import logging
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class CricketDataIngestor:
    """Ingests cricket JSON files and converts them to a structured DataFrame."""

    def __init__(self, raw_data_json_directory: str):
        """
        Initialize the ingestor with a directory of JSON files.

        Args:
            raw_data_json_directory: Path to directory containing cricket JSON files.
        """
        self.raw_data_path = Path(raw_data_json_directory)
        self.files = list(self.raw_data_path.glob("*.json"))
        logger.info(f"Found {len(self.files)} JSON files in {self.raw_data_path}")

        if not self.files:
            raise FileNotFoundError(f"No JSON files found in {self.raw_data_path}")

    def ingest_all(self) -> pd.DataFrame:
        """
        Parse all JSON files and return a combined DataFrame.

        Returns:
            DataFrame with all ball-by-ball data from all files.
        """
        all_dfs = []
        success_count = 0
        error_count = 0

        for file_path in self.files:
            try:
                df = self._parse_single_file(file_path)
                all_dfs.append(df)
                success_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to parse {file_path.name}: {e}")

        logger.info(f"Successfully parsed {success_count}/{len(self.files)} files")
        if error_count > 0:
            warnings.warn(f"Failed to parse {error_count} files", UserWarning)


        # Filter out empty DataFrames and drop all-NA columns to avoid FutureWarning
        non_empty_dfs = [
            df.dropna(axis=1, how="all") for df in all_dfs if not df.empty
        ]
        if not non_empty_dfs:
            logger.warning("All parsed files resulted in empty DataFrames")
            return pd.DataFrame()

        return pd.concat(non_empty_dfs, ignore_index=True)

    def _parse_single_file(self, file_path: Path) -> pd.DataFrame:
        """
        Parse a single JSON file into a DataFrame.

        Args:
            file_path: Path to the JSON file.

        Returns:
            DataFrame with ball-by-ball data from this file.
        """
        with open(file_path, "r") as f:
            data = json.load(f)

        match_id = file_path.stem
        match_info = self._extract_match_info(data, match_id)
        all_balls = self._extract_deliveries(data, match_info, match_id)

        if not all_balls:
            logger.warning(f"No deliveries found in {file_path.name}")
            return pd.DataFrame()

        return pd.DataFrame(all_balls)

    def _extract_match_info(self, data: dict, match_id: str) -> dict:
        """
        Extract match-level metadata from the JSON data.

        Args:
            data: Parsed JSON data.
            match_id: Match identifier (filename stem).

        Returns:
            Dictionary of match-level metadata.
        """
        info = data.get("info", {})
        meta = data.get("meta", {})

        # Extract teams safely
        teams = info.get("teams", [])
        team_a = teams[0] if len(teams) > 0 else None
        team_b = teams[1] if len(teams) > 1 else None

        if not teams:
            logger.warning(f"Match {match_id}: No teams found")

        # Extract event info
        event = info.get("event", {})

        # Extract toss info
        toss = info.get("toss", {})

        # Extract outcome info
        outcome = info.get("outcome", {})
        outcome_by = outcome.get("by", {})

        # Extract officials
        officials = info.get("officials", {})
        umpires = officials.get("umpires", [])
        tv_umpires = officials.get("tv_umpires", [])
        match_referees = officials.get("match_referees", [])

        # Extract dates (join as comma-separated string for single column)
        dates = info.get("dates", [])

        # Extract player of match
        player_of_match = info.get("player_of_match", [])

        match_info = {
            # Core identifiers
            "match_id": match_id,
            "data_version": meta.get("data_version"),
            # Match details
            "season": str(info.get("season")) if info.get("season") is not None else None,
            "dates": ",".join(dates) if dates else None,
            "start_date": dates[0] if dates else None,
            "city": info.get("city"),
            "venue": info.get("venue"),
            "balls_per_over": info.get("balls_per_over"),
            # Event/Series info
            "event_name": event.get("name"),
            "event_match_number": event.get("match_number"),
            # Match classification
            "gender": info.get("gender"),
            "match_type": info.get("match_type"),
            "match_type_number": info.get("match_type_number"),
            "team_type": info.get("team_type"),
            # Teams
            "team_a": team_a,
            "team_b": team_b,
            # Toss
            "toss_winner": toss.get("winner"),
            "toss_decision": toss.get("decision"),
            # Outcome
            "outcome_winner": outcome.get("winner"),
            "outcome_by_runs": outcome_by.get("runs"),
            "outcome_by_wickets": outcome_by.get("wickets"),
            "outcome_by_innings": outcome_by.get("innings"),
            "outcome_method": outcome.get("method"),  # e.g., "Awarded", "D/L"
            "outcome_result": outcome.get("result"),  # e.g., "draw", "tie", "no result"
            "player_of_match": ",".join(player_of_match) if player_of_match else None,
            # Officials
            "umpire_1": umpires[0] if len(umpires) > 0 else None,
            "umpire_2": umpires[1] if len(umpires) > 1 else None,
            "tv_umpire": tv_umpires[0] if tv_umpires else None,
            "match_referee": match_referees[0] if match_referees else None,
        }

        # Validate critical fields
        if not info.get("venue"):
            logger.warning(f"Match {match_id}: Missing venue")
        if not info.get("match_type"):
            logger.warning(f"Match {match_id}: Missing match_type")

        return match_info

    def _extract_deliveries(
        self, data: dict, match_info: dict, match_id: str
    ) -> list[dict]:
        """
        Extract ball-by-ball delivery data from the JSON.

        Args:
            data: Parsed JSON data.
            match_info: Match-level metadata to attach to each ball.
            match_id: Match identifier for logging.

        Returns:
            List of dictionaries, one per delivery.
        """
        all_balls = []
        innings_list = data.get("innings", [])

        if not innings_list:
            logger.warning(f"Match {match_id}: No innings data found")
            return all_balls

        for innings_idx, innings in enumerate(innings_list):
            batting_team = innings.get("team")

            if not batting_team:
                logger.warning(
                    f"Match {match_id}, Innings {innings_idx + 1}: Missing batting team"
                )

            # Determine bowling team
            if batting_team == match_info.get("team_a"):
                bowling_team = match_info.get("team_b")
            elif batting_team == match_info.get("team_b"):
                bowling_team = match_info.get("team_a")
            else:
                bowling_team = None

            overs = innings.get("overs", [])

            for over_data in overs:
                over_num = over_data.get("over")
                deliveries = over_data.get("deliveries", [])

                for ball_idx, delivery in enumerate(deliveries):
                    ball_record = self._parse_delivery(
                        delivery=delivery,
                        match_info=match_info,
                        innings_num=innings_idx + 1,
                        batting_team=batting_team,
                        bowling_team=bowling_team,
                        over_num=over_num,
                        ball_idx=ball_idx,
                        match_id=match_id,
                    )
                    all_balls.append(ball_record)

        return all_balls

    def _parse_delivery(
        self,
        delivery: dict,
        match_info: dict,
        innings_num: int,
        batting_team: str | None,
        bowling_team: str | None,
        over_num: int | None,
        ball_idx: int,
        match_id: str,
    ) -> dict:
        """
        Parse a single delivery into a flat dictionary.

        Args:
            delivery: The delivery data from JSON.
            match_info: Match-level metadata.
            innings_num: Current innings number (1-indexed).
            batting_team: Team currently batting.
            bowling_team: Team currently bowling.
            over_num: Current over number (0-indexed in source).
            ball_idx: Ball index within the over (0-indexed).
            match_id: Match identifier for logging.

        Returns:
            Dictionary containing all delivery data.
        """
        runs = delivery.get("runs", {})
        extras = delivery.get("extras", {})

        # Build the ball record with all match metadata
        ball_record = {
            **match_info,
            # Innings context
            "innings_num": innings_num,
            "batting_team": batting_team,
            "bowling_team": bowling_team,
            # Ball position
            "over": over_num,
            "ball": ball_idx + 1,
            # Players
            "batter": delivery.get("batter"),
            "bowler": delivery.get("bowler"),
            "non_striker": delivery.get("non_striker"),
            # Runs
            "runs_batter": runs.get("batter"),
            "runs_extras": runs.get("extras"),
            "runs_total": runs.get("total"),
            # Extras breakdown
            "extras_wides": extras.get("wides"),
            "extras_noballs": extras.get("noballs"),
            "extras_byes": extras.get("byes"),
            "extras_legbyes": extras.get("legbyes"),
            "extras_penalty": extras.get("penalty"),
        }

        # Handle wickets
        wickets = delivery.get("wickets", [])
        if wickets:
            ball_record["is_wicket"] = 1
            primary_wicket = wickets[0]
            ball_record["player_out"] = primary_wicket.get("player_out")
            ball_record["dismissal_kind"] = primary_wicket.get("kind")

            # Extract fielders involved
            fielders_list = primary_wicket.get("fielders", [])
            fielder_names = self._extract_fielder_names(fielders_list)
            ball_record["fielder_1"] = fielder_names[0] if len(fielder_names) > 0 else None
            ball_record["fielder_2"] = fielder_names[1] if len(fielder_names) > 1 else None

            # Handle multiple wickets (rare, e.g., hit wicket + run out)
            if len(wickets) > 1:
                logger.debug(
                    f"Match {match_id}, Over {over_num}.{ball_idx + 1}: "
                    f"Multiple wickets ({len(wickets)}) on single delivery"
                )
        else:
            ball_record["is_wicket"] = 0
            ball_record["player_out"] = None
            ball_record["dismissal_kind"] = None
            ball_record["fielder_1"] = None
            ball_record["fielder_2"] = None

        # Handle DRS reviews
        review = delivery.get("review", {})
        if review:
            ball_record["review_by"] = review.get("by")
            ball_record["review_umpire"] = review.get("umpire")
            ball_record["review_batter"] = review.get("batter")
            ball_record["review_decision"] = review.get("decision")
            ball_record["review_type"] = review.get("type")
        else:
            ball_record["review_by"] = None
            ball_record["review_umpire"] = None
            ball_record["review_batter"] = None
            ball_record["review_decision"] = None
            ball_record["review_type"] = None

        return ball_record

    def _extract_fielder_names(self, fielders_list: list) -> list[str]:
        """
        Extract fielder names from the fielders list.

        Fielders can be stored as either strings or dicts with 'name' key.

        Args:
            fielders_list: List of fielders from the JSON.

        Returns:
            List of fielder name strings.
        """
        names = []
        for fielder in fielders_list:
            if isinstance(fielder, str):
                names.append(fielder)
            elif isinstance(fielder, dict):
                name = fielder.get("name")
                if name:
                    names.append(name)
        return names