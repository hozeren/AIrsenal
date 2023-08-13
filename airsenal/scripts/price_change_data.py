"""
Classes for fetching and storing the transfers in/out for each player
The aim of this is to attempt to predict the price changes of players
and accordingly make transfers to maximise profit
"""
from sqlalchemy.orm.session import Session

from airsenal.framework.data_fetcher import FPLDataFetcher
from airsenal.framework.schema import session, TransferPriceTracker
from airsenal.framework.utils import NEXT_GAMEWEEK, CURRENT_SEASON

import datetime as dt
import pandas as pd


def fill_global_transfers_from_api(season: str, dbsession=Session):
    """
    Get the global transfers in/out at the current time.
    """
    fetcher = FPLDataFetcher()
    summary: dict = fetcher.get_current_summary_data()
    df: pd.DataFrame = (
        pd.DataFrame(summary["elements"])
        .rename(columns={"id": "player_id", "now_cost": "price"})[
            ["player_id", "price", "transfers_in", "transfers_out"]
        ]
        .assign(
            timestamp=dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            gameweek=NEXT_GAMEWEEK,
            season=season,
        )
    )
    # add to database
    for _, row in df.iterrows():
        print(f"Adding transfer data for player {row['player_id']}")
        dbsession.add(TransferPriceTracker(**row.to_dict()))
    dbsession.commit()
    
if __name__ == "__main__":
    fill_global_transfers_from_api(CURRENT_SEASON, dbsession=session)
