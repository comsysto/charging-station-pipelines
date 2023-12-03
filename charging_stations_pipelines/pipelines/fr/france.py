"""Pipeline for retrieving data from the French government website."""

import logging
import os
import pathlib

import pandas as pd
import requests as requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from charging_stations_pipelines.pipelines import Pipeline
from charging_stations_pipelines.pipelines.fr.france_mapper import map_address_fra, map_charging_fra, map_station_fra
from charging_stations_pipelines.pipelines.station_table_updater import StationTableUpdater
from charging_stations_pipelines.shared import download_file, reject_if

logger = logging.getLogger(__name__)


class FraPipeline(Pipeline):
    def _retrieve_data(self):
        data_dir = os.path.join(pathlib.Path(__file__).parent.resolve(), "../../..", "data")
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        tmp_data_path = os.path.join(data_dir, self.config["FRGOV"]["filename"])
        if self.online:
            logger.info("Retrieving Online Data")
            self.download_france_gov_file(tmp_data_path)
        self.data = pd.read_csv(os.path.join(data_dir, "france_stations.csv"), delimiter=",", encoding="utf-8",
                                encoding_errors="replace")

    def run(self):
        logger.info("Running FR GOV Pipeline...")
        self._retrieve_data()
        self.data.drop_duplicates(subset=["id_station_itinerance"], inplace=True)
        station_updater = StationTableUpdater(session=self.session, logger=logger)
        for _, row in tqdm(self.data.iterrows(), total=self.data.shape[0]):
            mapped_address = map_address_fra(row)
            mapped_charging = map_charging_fra(row)
            mapped_station = map_station_fra(row)
            mapped_station.address = mapped_address
            mapped_station.charging = mapped_charging
            station_updater.update_station(station=mapped_station, data_source_key='FRGOV')
        station_updater.log_update_station_counts()

    @staticmethod
    def download_france_gov_file(target_file):
        """Download a file from the French government website."""
        base_url = "https://transport.data.gouv.fr/resources/79624"

        r = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.content, "html.parser")

        all_links_on_gov_page = soup.findAll('a')

        link_to_dataset = list(
                filter(lambda a: a["href"].startswith("https://www.data.gouv.fr/fr/datasets"), all_links_on_gov_page))
        reject_if(len(link_to_dataset) != 1, "Could not determine source for french government data")
        download_file(link_to_dataset[0]["href"], target_file)
