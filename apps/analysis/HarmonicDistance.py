
from decimal import Decimal
from time import time
from typing import List, Tuple

from interfaces.database.LibrosaData import DataInterface
from apps.ConfiguredApp import App
from interfaces.Interface import DatabaseInterface
from util import Logs

### NOTE ###
# To prevent duplication of commutative operations,
# given a catalog and the 2-tuple:
#   (file_id1, file_id2),
# file_id1 < file_id2
# and given the 4-tuple:
#   (catalog1, file_id1, catalog2, file_id2),
# catalog1 <= catalog2 and file_id1 <= file_id2
# if catalog1 = catalog2, file_id1 < file_id2
# if file_id1 = file_id2, catalog1 < catalog2


INTERCATALOG_HARMONIC_DISTANCE_SCHEMA = \
"""
CREATE TABLE IF NOT EXISTS data (
  id integer PRIMARY KEY,
  catalog1 text NOT NULL,
  file1 integer NOT NULL,
  catalog2 text NOT NULL,
  file2 integer NOT NULL,
  distance float NOT NULL
);
"""
INTRACATALOG_HARMONIC_DISTANCE_SCHEMA = \
"""
CREATE TABLE IF NOT EXISTS data (
  id integer PRIMARY KEY,
  file1 integer NOT NULL,
  file2 integer NOT NULL,
  distance float NOT NULL
);
"""


class HarmonicDistanceData(DatabaseInterface):

    def __init__(self, dbpath=""):
        super().__init__(dbpath)

    def all_data(self):
        self.c.execute("SELECT * FROM data;")
        return self.c.fetchall()

    def make_dbpath(self, sonicat_path, dbname):
        pass

    def smallest_distances(self, n: int) -> List[Tuple[str]]:
        self.c.execute("SELECT * FROM data ORDER BY distance ASC LIMIT ?;", (n,))
        return self.c.fetchall()

    def largest_distances(self, n: int) -> List[Tuple[str]]:
        self.c.execute("SELECT * FROM data ORDER BY distance DESC LIMIT ?;", (n,))
        return self.c.fetchall()

    
class Intracatalog(HarmonicDistanceData):

    def __init__(self, sonicat_path, catalog):
        dbpath = self.make_dbpath(sonicat_path, self.make_dbname(catalog))
        super().__init__(dbpath)
        for statement in INTRACATALOG_HARMONIC_DISTANCE_SCHEMA:
            self.c.execute(statement)

    def make_dbname(self, catalog: str) -> str:
        return f"HarmonicDistance-{catalog}_Intracatalog.sqlite"

    def add_distance(self, file_id1, file_id2, distance) -> bool:
        self.c.execute("INSERT INTO data (file1, file2, distance) VALUES (?,?,?);",
                       (file_id1, file_id2, distance))
        self.db.commit()
        return True
    
    def distance(self, file_id1, file_id2) -> Decimal:
        self.c.execute("SELECT distance FROM data AND file1 = ? AND file2 = ?;",
                       (file_id1, file_id2))
        result = self.c.fetchone()[0]
        return Decimal(result)
    
    def all_data_by_file(self, file_id) -> List[Tuple[str]]:
        self.c.execute("SELECT * FROM data WHERE file1 = ? OR file2 = ?"\
                       "  ORDER BY distance ASC;",
                       (file_id,))
        return self.c.fetchall()

    def last_intracatalog_pair(self, catalog) -> Tuple(int):
        self.c.execute("SELECT file1, file2 FROM data ORDER BY id DESC LIMIT 1;")
        result = self.c.fetchone()
        return (int(result[0]), int(result[1])) if result else (0,0)


class Intercatalog(HarmonicDistanceData):

    def __init__(self, sonicat_path, catalog1, catalog2):
        dbpath = self.make_dbpath(sonicat_path, self.make_dbname(catalog1, catalog2))
        super().__init__(dbpath)
        for statement in INTERCATALOG_HARMONIC_DISTANCE_SCHEMA:
            self.c.execute(statement)

    def make_dbname(self, catalog1: str, catalog2) -> str:
        return f"HarmonicDistance-{catalog1}_{catalog2}_Intercatalog.sqlite"
    
    def add_distance(self, catalog1, file_id1, catalog2, file_id2, distance) -> bool:
        self.c.execute("INSERT INTO data (catalog1, file1, catalog2, file2, distance) VALUES (?,?,?,?,?);",
                       (catalog1, file_id1, catalog2, file_id2, distance))
        self.db.commit()
        return True
    
    def distance(self, catalog1, file_id1, catalog2, file_id2) -> Decimal:
        self.c.execute("SELECT distance FROM data"\
                       " WHERE catalog1 = ? AND file1 = ?"\
                       " AND catalog2 = ? AND file2 = ?",
                       (catalog1, file_id1, catalog2, file_id2))
        result = self.c.fetchone()[0]
        return Decimal(result)
    
    def all_data_by_file(self, catalog, file_id) -> List[Tuple[str]]:
        self.c.execute("SELECT * FROM data"\
                       "  WHERE (catalog1 = ? AND file1 = ?)"\
                       "  OR (catalog2 = ? AND file2 = ?)"\
                       "ORDER BY distance ASC;",
                       (catalog, file_id))
        return self.c.fetchall()


class HarmonicDistance(App):

    # Harmonic Distance is restart-safe, however historical revisionism in the
    # catalog will break things.

    def __init__(self, sonicat_path: str) -> None:
        super().__init__(sonicat_path, "")
        librosa_analysis_db_path = f"{self.cfg.data}/analysis/LibrosaAnalysis-ReadReplica.sqlite"
        self.rosa_data = DataInterface(librosa_analysis_db_path)
        self.cfg.name = "HarmonicDistance"
        self.cfg.log += "/analysis"
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"Application Initialization Successful")
        self.sonicat_path = sonicat_path

    def harmonic_distance(self, chroma_dist1, chroma_dist2) -> Decimal:
        return sum([(chroma_dist1[_i] - chroma_dist2[_i])**2 
                    for _i in range(0, len(chroma_dist1))]
                   )

    def log_failed_distance_calc(self, catalog1, file1, cd1, catalog2, file2, cd2) -> bool:
        self.log.error(f"Harmonic Distance Calculation Failure: {catalog1} ID {file1}, {catalog2} ID {file2}")
        self.log.error(f"{catalog1} ID {file1} chromatic distribution:")
        self.log.error(cd1)
        self.log.error(f"{catalog2} ID {file2} chromatic distribution:")
        self.log.error(cd2)
        return True
    
    def intracatalog_linear_run(self, catalog):
        timestamp = time()
        self.log.info(f"Initializing data for linear intracatalog run: {catalog}")
        results = Intracatalog(self.sonicat_path, catalog)
        cdists = self.rosa_data.all_chroma_distributions(catalog)
        all_ids = cdists.keys()
        all_ids.sort()
        last_completed = self.data.load_completed_intracatalog_pair(catalog)
        if last_completed[0] == last_completed[1] == 0:
            self.log.info("No previously calculated pairs found. Beginning new run.")
        else:
            self.log.info(f"Continuing interrupted run from pair {last_completed}.")
        for _id in all_ids:
            if _id < last_completed[1]:
                continue
            if time() - timestamp >= 600:
                results.export_replica()
            self.log.info(f"Calculating linear run with file ID {_id} as file1")
            for _id2 in all_ids:
                if any([_id2 <= _id, _id2 <= last_completed[0]]):
                    continue
                cd1, cd2 = cdists[_id], cdists[_id2]
                distance = self.harmonic_distance(cd1, cd2)
                if not distance and distance != 0.0:
                    self.log_failed_distance_calc(catalog, _id, cd1, catalog, _id2, cd2) 
                    continue
                results.add_distance(catalog, _id, catalog, _id2, distance)
                self.log.debug(f"Harmonic distance recorded for intracatalog pair {_id}, {_id2}")
        self.log.info(f"All distances calculated. Run terminated successfully.")

    def intercatalog_all_pairs_run(self, catalog_asset_pairs: List[Tuple[str]]
                                         ) -> bool:
        cdists = {p: {} for p in set([p[0] for p in catalog_asset_pairs])}
        for p in catalog_asset_pairs:
            cdists[p[0]][p[1]] = self.rosa_data.chroma_distribution(p[0], p[1])
        for p1 in catalog_asset_pairs:
            for p2 in catalog_asset_pairs:
                if p1 >= p2:
                    continue
                cd1 = cdists[p1[0]][p1[1]]
                cd2 = cdists[p2[0]][p2[1]]
                distance = self.harmonic_distance(cd1, cd2)
        pass

    def intercatalog_one_to_many_run(self, catalog_asset_pair: Tuple[str],
                                           catalog_asset_pairs: List[Tuple[str]]
                                           ) -> bool:
        pass


    ### This might be needed later, but not for linear intracatalog processing 
    #def reorder_id_pairs(self, catalog1, id1, catalog2, id2):
    #        if catalog1 == catalog2:
    #            ids = [id1, id2]
    #            ids.sort()
    #            if not ids[0] == id1:
    #                return (catalog1, id2, catalog2, id1)
    #        catalogs = [catalog1, catalog2]
    #        catalogs.sort()
    #        if not catalogs[0] == catalog1:
    #            return (catalog2, id2, catalog1, id1)
    #        return (catalog1, id1, catalog2, id2)
    
    #def completed_distribution_complements(self, catalog1, file1, catalog2=""):
    #    catalog2 = catalog1 if not catalog2 else catalog2
    #    self.c.execute("SELECT file2 FROM data"\
    #                   "  WHERE catalog1 = ? AND file1 = ?"\
    #                   "  AND catalog2 = ?"
    #                   "  ORDER BY file2 ASC;",
    #                   (catalog1, file1, catalog2))
    #    result = self.c.fetchall()
    #    ids = [_i[0] for _i in result] if result else []
    #    self.c.execute("SELECT file1 FROM data"\
    #                   "  WHERE catalog1 = ? AND file2 = ?"\
    #                   "  AND catalog2 = ?"
    #                   "  ORDER BY file1 ASC;",
    #                   (catalog2, file1, catalog1))
    #    result = self.c.fetchall()
    #    ids += [_i[0] for _i in result] if result else []
    #    ids.sort()
    #    return ids

    #def all_data_by_catalog(self, catalog1, catalog2="", lbound=0, ubound=0):
    #    catalog2 = catalog1 if not catalog2 else catalog2
    #    query = "SELECT * FROM data WHERE catalog1 = ? AND catalog2 = ?"
    #    arguments = [catalog1, catalog2]
    #    if lbound > 0:
    #        query += " AND file1 > ? AND file2 > ?"
    #        arguments += [lbound, lbound]
    #    if ubound > 0:
    #        query += " AND file1 < ? AND file2 < ?"
    #        arguments += [ubound, ubound]
    #    query += ";"
    #    self.c.execute(query, arguments)
    #    return self.c.fetchall()
