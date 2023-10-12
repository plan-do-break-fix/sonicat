
from typing import List, Tuple

from interfaces.Interface import DatabaseInterface


SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS audiodata (
  id integer PRIMARY KEY,
  file integer NOT NULL,
  catalog text NOT NULL,
  datatype integer NOT NULL,
  datavalue text NOT NULL,
  datasource integer NOT NULL,
  FOREIGN KEY (datatype)
    REFERENCES audiodatatype (id),
  FOREIGN KEY (datasource)
    REFERENCES datasource (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS audiodatatype (
  id integer PRIMARY KEY,
  name text NOT NULL,
  unit text
);
""",
"""
CREATE TABLE IF NOT EXISTS audiofeature(
  id integer PRIMARY KEY,
  file integer NOT NULL,
  catalog text NOT NULL,
  featuretype integer NOT NULL,
  datapath text NOT NULL,
  datasource integer NOT NULL,
  FOREIGN KEY (featuretype)
    REFERENCES audiofeaturetype (id),
  FOREIGN KEY (datasource)
    REFERENCES datasource (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS audiofeaturetype (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS datasource (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
INSERT OR REPLACE INTO audiodatatype
  (id, name, unit)
VALUES
  (1, 'duration', 's'),
  (2, 'tempo', 'bpm'),
  (3, 'key', NULL)
;
""",
"""
INSERT OR REPLACE INTO audiofeaturetype
  (id, name)
VALUES
  (1, 'chroma_dist_hard_threshold'),
  (2, 'beat_frames')
;
""",
"""
INSERT OR REPLACE INTO datasource
  (id, name)
VALUES
  (1, 'file_path'),
  (2, 'product_page'),
  (3, 'librosa')
;
"""
]


class AnalysisInterface(DatabaseInterface):

    def __init__(self, dbpath=""):
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)
        self.db.commit()

    def new_data(self, file_id: str,
                       catalog: str,
                       dtype_id: str,
                       dvalue: str,
                       dsource_id: str,
                       finalize=False
                       ) -> bool:
        self.c.execute("""
        INSERT INTO audiodata
        (file, catalog, datatype, datavalue, datasource)
        VALUES (?,?,?,?,?)
        ;""", (file_id, catalog, dtype_id, dvalue, dsource_id))
        if finalize:
            self.db.commit()
        return True

    def new_feature(self, file_id: str,
                          catalog: str,
                          ftype_id: str,
                          datapath: str,
                          dsource_id: str,
                          finalize=False
                          ) -> bool:
        self.c.execute("""
        INSERT INTO audiofeature
        (file, catalog, featuretype, datapath, datasource)
        VALUES (?,?,?,?,?)
        ;""", (file_id, catalog, ftype_id, datapath, dsource_id))
        if finalize:
            self.db.commit()
        return True

    def dtype_id(self, dtype_name: str) -> str:
        self.c.execute("SELECT id FROM audiodatatype WHERE name = ?",
                       (dtype_name,))
        return self.c.fetchone()[0]
    
    def ftype_id(self, ftype_name: str) -> str:
        self.c.execute("SELECT id FROM audiofeaturetype WHERE name = ?",
                       (ftype_name,))
        return self.c.fetchone()[0]
    
    def dtype(self, dtype_id: str) -> str:
        self.c.execute("SELECT name FROM audiodatatype WHERE id = ?",
                       (dtype_id,))
        return self.c.fetchone()[0]
    
    def ftype(self, ftype_id: str) -> str:
        self.c.execute("SELECT name FROM audiofeaturetype WHERE id = ?",
                       (ftype_id,))
        return self.c.fetchone()[0]
    
    def data_value(self, file_id, catalog, dtype_id) -> Tuple[str]:
        self.c.execute("""
        SELECT id, datavalue FROM audiodata
        WHERE file = ? AND catalog = ? AND datatype = ?
        ;""", (file_id, catalog, dtype_id))
        return self.c.fetchone()
        
    def data_source_id(self, audio_data_id) -> str:
        self.c.execute("SELECT datasource FROM audiodata WHERE id = ?;",
                       (audio_data_id,))
        return self.c.fetchone()[0]

    def feature_data_path(self, file_id, catalog, ftype_id) -> str:
        self.c.execute("""
        SELECT datapath FROM audiofeature
        WHERE file = ? AND catalog = ? AND featuretype = ?
        ;""", (file_id, catalog, ftype_id))
        return self.c.fetchone()[0]
        
    def all_files_having_feature(self, catalog, ftype_id) -> List[str]:
        self.c.execute("""
        SELECT file FROM audiofeature
        WHERE catalog = ? AND featuretype = ?
        ;""", (catalog, ftype_id))
        return [_i[0] for _i in self.c.fetchall()]
    
    def all_files_having_data(self, catalog, dtype_id) -> List[str]:
        self.c.execute("""
        SELECT file FROM audiodata
        WHERE catalog = ? AND datatype = ?
        ;""", (catalog, dtype_id))
        return [_i[0] for _i in self.c.fetchall()]
    
    def all_files_having_data_by_source(self, catalog: str,
                                              dtype_id: str,
                                              source_id: str
                                              ) -> List[str]:
        self.c.execute("""
        SELECT file FROM audiodata
        WHERE catalog = ? AND datatype = ? AND datasource = ?
        ;""", (catalog, dtype_id, source_id))
        return [_i[0] for _i in self.c.fetchall()]

    def all_file_data_by_type(self, catalog, dtype_id) -> List[str]:
        self.c.execute("""
        SELECT file, datavalue FROM audiodata
        WHERE catalog = ? AND datatype = ?
        ;""", (catalog, dtype_id))
        return self.c.fetchall()
    
    def all_file_data_by_type_and_source(self, catalog: str,
                                               dtype_id: str,
                                               source_id: str
                                               ) -> List[str]:
        self.c.execute("""
        SELECT file, datavalue FROM audiodata
        WHERE catalog = ? AND datatype = ? AND datasource = ?
        ;""", (catalog, dtype_id, source_id))
        return self.c.fetchall()
    
    def all_feature_data_paths_by_type(self, catalog, ftype_id) -> List[str]:
        self.c.execute("""
        SELECT file, datapath FROM audiofeature
        WHERE catalog = ? AND featuretype = ?
        ;""", (catalog, ))
        return self.c.fetchall()

    def all_features_by_file_ids(self, catalog, file_ids) -> List[Tuple[str]]:
        self.c.execute("""
        SELECT file, featuretype, datapath, datasource FROM audiofeature
        WHERE catalog = ? AND file IN (?)
        ;""", (catalog, ','.join(file_ids)))
        return self.c.fetchall()
