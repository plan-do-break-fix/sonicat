
from typing import Dict, List, Tuple

from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS audiodata(
  id integer PRIMARY KEY,
  file integer NOT NULL,
  catalog text NOT NULL,
  datatype integer NOT NULL,
  datavalue text,
  datafilepath text,
  dataforeignkey integer,
  FOREIGN KEY (datatype)
    REFERENCES audiodatatype (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS audiodatatype (
  id integer PRIMARY KEY,
  name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS chromadistribution (
  id integer PRIMARY KEY,
  catalog text NOT NULL,
  file integer NOT NULL,
  c01 real NOT NULL,
  c02 real NOT NULL,
  c03 real NOT NULL,
  c04 real NOT NULL,
  c05 real NOT NULL,
  c06 real NOT NULL,
  c07 real NOT NULL,
  c08 real NOT NULL,
  c09 real NOT NULL,
  c10 real NOT NULL,
  c11 real NOT NULL,
  c12 real NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS log (
    id integer PRIMARY KEY,
    asset integer NOT NULL,
    catalog text NOT NULL
);
""",
"""
INSERT OR REPLACE INTO audiodatatype
  (id, name)
VALUES
  (1, 'duration'),
  (2, 'tempo'),
  (3, 'chroma_distribution'), 
  (4, 'beat_frames')
;
"""
]


class DataInterface(DatabaseInterface):

    def __init__(self, dbpath=""):
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)
        self.db.commit()
        self.dtype_ids = {}

    def new_data(self, file_id: str,
                       catalog: str,
                       dtype_id: str,
                       dvalue="",
                       dpath="",
                       dkeyid="",
                       finalize=False
                       ) -> bool:
        if dvalue and not (dpath or dkeyid):
            self.c.execute("""
            INSERT INTO audiodata (file, catalog, datatype, datavalue)
            VALUES (?,?,?,?);
            """, (file_id, catalog, dtype_id, dvalue))
        elif dpath and not (dvalue or dkeyid):
            self.c.execute("""
            INSERT INTO audiodata (file, catalog, datatype, datafilepath)
            VALUES (?,?,?,?);
            """, (file_id, catalog, dtype_id, dpath))
        elif dkeyid and not (dvalue or dpath):
            self.c.execute("""
            INSERT INTO audiodata (file, catalog, datatype, dataforeignkey)
            VALUES (?,?,?,?);
            """, (file_id, catalog, dtype_id, dkeyid))
        else:
            return True
        if finalize:
            self.db.commit()
        return True

    def new_chroma_distribution(self, catalog, file_id, cdist):
        if not len(cdist) == 12:
            raise ValueError
        self.c.execute("""
        INSERT INTO chromadistribution
          (catalog, file, c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12)
        VALUES
          (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ;""", (catalog, file_id, cdist[0], cdist[1], cdist[2], cdist[3],
                                 cdist[4], cdist[5], cdist[6], cdist[7],
                                 cdist[8], cdist[9], cdist[10], cdist[11]))

    def dtype_id(self, dtype_name: str) -> str:
        if not dtype_name in self.dtype_ids.keys():
            self.c.execute("SELECT id FROM audiodatatype WHERE name = ?",
                           (dtype_name,))
            self.dtype_ids[dtype_name] = self.c.fetchone()[0]
        return self.dtype_ids[dtype_name]
    
    def dtype(self, dtype_id: str) -> str:
        self.c.execute("SELECT name FROM audiodatatype WHERE id = ?",
                       (dtype_id,))
        return self.c.fetchone()[0]
    
    def data_value(self, file_id: str, catalog, dtype_id) -> Tuple[str]:
        self.c.execute("""
        SELECT id, datavalue FROM audiodata
        WHERE file = ? AND catalog = ? AND datatype = ?
        ;""", (file_id, catalog, dtype_id))
        return self.c.fetchone()
    
    def data_values(self, file_ids: List[str], catalog, dtype_id) -> Tuple[str]:
        query = "SELECT file, datavalue FROM audiodata WHERE catalog = ? AND datatype = ?"
        query += f" AND file IN ({file_ids[0]}"
        for _id in file_ids[1:]:
            query += f",{_id}"
        query += ");"
        self.c.execute(query, (catalog, dtype_id))
        return self.c.fetchall()
    
    def data_file_path(self, file_id, catalog, dtype_id) -> Tuple[str]:
        self.c.execute("""
        SELECT id, datafilepath FROM audiodata
        WHERE file = ? AND catalog = ? AND datatype = ?
        ;""", (file_id, catalog, dtype_id))
        return self.c.fetchone()
    
    def data_foreign_key(self, file_id, catalog, dtype_id) -> Tuple[str]:
        self.c.execute("""
        SELECT id, dataforeignkey FROM audiodata
        WHERE file = ? AND catalog = ? AND datatype = ?
        ;""", (file_id, catalog, dtype_id))
        return self.c.fetchone()
    
    def all_files_having_data(self, catalog, dtype_id) -> List[str]:
        self.c.execute("""
        SELECT file FROM audiodata
        WHERE catalog = ? AND datatype = ?
        ORDER BY file ASC
        ;""", (catalog, dtype_id))
        return [_i[0] for _i in self.c.fetchall()]
    
    def log_completed_asset(self, catalog, asset_id):
        self.c.execute("INSERT INTO log (catalog, asset) VALUES (?,?);",
                       (catalog, asset_id))
        self.db.commit()

    def completed_assets(self, catalog) -> List[str]:
        self.c.execute("SELECT asset FROM log WHERE catalog = ?;", (catalog,))
        return [_i[0] for _i in self.c.fetchall()]
    
    def all_chroma_distributions(self, catalog, lbound=0, ubound=0) -> Dict[str, List[float]]:
        query = "SELECT * FROM chromadistribution WHERE catalog = ?"
        arguments = [catalog]
        if lbound > 0:
            query += " AND file > ?"
            arguments.append(lbound)
        if ubound > 0:
            query += " AND file < ?"
            arguments.append(ubound)
        query += " ORDER BY file ASC;"
        self.c.execute(query, arguments)
        return {_i[2]: _i[3:] for _i in self.c.fetchall()}



        
        