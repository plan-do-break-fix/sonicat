

from interfaces.Interface import DatabaseInterface

from typing import List

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS data (
    id integer PRIMARY KEY,
    file integer NOT NULL,
    catalog text NOT NULL,
    bpm integer,
    key text
);
""",
"""
CREATE TABLE IF NOT EXISTS log (
    id integer PRIMARY KEY,
    asset integer NOT NULL,
    catalog text NOT NULL
);
"""
]


class Interface(DatabaseInterface):

    def __init__(self, dbpath):
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)

    def new_path_parse(self, file_id: str,
                             catalog: str,
                             bpm: str,
                             key: str,
                             finalize=False
                             ) -> bool:
        self.c.execute("INSERT INTO data (file, catalog) VALUES (?,?);",
                       (file_id, catalog))
        if bpm or key:
            self.c.execute("SELECT last_insert_rowid();")
            parse_id = self.c.fetchone()[0]
            if bpm:
                self.c.execute("UPDATE data SET bpm = ? WHERE id = ?;",
                               (bpm, parse_id))
            if key:
                self.c.execute("UPDATE data SET key = ? WHERE id = ?;",
                               (key, parse_id))
        if finalize:
            self.db.commit()
        return True

    def log_asset_parse(self, asset_id: str,
                              catalog: str,
                              finalize=False
                              ) -> bool:
        self.c.execute("INSERT INTO log (asset, catalog) VALUES (?,?);",
                       (asset_id, catalog))
        if finalize:
            self.db.commit()
        return True

    def parsed_asset_ids(self, catalog: str) -> List[str]:
        self.c.execute("SELECT DISTINCT asset FROM log WHERE catalog = ?;",
                       (catalog,))
        return [_i[0] for _i in self.c.fetchall()]