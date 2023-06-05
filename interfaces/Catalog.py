import shutil
import sqlite3
from typing import Dict, List

from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS asset (
    id integer PRIMARY KEY,
    name text NOT NULL,
    label integer NOT NULL,
    digest text,
    year int,
    cover text,
    FOREIGN KEY (label) REFERENCES publisher (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS file (
    id integer PRIMARY KEY,
    asset integer NOT NULL,
    basename text NOT NULL,
    dirname text NOT NULL,
    digest text NOT NULL,
    size integer NOT NULL,
    filetype integer NOT NULL,
    FOREIGN KEY (asset) REFERENCES asset (id),
    FOREIGN KEY (filetype) REFERENCES file_type (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS label (
    id integer PRIMARY KEY,
    name text NOT NULL,
    dirname text NOT NULL,
    cover text
);
""",
"""
CREATE TABLE IF NOT EXISTS series (
    id integer PRIMARY KEY,
    name text NOT NULL,
    label integer,
    FOREIGN KEY (label) REFERENCES label (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS members (
    id integer PRIMARY KEY,
    asset integer NOT NULL,
    series integer NOT NULL,
    ordinal integer,
    FOREIGN KEY (asset) REFERENCES asset (id),
    FOREIGN KEY (series) REFERENCES series (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS tag (
    id integer PRIMARY KEY,
    name text NOT NULL,
    value text
);
""",
"""
CREATE TABLE IF NOT EXISTS source (
    id integer PRIMARY KEY,
    name text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS tags (
    id integer PRIMARY KEY,
    tag integer NOT NULL,
    asset integer,
    file integer,
    label integer,
    series integer,
    source integer NOT NULL,
    FOREIGN KEY (asset) REFERENCES asset (id),
    FOREIGN KEY (file) REFERENCES file (id),
    FOREIGN KEY (label) REFERENCES label (id),
    FOREIGN KEY (series) REFERENCES series (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS filetype (
    id integer PRIMARY KEY,
    name text NOT NULL
);
""",
"""
INSERT INTO source (name)
    VALUES ("system"), ("user");
"""
]

# note: the 'finalize' argument in new_<entity>() methods allows for a 
#       single, atomic commit of all asset data to the database. 



class Interface(DatabaseInterface):

    def __init__(self, dbpath="") -> None:
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)

    def new_asset(self, cname: str, 
                        label: int,
                        finalize=False
                        ) -> bool:
        self.c.execute(f"""
        INSERT INTO asset (name, label) VALUES (?,?)
        ;""", (cname, label))
        if finalize:
            self.db.commit()
        return True

    def update_asset_cover(self, asset: int, path: str) -> bool:
        self.c.execute("UPDATE asset SET cover = ? WHERE id = ?;",
                           (path, asset))
        self.db.commit()
        return True

    def update_asset_year(self, asset: int, year: int) -> bool:
        self.c.execute("UPDATE asset SET year = ? WHERE id = ?;",
                           (year, asset))
        self.db.commit()
        return True

    def update_asset_digest(self, asset: int, digest: str) -> bool:
        self.c.execute("UPDATE asset SET digest = ? WHERE id = ?;",
                           (digest, asset))
        self.db.commit()
        return True

    def new_file(self, asset: int,
                       basename: str,
                       dirname: str,
                       digest: str,
                       size: int,
                       filetype: int,
                       finalize=False
                       ) -> bool:
        self.c.execute("""
        INSERT INTO file (asset, basename, dirname, digest, size, filetype)
        VALUES (?,?,?,?,?,?)
        ;""", (asset, basename, dirname, digest, size, filetype))
        if finalize:
            self.db.commit()
        return True

    def new_label(self, name: str, dirname: str) -> bool:
        self.c.execute("INSERT INTO label (name, dirname) VALUES (?, ?);",
                       (name, dirname))
        self.db.commit()
        return True

    def update_label_cover(self, label: int, path: str) -> bool:
        self.c.execute("UPDATE label SET cover = ? WHERE id = ?;",
                           (path, label))
        self.db.commit()
        return True

    def new_filetype(self, name: str) -> bool:
        self.c.execute("INSERT INTO filetype (name) VALUES (?);", (name.lower(),))
        self.db.commit()
        return True

    def new_series(self, name: str) -> bool:
        self.c.execute("INSERT INTO series (name) VALUES (?);", (name,))
        self.db.commit()
        return True

    def add_members(self, asset: int, series: int, ordinal=None) -> bool:
        if ordinal:
            self.c.execute("INSERT INTO members (asset, series, ordinal)\
                            VALUES (?,?,?);",
                           (asset, series, ordinal))
        else:
            self.c.execute("INSERT INTO members (asset, series) VALUES (?,?);",
                           (asset, series))
        self.db.commit()
        return True

    def new_tag(self, name: str, value=None) -> bool:
        if value:
            self.c.execute("INSERT INTO tag (name, value) VALUES (?,?);",
                           (name, value))
        else:
            self.c.execute("INSERT INTO tag (name) VALUES (?);", (name,))
        self.db.commit()
        return True

    def add_tags(self, tag: int, target_type: str, target: int) -> bool:
        query = f"INSERT INTO tags (name, {target_type}) VALUES (?, ?);"
        self.c.execute(query, (tag, target))
        self.db.commit()
        return True


    def asset_exists(self, cname: str) -> bool:
        self.c.execute("SELECT * FROM asset WHERE name=?;", (cname,))
        return bool(self.c.fetchone())

    def asset_id(self, cname: str) -> int:
        self.c.execute("SELECT id FROM asset WHERE name=?;", (cname,))
        return self.c.fetchone()[0]

    def asset_data(self, asset: int) -> Dict:
        self.c.execute("SELECT * FROM asset WHERE id=?;", (asset,))
        result = self.c.fetchone()[0]
        return {
            "id": result[0],
            "name": result[1],
            "label": result[2],
            "digest": result[3],
            "year": result[4],
            "cover": result[5]
        }

    def filetype_exists(self, ext: str) -> bool:
        self.c.execute("SELECT * FROM filetype WHERE name=?;", (ext.lower(),))
        return bool(self.c.fetchone())

    def filetype_id(self, ext: str) -> int:
        self.c.execute("SELECT id FROM filetype WHERE name=?;", (ext.lower(),))
        return self.c.fetchone()[0]

    def label_exists(self, name: str) -> bool:
        self.c.execute("SELECT * FROM label WHERE name=?;", (name,))
        return bool(self.c.fetchone())

    def label_id_by_name(self, name: str) -> int:
        self.c.execute("SELECT id FROM label WHERE name=?;", (name,))
        return self.c.fetchone()[0]

    def label_id_by_dirname(self, dirname: str) -> int:
        self.c.execute("SELECT id FROM label WHERE dirname=?;", (dirname,))
        return self.c.fetchone()[0]

    def tag_exists(self, name: str) -> bool:
        self.c.execute("SELECT * FROM tag WHERE name=?;", (name,))
        return bool(self.c.fetchone())

    def tag_id(self, name: str) -> int:
        self.c.execute("SELECT id FROM tag WHERE name=?;", (name,))
        return self.c.fetchone()[0]





    
    def asset_cover(self, asset: int) -> str:
        self.c.execute("SELECT cover FROM asset WHERE id = ?;",
                       (asset,))    
        return self.c.fetchone()

    def label_cover(self, label: int) -> str:
        self.c.execute("SELECT cover FROM label WHERE id = ?;",
                       (label,))    
        return self.c.fetchone()





    def get_asset_file_ids(self, asset_id: int) -> List[int]:
        self.c.execute("SELECT id FROM file WHERE asset = ?", (asset_id,))
        return self.c.fetchall()

    #def get_path(self, asset: str)


    def all_label_dirs(self) -> List[str]:
        self.c.execute("SELECT dirname FROM label")
        return [_i[0] for _i in self.c.fetchall()]

    def all_assets_by_label(self, label_id: int) -> List[str]:
        self.c.execute("SELECT name FROM asset WHERE label=?;", (label_id,))
        return [_i[0] for _i in self.c.fetchall()]
