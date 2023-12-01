

from interfaces.Interface import DatabaseInterface

from typing import Dict, List, Tuple


SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS asset (
    id integer PRIMARY KEY,
    name text NOT NULL,
    label integer NOT NULL,
    managed integer NOT NULL,
    FOREIGN KEY (label)
      REFERENCES label (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS file (
    id integer PRIMARY KEY,
    asset integer NOT NULL,
    basename text NOT NULL,
    dirname text NOT NULL,
    size integer,
    filetype integer,
    digest text,
    FOREIGN KEY (asset)
      REFERENCES asset (id)
      ON DELETE CASCADE,
    FOREIGN KEY (filetype)
      REFERENCES filetype (id)
);
""",
"""
CREATE TABLE IF NOT EXISTS label (
    id integer PRIMARY KEY,
    name text NOT NULL,
    dirname text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS filetype (
    id integer PRIMARY KEY,
    name text NOT NULL
);
"""
]


class ReadInterface(DatabaseInterface):

    def __init__(self, dbpath="") -> None:
        super().__init__(dbpath)

  # Asset ID Methods
    def all_asset_ids(self) -> List[str]:
        self.c.execute("SELECT id FROM asset;")
        return [_i[0] for _i in self.c.fetchall()]

    def asset_id(self, cname: str) -> str:
        self.c.execute("SELECT id FROM asset WHERE name = ?;", (cname,))
        return self.c.fetchone()[0]

    def asset_ids_by_label(self, label_id: str) -> List[str]:
        self.c.execute("SELECT id FROM asset WHERE label = ?;", (label_id,))
        return [_i[0] for _i in self.c.fetchall()]

    def asset_id_from_file(self, file_id: str) -> str:
        self.c.execute("SELECT asset FROM file WHERE id = ?;", (file_id,))
        return self.c.fetchone()[0]
        
  # File ID Methods
    def file_id_by_name(self, basename: str, dirname: str) -> str:
        self.c.execute("SELECT id FROM file WHERE basename = ? and dirname = ?;",
                       (basename, dirname))
        return self.c.fetchone()[0]
    
    def file_ids_by_asset(self, asset_id: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE asset = ?;", (asset_id,))
        return [_i[0] for _i in self.c.fetchall()]

    def file_ids_by_label(self, label_id: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE label = ?;", (label_id,))
        return [_i[0] for _i in self.c.fetchall()]

    def file_ids_by_asset_and_type(self, asset_id: str, filetype_id: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE asset = ? and filetype = ?;",
                       (asset_id, filetype_id))
        return [_i[0] for _i in self.c.fetchall()]

    def file_ids_by_digest(self, digest: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE digest = ?;", (digest,))
        return [_i[0] for _i in self.c.fetchall()]

  # Label ID Methods
    def label_id_by_name(self, name: str) -> str:
        self.c.execute("SELECT id FROM label WHERE name = ?;", (name,))
        return self.c.fetchone()[0]

    def label_id_by_dirname(self, dirname: str) -> str:
        self.c.execute("SELECT id FROM label WHERE dirname = ?;", (dirname,))
        return self.c.fetchone()[0]

  #Asset Data
    def encode_asset_data(self, result: Tuple[str]) -> Dict:
        return {
            "id": result[0],
            "name": result[1],
            "label": result[2],
            "digest": result[3],
            "year": result[4],
            "cover": result[5]
        }

    def asset_data(self, asset_id: str) -> Dict:
        self.c.execute("SELECT * FROM asset WHERE id = ?;", (asset_id,))
        result = self.c.fetchone()[0]
        return self.encode_asset_data(result) if result else {}
    
    def asset_data_by_label(self, label_id: str) -> List[Dict]:
        self.c.execute("SELECT * FROM asset WHERE label = ?;", (label_id,))
        results = self.c.fetchall()
        return [self.encode_asset_data(_r) for _r in results] if results else []
    
  #File Data
    def encode_file_data(self, result: Tuple[str]) -> Dict:
        return {"id": result[0],
                "asset": result[1],
                "basename": result[2],
                "dirname": result[3],
                "size": result[4],
                "filetype": result[5],
                "digest": result[6]
                }

    def file_data(self, file_id: str) -> Dict:
        self.c.execute("SELECT * FROM file WHERE id = ?;", (file_id,))
        result = self.c.fetchone()
        return self.encode_file_data(result)
        
    def file_data_by_asset_and_type(self, asset_id: str, filetype_id: str) -> List[Dict]:
        self.c.execute("SELECT * FROM file WHERE asset = ? AND filetype = ?;",
                       (asset_id, filetype_id))
        results = self.c.fetchall()
        if not results:
            return []
        return [self.encode_file_data for res in results]

  # Label Data
    def all_label_dirs(self) -> List[str]:
        self.c.execute("SELECT dirname FROM label")
        return [_i[0] for _i in self.c.fetchall()]
    
    def label_dir(self, label_id: str) -> str:
        self.c.execute("SELECT dirname FROM label WHERE id = ?", (label_id,))
        return self.c.fetchone()[0]

    def label_name(self, label_id: str) -> str:
        self.c.execute("SELECT name FROM label WHERE id = ?", (label_id,))
        return self.c.fetchone()[0]

  # Filetype Methods
    def filetype_id(self, ext: str) -> str:
        self.c.execute("SELECT id FROM filetype WHERE name = ?;", (ext.lower(),))
        return self.c.fetchone()[0]

    def filetype_name(self, filetype_id: str) -> str:
        self.c.execute("SELECT name FROM filetype WHERE id = ?",
                       (filetype_id,))
        return self.c.fetchone()[0]
         
  #Boolean Check Methods
    def asset_exists(self, cname: str) -> bool:
        self.c.execute("SELECT * FROM asset WHERE name = ?;", (cname,))
        return bool(self.c.fetchone())

    def asset_is_managed(self, asset_id: str) -> bool:
        self.c.execute("SELECT managed FROM asset WHERE id = ?;", (asset_id,))
        return bool(self.c.fetchone()[0])
    
    def filetype_exists(self, ext: str) -> bool:
        self.c.execute("SELECT * FROM filetype WHERE name = ?;", (ext.lower(),))
        return bool(self.c.fetchone())

    def label_exists(self, name: str) -> bool:
        self.c.execute("SELECT * FROM label WHERE name = ?;", (name,))
        return bool(self.c.fetchone())

  # Counts
    def count_labels(self) -> int:
        self.c.execute("SELECT COUNT id FROM label;")
        result = self.c.fetchone()[0]
        return int(result) if result else 0

    def count_assets(self) -> int:
        self.c.execute("SELECT COUNT id FROM asset;")
        result = self.c.fetchone()[0]
        return int(result) if result else 0

    def count_label_assets(self, label_id: int) -> int:
        self.c.execute("SELECT COUNT if FROM asset WHERE label = ?;",
                       (label_id,))
        result = self.c.fetchone()[0]
        return int(result) if result else 0

    def count_assets_by_labels(self) -> Dict[str, int]:
        pass


class WriteInterface(ReadInterface):

    def __init__(self, dbpath="") -> None:
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)
        self.db.commit()

    def new_asset(self, cname: str, 
                        label_id: str,
                        managed: int,
                        finalize=False
                        ) -> bool:
        self.c.execute("INSERT INTO asset (name, label, managed) VALUES (?,?,?);",
                       (cname, label_id, managed))
        if finalize:
            self.db.commit()
        return True

   # Calls to new_extensionless_file() can now just call the std method
    def new_file(self, asset_id: int,
                       basename: str,
                       dirname: str,
                       size: int,
                       filetype_id="",
                       finalize=False
                       ) -> bool:
        query = "INSERT INTO file (asset, basename, dirname, size"
        args = [asset_id, basename, dirname, size]
        if filetype_id:
            query += ", filetype) VALUES (?,?,?,?,?);"
            args.append(filetype_id)
        else:
            query += ") VALUES (?,?,?,?);"
        self.c.execute(query, args)
        if finalize:
            self.db.commit()
        return True
