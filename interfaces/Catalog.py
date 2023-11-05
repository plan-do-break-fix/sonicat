
import shutil
from typing import Dict, List

from interfaces.Interface import DatabaseInterface
from util.NameUtility import NameUtility
from util.FileUtility import Archive


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



class CatalogInterface(DatabaseInterface):

    def __init__(self, dbpath="") -> None:
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)

  #Insert Statement Methods
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

    def rename_asset(self, asset_id: int, new_cname: str) -> bool:
        self.c.execute("UPDATE asset SET name = ? WHERE id = ?;",
                           (new_cname, asset_id))
        self.db.commit()
        return True

    def new_file(self, asset_id: int,
                       basename: str,
                       dirname: str,
                       size: int,
                       filetype: int,
                       finalize=False
                       ) -> bool:
        self.c.execute("""
        INSERT INTO file (asset, basename, dirname, size, filetype)
        VALUES (?,?,?,?,?)
        ;""", (asset_id, basename, dirname, size, filetype))
        if finalize:
            self.db.commit()
        return True
    
    def new_extensionless_file(self, asset_id: int,
                       basename: str,
                       dirname: str,
                       size: int,
                       finalize=False
                       ) -> bool:
        self.c.execute("""
        INSERT INTO file (asset, basename, dirname, size)
        VALUES (?,?,?,?)
        ;""", (asset_id, basename, dirname, size))
        if finalize:
            self.db.commit()
        return True

    def new_label(self, name: str, dirname: str) -> bool:
        self.c.execute("INSERT INTO label (name, dirname) VALUES (?, ?);",
                       (name, dirname))
        self.db.commit()
        return True

    def new_filetype(self, name: str) -> bool:
        self.c.execute("INSERT INTO filetype (name) VALUES (?);", (name.lower(),))
        self.db.commit()
        return True

  #Update Asset/File Methods
    def update_asset_name(self, asset_id: str, new_name: str) -> bool:
        self.c.execute("UPDATE asset SET name = ? WHERE id = ?;",
                       (new_name, asset_id))
        return True
    
    def update_asset_label(self, asset_id: str, new_label: int) -> bool:
        self.c.execute("UPDATE asset SET label = ? WHERE id = ?;",
                       (new_label, asset_id))
        self.db.commit()
        return True
    
    def remove_asset(self, asset_id: str) -> bool:
        self.c.execute("DELETE FROM asset WHERE id = ?;",
                       (asset_id,))
        self.db.commit()
        return True
    
    def remove_file(self, file_id: str) -> bool:
        self.c.execute("DELETE FROM asset WHERE id = ?;",
                       (file_id,))
        self.db.commit()
        return True

    def remove_files_by_name(self, asset_id: str, fname: str) -> bool:
        self.c.execute("DELETE FROM file WHERE asset = ? and LOWER(basename) = ?;",
                       (asset_id, fname.lower()))
        self.db.commit()
        return True

  #ID Getter Methods
    def asset_id(self, cname: str) -> str:
        self.c.execute("SELECT id FROM asset WHERE name = ?;", (cname,))
        return self.c.fetchone()[0]

    def asset_ids_by_label(self, label_id: str) -> List[str]:
        self.c.execute("SELECT id FROM asset WHERE label = ?;", (label_id,))
        return self.c.fetchall()

    def asset_cnames_by_label(self, label_id: str) -> List[str]:
        self.c.execute("SELECT cname FROM asset WHERE label = ?;", (label_id,))
        return self.c.fetchall()
   
    def all_asset_ids(self, limit=0, excluding=[]) -> List[str]:
        query_str = "SELECT id FROM asset"
        if excluding:
            query_str += f" WHERE id NOT IN ({','.join(excluding)})"
        if limit > 0:
            query_str += f" LIMIT {limit}"
        query_str += ";"
        self.c.execute(query_str)
        return [_i[0] for _i in self.c.fetchall()]

    def all_asset_ids_in_range(self, i_id=0, f_id=0) -> List[str]:
        if not i_id > 0:
            i_id = 1
        if not f_id > 0:
            self.c.execute("SELECT id FROM asset ORDER BY id DESC LIMIT 1;")
            res = self.c.fetchone()
            f_id = res[0] if res else 0
        self.c.execute("SELECT id FROM asset WHERE id >= ? and id <= ? ORDER BY id ASC;",
                       (i_id, f_id))
        return self.c.fetchall()


    def asset_ids_from_file_ids(self, file_ids: List[str]) -> List[str]:
        self.c.execute("SELECT DISTINCT asset FROM files"\
                       f" WHERE id IN ({','.join(file_ids)});")
        return [_i[0] for _i in self.c.fetchall()]
        
    def filetype_id(self, ext: str) -> str:
        self.c.execute("SELECT id FROM filetype WHERE name = ?;", (ext.lower(),))
        return self.c.fetchone()[0]

    def label_id_by_name(self, name: str) -> str:
        self.c.execute("SELECT id FROM label WHERE name = ?;", (name,))
        return self.c.fetchone()[0]

    def label_id_by_dirname(self, dirname: str) -> str:
        self.c.execute("SELECT id FROM label WHERE dirname = ?;", (dirname,))
        return self.c.fetchone()[0]
    
    def file_id_by_name(self, basename: str, dirname: str) -> str:
        self.c.execute("SELECT id FROM file WHERE basename = ? and dirname = ?;",
                       (basename, dirname))
        return self.c.fetchone()[0]
    
    def file_ids_by_asset(self, asset_id: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE asset = ?;", (asset_id,))
        return [_i[0] for _i in self.c.fetchall()]

    def file_ids_by_asset_and_type(self, asset_id: str, filetype_id: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE asset = ? and filetype = ?;",
                       (asset_id, filetype_id))
        return [_i[0] for _i in self.c.fetchall()]

    def file_ids_by_digest(self, digest: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE digest = ?;", (digest,))
        return [_i[0] for _i in self.c.fetchall()]

    def file_ids_by_label(self, label_id: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE label = ?;", (label_id,))
        return [_i[0] for _i in self.c.fetchall()]

         
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

  #Data Getter Methods
    #Asset Data
    def asset_cname(self, asset_id: str) -> str:
        self.c.execute("SELECT name FROM asset WHERE id = ?;",
                       (asset_id,))
        result = self.c.fetchone()
        return result[0] if result else ""

    def asset_data(self, asset_id: str) -> Dict:
        self.c.execute("SELECT * FROM asset WHERE id = ?;", (asset_id,))
        result = self.c.fetchone()[0]
        return {
            "id": result[0],
            "name": result[1],
            "label": result[2],
            "digest": result[3],
            "year": result[4],
            "cover": result[5]
        }
    
    #File Data
    def file_path(self, file_id: str) -> str:
        self.c.execute("SELECT basename, dirname FROM file WHERE id = ?;",
                       (file_id,))
        result = self.c.fetchone()
        return f"{result[1]}/{result[0]}"

    def file_data(self, file_id: int) -> Dict:
        self.c.execute("SELECT * FROM file WHERE id = ?;", (file_id,))
        result = self.c.fetchone()
        return {
            "id": result[0],
            "asset": result[1],
            "basename": result[2],
            "dirname": result[3],
            "size": result[4],
            "filetype": result[5],
            "digest": result[6]
        }
    
    def asset_files_by_type(self, asset: str, filetype: str) -> List[str]:
        self.c.execute("SELECT id FROM file WHERE asset = ? and filetype = ?;",
                       (asset, filetype))
        return self.c.fetchall()

    def file_filetype_id(self, file_id: str) -> str:
        self.c.execute("SELECT filetype FROM file WHERE ID = ?;", (file_id,))
        return self.c.fetchone()[0]

    # Label Data
    def label(self, label_id: str) -> str:
        self.c.execute("SELECT name FROM label WHERE id = ?", (label_id,))
        return self.c.fetchone()[0]

    def label_dir(self, label_id: str) -> str:
        self.c.execute("SELECT dirname FROM label WHERE id = ?", (label_id,))
        return self.c.fetchone()[0]

    def all_label_dirs(self) -> List[str]:
        self.c.execute("SELECT dirname FROM label")
        return [_i[0] for _i in self.c.fetchall()]

    def asset_cnames_by_label(self, label_id: str) -> List[str]:
        self.c.execute("SELECT name FROM asset WHERE label = ?;", (label_id,))
        return [_i[0] for _i in self.c.fetchall()]

    def asset_ids_by_label(self, label_id: str) -> List[str]:
        self.c.execute("SELECT id FROM asset WHERE label = ?;", (label_id,))
        return [_i[0] for _i in self.c.fetchall()]

    # Filetype Data
    def filetype(self, filetype_id: str) -> str:
        self.c.execute("SELECT name FROM filetype WHERE id = ?",
                       (filetype_id,))
        return self.c.fetchone()[0]

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

  # Export
    def export_asset_precheck(self, asset_id) -> bool:
        if not self.asset_is_managed(asset_id):
            return False
        return True
    
    def export_asset_to_temp(self, asset_id, managed_path, temp_path) -> bool:
        if not self.export_asset_precheck(asset_id):
            return False
        if not shutil.os.path.isdir(temp_path):
            shutil.os.makedirs(temp_path, exist_ok=True)
        cname = self.asset_cname(asset_id)
        label_dir = NameUtility.label_dir_from_cname(cname)
        archive_path = f"{managed_path}/{label_dir}/{cname}.rar"
        shutil.copyfile(archive_path, f"{temp_path}/{cname}.rar")
        Archive.restore(f"{temp_path}/{cname}.rar")
        shutil.os.remove(f"{temp_path}/{cname}.rar")
        return True

  # Complex ID Getters
    def label_file_id_pairs(self, filetype_ids=""):
        query = "SELECT a.label, f.id FROM"\
                "  asset a JOIN"\
                "  file f ON a.id = f.asset"
        if filetype_ids:
            query += f" WHERE f.filetype = {filetype_ids.pop()}"
            while filetype_ids:
                query += f" OR f.filetype = {filetype_ids.pop()}"
        query += ";"
        self.c.execute(query)
        return self.c.fetchall()



  #  
  #  def asset_file_id_dict(self, filetype_ids=""):
  #      query = "SELECT asset, id FROM file"
  #      if filetype_ids:
  #          query += f" WHERE filetype = {filetype_ids.pop()}"
  #          while filetype_ids:
  #              query += f" OR filetype = {filetype_ids.pop()}"
  #      query += ";"
  #      self.c.execute(query)
  #      output = {}
  #      for _res in self.c.fetchall():
  #          if _res[0] not in output.keys():
  #              output[_res[0]] = []
  #          output[_res[0]].append(_res[1])
  #      return output
#
  #  def file_asset_id_pairs(self, filetype_ids="") -> List[Tuple[str]]:
  #      query = "SELECT id, asset FROM file"
  #      if filetype_ids:
  #          query += f" WHERE filetype = {filetype_ids.pop()}"
  #          while filetype_ids:
  #              query += f" OR filetype = {filetype_ids.pop()}"
  #      query += ";"
  #      self.c.execute(query)
  #      return self.c.fetchall()
#
  #  def asset_label_id_pairs(self) -> List["str"]:
  #      self.c.execute("SELECT id, label FROM asset;")
  #      return self.c.fetchall()
  #  
