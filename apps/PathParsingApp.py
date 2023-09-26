
from typing import List

from apps.ConfiguredApp import App
from interfaces.Interface import DatabaseInterface
from interfaces.Catalog import CatalogInterface
from interfaces.Tokens import TokensInterface
from util import Logs
from util import Parser


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
)
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
    
    def is_file_parsed(self, file_id: str, catalog: str) -> str:
        self.c.execute("SELECT id FROM data WHERE file = ? and catalog = ?;",
                       (file_id, catalog))
        return bool(self.c.fetchone())
    
    def are_asset_files_parsed(self, asset_id: str, catalog: str) -> bool:
        self.c.execute("SELECT id FROM log WHERE asset = ?;",
                       (asset_id, catalog))
        return bool(self.c.fetchone())

    def parsed_asset_ids(self, catalog: str) -> List[str]:
        self.c.execute("SELECT DISTINCT asset FROM log WHERE catalog = ?;",
                       (catalog,))
        return [_i[0] for _i in self.c.fetchall()]


class PathParser(App):

    def __init__(self, sonicat_path: str, app_key: str) -> None:
        super().__init__(sonicat_path, app_key)
        catalog_db_path = f"{self.cfg.data}/catalog/{self.cfg.name}.sqlite"
        self.catalog = CatalogInterface(catalog_db_path)
        self.cfg.name = "PathParser"
        self.cfg.log += "/tokens"
        self.log = Logs.initialize_logging(self.cfg)
        self.log.info(f"BEGIN {self.cfg.name} application initialization")
        self.log.debug("Connected to Catalog interface")
        self.parser = Parser.AudioFilePathParser()
        self.log.debug("Path Parser initialized")
        path_parsing_db_path = f"{self.cfg.data}/tokens/PathParsing.sqlite"
        self.record = Interface(path_parsing_db_path)
        self.log.debug("Connected to database")
        tokens_db_path = f"{self.cfg.data}/tokens/Tokens.sqlite"
        self.tokens = TokensInterface(tokens_db_path)
        self.log.debug("Connected to Tokens interface")
        self.audio_type_ids = [self.catalog.filetype_id(_ext) 
                           for _ext in self.parser.audio_exts 
                           if self.catalog.filetype_exists(_ext)]
        self.log.info(f"END application initialization: Success")

    def parse_path(self, file_id) -> bool:
        path = self.catalog.file_path(file_id)
        result: Parser.ParsedAudioFilePath = self.parser.parse_path(path)
        self.tokens.add_file_tokens(file_id,
                                    self.cfg.app_key,
                                    result.tokens,
                                    finalize=False)
        self.record.new_path_parse(file_id,
                                   self.cfg.app_key,
                                   result.tempo,
                                   result.key,
                                   finalize=False)
        return True

    def parse_asset_file_paths(self, asset_id: str) -> bool:
        file_ids = self.catalog.file_ids_by_asset(asset_id)
        for _fid in file_ids:
            if not self.catalog.file_filetype_id(_fid) in self.audio_type_ids:
                continue
            self.parse_path(_fid)
        self.tokens.db.commit()
        self.record.log_asset_parse(asset_id, self.cfg.app_key, finalize=True)
        return True

    def asset_with_unparsed_files(self) -> str:
        parsed_assets = [str(_i) for _i
                         in self.record.parsed_asset_ids(self.cfg.app_key)]
        return self.catalog.all_asset_ids(limit=1,
                                          excluding=parsed_assets
                                          )[0]

    def run(self):
        target_asset_id = self.asset_with_unparsed_files()
        while target_asset_id:
            cname = self.catalog.asset_cname(target_asset_id)
            self.log.info(f"BEGIN parsing audio file paths in {cname}")
            self.parse_asset_file_paths(target_asset_id)
            self.log.info(f"END asset file path parsing: Success")
            target_asset_id = self.asset_with_unparsed_files()
        return True
    

