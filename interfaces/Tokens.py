
from cachetools import LRUCache
from typing import Dict, List, Tuple

from interfaces.Interface import DatabaseInterface

SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS token (
    id integer PRIMARY KEY,
    value text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS filepathtokens (
    id integer PRIMARY KEY,
    token integer NOT NULL,
    file integer NOT NULL,
    catalog text
);
""",
]
CACHE_SIZE = 5000000

class TokensInterface(DatabaseInterface):

    def __init__(self, dbpath="") -> None:
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)
        self.cache = LRUCache(maxsize=CACHE_SIZE)
        self.populate_token_id_cache()

    def populate_token_id_cache(self):
        for id_value_pair in self.all_tokens()[:CACHE_SIZE]:
            self.cache[id_value_pair[0]] = id_value_pair[1]

    def new_token(self, token: str) -> bool:
        self.c.execute("INSERT INTO token (value) VALUES (?);", (token,))
        self.db.commit()

    def new_file_token(self, token_id: str,
                             file_id: str,
                             app_key: str,
                             finalize=False
                             ) -> bool:
        self.c.execute("INSERT INTO filepathtokens (token, file, catalog) "\
                       "VALUES (?,?,?);", (token_id, file_id, app_key))
        if finalize:
            self.db.commit()
        return True

    def add_file_tokens(self, file_id: str,
                              app_key: str,
                              tokens: List[str],
                              finalize=False
                              ) -> bool:
        for _token in tokens:
            _id = self.cache.get(_token)
            if not _id:
                self.cache[_token] = self.token_id_with_insert(_token)
                _id = self.cache.get(_token)
            self.new_file_token(_id,
                                file_id,
                                app_key,
                                finalize=False
                                )
        if finalize:
            self.db.commit()
        return True
    
    def token(self, token_id: str) -> str:
        self.c.execute("SELECT value FROM token WHERE id = ?;", (token_id,))
        result = self.c.fetchone()
        return result[0] if result else 0

    def token_id(self, token: str) -> str:
        self.c.execute("SELECT id FROM token WHERE value = ?;", (token,))
        result = self.c.fetchone()
        return result[0] if result else 0
    
    def token_id_with_insert(self, token: str) -> int:
        _id = self.token_id(token)
        if not _id:
            self.new_token(token)
            self.c.execute("SELECT last_insert_rowid();")
            _id = self.c.fetchone()[0]
        return _id
    
    def tokens_by_ids(self, token_ids: List[str]) -> List[str]:
        self.c.execute("SELECT * FROM token"
                       f" WHERE id IN ({','.join(token_ids)});")
        return [_i[1] for _i in self.c.fetchall()]

    def all_tokens(self) -> List[Tuple[str, str]]:
        self.c.execute("SELECT * FROM token;")
        return self.c.fetchall()
    
    def token_ids_by_file(self, file_id: str, app_key: str) -> List[str]:
        self.c.execute("SELECT token FROM filepathtokens"\
                       "  WHERE file = ? AND catalog = ?;",
                       (file_id, app_key))
        return [_i[0] for _i in self.c.fetchall()]

    def token_ids_by_files(self, file_ids: List[str],
                                 app_key: str
                                 ) -> List[str]:
        self.c.execute("SELECT token FROM filepathtokens"\
                       f" WHERE file in ({','.join(file_ids)})"\
                       "  AND catalog = ?;",
                       (app_key,))
        return [_i[0] for _i in self.c.fetchall()]
    
    def unique_token_ids_by_file(self, file_id: str, app_key: str) -> List[str]:
        self.c.execute("SELECT DISTINCT token FROM filepathtokens"\
                       "  WHERE file = ? AND catalog = ?;",
                       (file_id, app_key))
        return [_i[0] for _i in self.c.fetchall()]

    def unique_token_ids_by_files(self, file_ids: List[str],
                                        app_key: str
                                        ) -> List[str]:
        self.c.execute("SELECT DISTINCT token FROM filepathtokens"\
                       f" WHERE file in ({','.join(file_ids)})"\
                       "  AND catalog = ?;",
                       (app_key,))
        return [_i[0] for _i in self.c.fetchall()]

  # COUNTS
    def n_tokens(self, catalog: str) -> int:
        self.c.execute("SELECT COUNT(id) FROM filepathtokens WHERE catalog = ?;",
                       (catalog,))
        return int(self.c.fetchone()[0])

    def n_unique_tokens(self, catalog: str) -> int:
        self.c.execute("SELECT COUNT(DISTINCT id) FROM filepathtokens"\
                       "  WHERE catalog = ?;",
                       (catalog,))
        return int(self.c.fetchone()[0])

    def n_tokens_by_file(self, file_id: str, catalog:str) -> int:
        self.c.execute("SELECT COUNT(id) FROM filepathtokens"\
                       "  WHERE file = ? AND catalog = ?;",
                       (file_id, catalog))
        return [_i[0] for _i in self.c.fetchall()]
        
    def n_unique_tokens_by_file(self, file_id: str, catalog:str) -> int:
        self.c.execute("SELECT COUNT(DISTINCT id) FROM filepathtokens"\
                       "  WHERE file = ? AND catalog = ?;",
                       (file_id, catalog))
        return int(self.c.fetchone()[0])
        
    def n_tokens_by_files(self, file_ids: List[str], catalog:str) -> int:
        self.c.execute("SELECT COUNT(id) FROM filepathtokens"\
                       f" WHERE file in ({','.join(file_ids)})"\
                       "  AND catalog = ?;",
                       (catalog,))
        return int(self.c.fetchone()[0])
        
    def n_unique_tokens_by_files(self, file_ids: List[str],
                                       catalog: str
                                       ) -> int:
        self.c.execute("SELECT COUNT(DISTINCT id) FROM filepathtokens"\
                       f" WHERE file IN ({','.join(file_ids)})"\
                       "  AND catalog = ?;",
                       (catalog,))
        return int(self.c.fetchone()[0])
        return [_i[0] for _i in self.c.fetchall()]


    def n_occurrences_total(self, token_id: str, catalog: str) -> int:
        self.c.execute("SELECT COUNT(id) FROM filepathtokens"\
                       "WHERE token = ? AND catalog = ?;",
                       (token_id, catalog))
        return int(self.c.fetchone()[0])

    def n_occurrences_in_files(self, token_id: str,
                                     file_ids: List[str],
                                     catalog: str
                                     ) -> int:
        self.c.execute("SELECT COUNT(id) FROM filepathtokens"\
                       f" WHERE file in ({','.join(file_ids)})"\
                       "  AND token = ? AND catalog = ?;",
                       (token_id, catalog))
        return int(self.c.fetchone()[0])

    def n_files_with_token(self, token_id: str, catalog: str) -> int:
        self.c.execute("SELECT COUNT(DISTINCT file) FROM filepathtokens"\
                       "  WHERE token = ? AND catalog = ?;",
                       (token_id, catalog))
        return int(self.c.fetchone()[0])

    def n_files_in_database(self, catalog: str) -> str:
        self.c.execute("SELECT COUNT(DISTINCT file) FROM filepathtokens;")
        return int(self.c.fetchone()[0])
