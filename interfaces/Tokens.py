
from typing import Dict, List

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

class TokensInterface(DatabaseInterface):

    def __init__(self, dbpath="") -> None:
        super().__init__(dbpath)
        for statement in SCHEMA:
            self.c.execute(statement)

    def new_token(self, token: str) -> bool:
        self.c.execute("INSERT INTO token (value) VALUES (?);", (token,))
        self.db.commit()

    def new_file_token(self, token_id: str,
                             file_id: str,
                             app_key: str,
                             finalize=False
                             ) -> bool:
        self.c.execute("INSERT INTO filepathtokens (token, file, catalog) VALUES"\
                       "(?,?,?);", (token_id, file_id, app_key))
        if finalize:
            self.db.commit()
        return True

    def add_file_tokens(self, file_id: str,
                              app_key: str,
                              tokens: List[str],
                              finalize=False
                              ) -> bool:
        for _token in tokens:
            token_id = self.token_id_with_insert(_token)
            self.new_file_token(token_id, file_id, app_key, finalize=False)
        if finalize:
            self.db.commit()
        return True

    def token_id(self, token: str) -> int:
        self.c.execute("SELECT id FROM token WHERE value = ?;", (token,))
        result = self.c.fetchone()
        return result[0] if result else 0
    
    def token_id_with_insert(self, token: str) -> int:
        _id = self.token_id(token)
        if not _id:
            self.new_token(token)
        self.c.execute("SELECT last_insert_rowid();")
        return self.c.fetchone()[0]
    
    def token_ids_by_file(self, file_id: str, app_key: str) -> List[str]:
        self.c.execute("SELECT id FROM token WHERE file = ? and catalog = ?;",
                       (file_id, app_key))
        return self.c.fetchall()
    
    def tokens_by_ids(self, token_ids: List[str]) -> List[str]:
        self.c.execute(f"SELECT * FROM token WHERE id IN ({','.join(token_ids)});")
        return self.c.fetchall()