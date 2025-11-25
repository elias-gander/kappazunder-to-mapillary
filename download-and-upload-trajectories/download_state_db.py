import sqlite3


class DownloadStateDb:
    def __init__(self, trajectories):
        self.conn = sqlite3.connect("download-state.db")
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()

        cur.execute("PRAGMA journal_mode=WAL;")

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS trajectories (
            trajectory_id TEXT PRIMARY KEY,

            download_id TEXT,
            download_bytes INTEGER,
            download_expires_at TEXT,

            is_sensor1_completed INTEGER DEFAULT 0,
            is_sensor2_completed INTEGER DEFAULT 0,
            is_sensor3_completed INTEGER DEFAULT 0,
            is_sensor4_completed INTEGER DEFAULT 0
        );
        """
        )

        if cur.execute("SELECT 1 FROM trajectories LIMIT 1;").fetchone() is None:
            for row in trajectories.itertuples():
                cur.execute(
                    """
                    INSERT OR IGNORE INTO trajectories (trajectory_id)
                    VALUES (?);
                """,
                    (row.trajectoryid,),
                )
            print("No download-state.db found, initialized with trajectory IDs.")

        self.conn.commit()

    def execute(self, sql):
        cur = self.conn.cursor()
        cur = cur.execute(sql)
        self.conn.commit()
        return cur.fetchall() if cur.description is not None else None
