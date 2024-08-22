from pathlib import Path


class DBFileSystem:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def create_table_path(self, table_name: str) -> None:
        pass

    def create_partition_path(self, table_name: str, partition_id: int) -> None:
        pass

    def create_segment_path(self, table_name: str, partition_id: int, segment_id: int) -> None:
        pass

    def delete_table_path(self, table_name: str) -> None:
        pass

    def delete_partition_path(self, table_name: str, partition_id: int) -> None:
        pass

    def delete_segment_path(self, table_name: str, partition_id: int, segment_id: int) -> None:
        pass

    def get_table_path(self, table_name: str) -> Path:
        pass

    def get_partition_path(self, table_name: str, partition_id: int) -> Path:
        pass

    def get_segment_path(self, table_name: str, partition_id: int, segment_id: int) -> Path:
        pass
