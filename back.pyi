from typing import Any

def setup_directories() -> None: ...
def make_folder(year: Any, month: Any, day: Any, hour: Any): ...
def assets_download(dic_start_params: Any, dic_end_params: Any) -> None: ...
def csv_folders(year: Any, month: Any, day: Any, hour: Any, root_dir: Any = ...): ...
def merge_csv(dir: Any, csv_dir: Any, name: Any) -> None: ...
def flash_csv(assets_dir: Any, csv_dir: Any, root_dir: Any): ...
def group_csv(assets_dir: Any, csv_dir: Any, root_dir: Any) -> None: ...
def event_csv(assets_dir: Any, csv_dir: Any, root_dir: Any) -> None: ...
def create_csv(year: Any, month: Any, day: Any, hour: Any, category: Any, root_dir: Any = ...) -> None: ...
def data_acces(params: Any) -> None: ...
def generate_map(dic_start_params: Any, dic_end_params: Any, radius: Any, center: Any, category: str = ...) -> None: ...
