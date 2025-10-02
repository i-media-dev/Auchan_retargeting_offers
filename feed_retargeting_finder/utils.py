from datetime import datetime
import pandas as pd
from pathlib import Path

from constants import (
    DISPLAY_WIDTH,
    DISPLAY_MAX_COLUMNS,
    DISPLAY_MAX_COLWIDTH,
    DISPLAY_MAX_ROWS,
    DATETIME_FOR_NAME,
)

def file_save(df: pd.DataFrame, in_file: bool = True):
    """Сохраняет файл в указанной папке."""
    if in_file:
        # Сохранить в файл
        parent_folder = Path(__file__).parent
        folder_path = parent_folder / 'outputs'
        folder_path.mkdir(parents=True, exist_ok=True)
        file_path = folder_path / f'{parent_folder}_{datetime.now().strftime(DATETIME_FOR_NAME)}.xlsx'
        df.to_excel(file_path, index=False)
    else:
        # Вывести в консоль
        pd.set_option('display.width', DISPLAY_WIDTH)
        pd.set_option('display.max_columns', DISPLAY_MAX_COLUMNS)
        pd.set_option('display.max_colwidth', DISPLAY_MAX_COLWIDTH)
        pd.set_option('display.max_rows', DISPLAY_MAX_ROWS)
        print(df)