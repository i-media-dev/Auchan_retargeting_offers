from datetime import datetime
import inspect
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
        file_path = (
            folder_path / f'{parent_folder}_'
            f'{datetime.now().strftime(DATETIME_FOR_NAME)}.xlsx'
        )
        df.to_excel(file_path, index=False)
    else:
        # Вывести в консоль
        pd.set_option('display.width', DISPLAY_WIDTH)
        pd.set_option('display.max_columns', DISPLAY_MAX_COLUMNS)
        pd.set_option('display.max_colwidth', DISPLAY_MAX_COLWIDTH)
        pd.set_option('display.max_rows', DISPLAY_MAX_ROWS)
        print(df)


def check_dataframe_exist(df):
    """Проверить, что DataFrame существует и не пуст."""

    if df is None or df.empty:
        caller_frame = inspect.currentframe().f_back
        caller_name = caller_frame.f_code.co_name
        raise ValueError(f'Метод {caller_name}: '
                         f'DataFrame пуст или не инициализирован')


def set_none_values(element: pd.Series) -> pd.Series:
    """Установить значения None для полей новой пары."""

    element['new_id'] = None
    element['new_name'] = None
    element['new_url'] = None
    element['new_price'] = None

    return element
