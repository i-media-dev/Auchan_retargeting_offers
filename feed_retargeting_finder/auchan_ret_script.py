import xml.etree.ElementTree as ET
import inspect

import pandas as pd
import requests
from constants import (
    MAX_APPRECIATION_COEF,
    FEED,
)
from utils import (
    set_none_values,
)


class RemarketingFeedMatch:
    """Класс для поиска соответствий в фиде для офферного ремаркетинга."""

    tree = requests.get(FEED, timeout=10)

    @classmethod
    def tree_raise_status(cls):
        """Проверить HTTP статус"""
        cls.tree.raise_for_status()

    @staticmethod
    def _check_dataframe_exist(df):
        """Проверить, что DataFrame существует и не пуст."""

        if df is None or df.empty:
            caller_frame = inspect.currentframe().f_back
            caller_name = caller_frame.f_code.co_name
            raise ValueError(f'Метод {caller_name}: '
                             f'DataFrame пуст или не инициализирован')

    def __init__(self):
        try:
            self.root = ET.fromstring(self.tree.content)
            self.df = None

        except requests.exceptions.RequestException as e:
            raise ValueError(f"Ошибка загрузки фида: {str(e)}")

        except ET.ParseError as e:
            raise ValueError(f"Ошибка парсинга XML: {str(e)}")

    def feed_to_dataframe(self, rows_limit: int = None):
        """Преобразовать фид в DataFrame"""

        if self.root is None:
            raise ValueError("XML фид не загружен")

        data = []

        for i, value in enumerate(self.root.iter('offer')):
            if rows_limit and i >= rows_limit:
                break
            if not value.findtext('price'):
                continue

            offer_data = {
                'id': value.attrib.get('id'),
                'name': value.findtext('name'),
                'url': value.findtext('url'),
                'price': float(value.findtext('price')),
                'categoryId': value.findtext('categoryId'),
            }

            data.append(offer_data)

        self.df = pd.DataFrame(data)

    def filter_has_matches(self):
        """Отфильтровать только те строки,
        для которых есть альтернативное значение.
        """

        self._check_dataframe_exist(self.df)

        try:
            self.df['first_3'] = self.df['name'].apply(
                lambda x: ' '.join(x.split()[:3]))
            self.df['matches_count'] = (
                self.df['first_3'].map(self.df['first_3'].value_counts())
            )
            self.df = self.df[self.df['matches_count'] > 1]

            if self.df.empty:
                raise ValueError('Ни у одного товара '
                                 'не совпадают первые 3 слова')

        except KeyError as e:
            raise ValueError(f"Отсутствует необходимое поле: {str(e)}")

    def _pair(self, element: pd.Series) -> pd.Series:
        """Подобрать пару для замены."""

        if (pd.isna(element['price'])
                or pd.isna(element['first_3'])
                or element['price'] <= 0):
            set_none_values(element)

        filtered_df = self.df[
            (self.df['price'] > element['price'])
            & (self.df['price'] < element['price'] * MAX_APPRECIATION_COEF)
            & (self.df['first_3'] == element['first_3'])
        ].sort_values(by=['price']).reset_index(drop=True)

        if not filtered_df.empty:
            a = filtered_df.iloc[0]
            element['new_id'] = a['id']
            element['new_name'] = a['name']
            element['new_url'] = a['url']
            element['new_price'] = a['price']
        else:
            set_none_values(element)

        return element

    def apply_pair(self):
        """Применить pair ко всем строкам."""

        self._check_dataframe_exist(self.df)
        self.df = self.df.apply(lambda x: self._pair(x), axis=1)

    def final_view(self):
        """Убрать вспомогательные столбцы и отфильтровать строки без пары."""

        self._check_dataframe_exist(self.df)
        self.df = self.df.drop(columns=['first_3', 'matches_count'])
        self.df = self.df[self.df['new_name'].notna()]
