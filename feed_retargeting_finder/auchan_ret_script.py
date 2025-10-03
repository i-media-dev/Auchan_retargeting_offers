import xml.etree.ElementTree as ET

import pandas as pd
import requests
from constants import (
    MAX_APPRECIATION_COEF,
)


class RemarketingFeedMatch:
    """Класс для поиска соответствий в фиде для офферного ремаркетинга."""

    def __init__(self, feed):
        tree = requests.get(feed)
        self.root = ET.fromstring(tree.content)
        self.df = None

    def feed_to_dataframe(self, rows_limit: int = None):
        """Преобразовать фид в DataFrame"""

        data = []

        for i, value in enumerate(self.root.iter('offer')):
            if rows_limit and i >= rows_limit:
                break

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

        self.df['first_3'] = self.df['name'].apply(
            lambda x: ' '.join(x.split()[:3])
        )
        self.df['matches_count'] = (
            self.df['first_3'].map(
                self.df['first_3'].value_counts()
            )
        )
        self.df = self.df[self.df['matches_count'] > 1]

    def _pair(self, element: pd.Series) -> pd.Series:
        """Подобрать пару для замены."""

        try:
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
                element['new_id'] = None
                element['new_name'] = None
                element['new_url'] = None
                element['new_price'] = None

        except Exception as e:
            print(f"Error: {str(e)}. Element: {element}")

        return element

    def apply_pair(self):
        """Применить pair ко всем строкам."""

        try:
            self.df = self.df.apply(lambda x: self._pair(x), axis=1)

        except KeyError:
            print('DataFrame пуст')

    def final_view(self):
        """Убрать вспомогательные столбцы и отфильтровать строки без пары."""

        self.df = self.df.drop(columns=['first_3', 'matches_count'])
        self.df = self.df[self.df['new_name'].notna()]
