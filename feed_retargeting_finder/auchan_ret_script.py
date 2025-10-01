from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd
import requests

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 100)
# pd.set_option('display.max_rows', None)

class RemarketingFeedMatch:
    """Класс для поиска соответствий в фиде для офферного ремаркетинга"""

    def __init__(self, feed):
        tree = requests.get(feed)
        self.root = ET.fromstring(tree.content)
        self.df = None

    def feed_to_dataframe(self, rows_limit: int = None):
        """Преобразовать фид в DataFrame"""
        data = []

        for i, v in enumerate(self.root.iter('offer')):
            if rows_limit is not None and i >= rows_limit:
                break

            offer_data = {
                'id': v.attrib.get('id'),
                'name': v.findtext('name'),
                'url': v.findtext('url'),
                'price': float(v.findtext('price')),
                'categoryId': v.findtext('categoryId'),
            }

            data.append(offer_data)

        self.df = pd.DataFrame(data)

    def filter_has_matches(self):
        """Отфильтровать только те строки, для которых есть альтернативное значение"""
        df = self.df
        df['first_3'] = df['name'].apply(lambda x: ' '.join(x.split()[:3]))
        df['matches_count'] = df['first_3'].apply(lambda x: df['first_3'].to_list().count(x))
        df = df[df['matches_count'] > 1]

        self.df = df

    def pair(self, element):
        """Подставить альтернативное значение для строки в DataFrame"""
        mass = self.df
        try:
            filtered_mass = mass[
                (mass['price'] > element['price']) &
                (mass['price'] < element['price'] * 1.2) &
                (mass['first_3'] == element['first_3'])
                ].sort_values(by=['price']).reset_index(drop=True)

            if not filtered_mass.empty:
                # Если есть совпадения
                a = filtered_mass.iloc[0]
                element['new_id'] = a['id']
                element['new_name'] = a['name']
                element['new_url'] = a['url']
                element['new_price'] = a['price']
            else:
                # Если нет совпадений
                element['new_id'] = None
                element['new_name'] = None
                element['new_url'] = None
                element['new_price'] = None

        except Exception as e:
            print(f"Error: {str(e)}. Element: {element}")

        return element

    def apply_pair(self):
        """Применить pair ко всем строкам"""
        df = self.df
        df = df.apply(lambda x: self.pair(x), axis=1)
        self.df = df

    def final_view(self):
        """Убрать вспомогательные столбцы и отфильтровать непустые альт. значения"""
        df = self.df
        df = df.drop(columns=['first_3', 'matches_count'])
        df = df.mass[df['new_name'].notna()]
        self.df = df

    def file_save(self, mode: int = 0):
        """Создает путь к файлу в указанной папке."""
        df = self.df
        if mode == 0:
            # Сохранить в файл
            folder_path = Path(__file__).parent / 'outputs'
            folder_path.mkdir(parents=True, exist_ok=True)
            file_path = folder_path / f'remarketing_find_{datetime.now().strftime("%Y%m%d-%H%M%S")}.xlsx'
            df.to_excel(file_path, index=False)
        elif mode == 1:
            # Вывести в консоль
            print(df)

def main():
    feed = 'https://skrypnikovmk.com/moscow.xml'
    ex = RemarketingFeedMatch(feed)
    ex.feed_to_dataframe(50)
    ex.filter_has_matches()
    ex.apply_pair()
    ex.final_view()
    ex.file_save(1)

if __name__ == '__main__':
    main()



