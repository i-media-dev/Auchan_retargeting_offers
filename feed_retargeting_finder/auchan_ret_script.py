import xml.etree.ElementTree as ET
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 100)
# pd.set_option('display.max_rows', None)

class RemarketingFeedMatch:
    """Класс для поиска соответствий в фиде для офферного ремаркетинга"""

tree = requests.get('https://skrypnikovmk.com/moscow.xml')
root = ET.fromstring(tree.content)

def file_save(df: pd.DataFrame, mode: int = 0):
    """Защищенный метод. Создает путь к файлу в указанной папке."""
    if mode == 0:
        # Сохранить в файл
        folder_path = Path(__file__).parent / 'outputs'
        folder_path.mkdir(parents=True, exist_ok=True)

        file_path = folder_path / f'remarketing_find_{datetime.now().strftime("%Y%m%d-%H%M%S")}.xlsx'

        df.to_excel(file_path, index=False)
    elif mode == 1:
        # Вывести в консоль
        print(df)

def feed_to_dataframe(rows_limit: int = None) -> pd.DataFrame:
    """Преобразует фид в DataFrame"""
    data = []

    for i, v in enumerate(root.iter('offer')):
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

    return pd.DataFrame(data)

def filter_has_matches(df):
    """Фильтрует только те строки, для которых есть альтернативное значение"""
    df['first_3'] = df['name'].apply(lambda x: ' '.join(x.split()[:3]))
    df['matches_count'] = df['first_3'].apply(lambda x: df['first_3'].to_list().count(x))
    df = df[df['matches_count'] > 1]

    return df

def pair(element: pd.Series):
    try:
        filtered_mass = mass[
            (mass['price'] > element['price']) &
            (mass['price'] < element['price']*1.2) &
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

mass = feed_to_dataframe(100)
mass = filter_has_matches(mass)
mass = mass.apply(pair, axis=1)
mass = mass.drop(columns=['first_3', 'matches_count'])
mass = mass[mass['new_name'].notna()]
file_save(mass, 0)
