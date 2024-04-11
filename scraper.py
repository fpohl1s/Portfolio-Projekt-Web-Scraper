# %% [markdown]
# # Shopify Scraper
# 
# #### Ziel:
# - Die Produktdaten in einem Dataframe speichern, inklusive einer Liste der Image_URLs
# - Funktionen in 3 Tasks gliedern für Airflow Automation (Extraktion, Transformieren, Laden)

# %%
# Laden der Bibliotheken und Zugriff auf die Log-In Daten in der .env-Datei
import numpy as np
import pandas as pd
import json

import requests
from bs4 import BeautifulSoup,MarkupResemblesLocatorWarning
import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData, select, insert, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

from time import sleep
from datetime import datetime

import os
from dotenv import load_dotenv

load_dotenv()

# %% [markdown]
# ## Gliederung:
# 
# - Task1: Extraktion der Website Daten und speichern als csv je Shop
# - Task2: Transformieren der Daten und Mergen zu einem Dataframe udn abspeichern als csv
# - Task3: Laden des Dataframes in die MySQL-Datenbank

# %% [markdown]
# ### Task1: Extraktion der Website-Daten

# %%
# Funktion zum Iterieren über die einzelnen Pages einer Website
def IteratePages(total_url:object):
    total_df = pd.DataFrame()
    page = 1
    while True:
        page_url = total_url + f'?page={page}'

        if page % 10 == 0:
            sleep(10)

        df = pd.read_json(page_url)

        if df.shape == (0, 1):
            break

        df = pd.json_normalize(df['products'])
        total_df = pd.concat([total_df, df], ignore_index=True)

        page += 1
    
    return total_df

# %%
# Iterieren über die einzelnen Websites und umwandeln der Daten in Dataframes
def ScrapeURL(base_url:object):
    total_url = base_url + '/collections/all/products.json'
    
    if (requests.get(total_url)).status_code != 200:
        print(f'Fehler beim Laden der Website: {base_url}')
        # break

    df = IteratePages(total_url)

    images_data = []
    for row in df['images']:
        images_data.extend(row)
    df_image = pd.DataFrame(images_data)

    variants_data = []
    for row in df['variants']:
        variants_data.extend(row)
    df_variant = pd.DataFrame(variants_data)

    df_product = df[['id', 'title', 'handle', 'body_html', 'published_at', 'created_at', 'updated_at', 'vendor', 'product_type', 'tags']]
    
    return df_product, df_variant, df_image

# %%
# Hilfstabllen werden geladen
def LoadHelperSheets():
    # Tabelle mit den Shopnamen und URL-Daten
    df_brands = pd.read_csv('Hilfstabellen/Brand_URL.csv', delimiter=';')

    # Tabellen zum mapping von Prdukttyp und Farben
    df_map_product_type = pd.read_csv(f'Hilfstabellen/Product_type.csv', delimiter=';') 
    df_map_color = pd.read_csv(f'Hilfstabellen/color.csv', delimiter=';')

    return df_brands, df_map_product_type, df_map_color

# %%
# Laden der Hilfstabellen, speichern der Produktdaten je Unternehmen als parquet
def IterateAllShops():
    df_brands = LoadHelperSheets()[0]

    for index, row in df_brands.iterrows():
        company = row['company']
        url = row['url']

        df_product, df_variant, df_image = ScrapeURL(url)

        df_product.to_parquet(f'Marken/product/product_{company}.parquet', engine='pyarrow')
        df_variant.to_parquet(f'Marken/variant/variant_{company}.parquet', engine='pyarrow')
        df_image.to_parquet(f'Marken/image/image_{company}.parquet', engine='pyarrow')
        print(f'Marke: {company}, Product: {df_product.shape[0]}, , Variant: {df_variant.shape[0]} , Image: {df_image.shape[0]}')

    return print('Task 1 erfolgreich ausgeführt!')

# %% [markdown]
# ### Task2: Transformieren der Daten und Mergen

# %%
# Html-Inhalt Lesbar-machen
def extract_and_clean_text(html):
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator='\n')
    return ' '.join(text.split())

# %%
# Transformation der Product-Tabelle
def TransformProduct(df_product:pd.DataFrame,shopname:object,df_map_product_type):
    df_product['body_html'] = df_product['body_html'].fillna('')
    df_product.loc[:, 'body_html'] = df_product['body_html'].astype(str).apply(extract_and_clean_text)
    
    df_product['shopname'] = shopname
    df_product['online'] = 1
    df_product = df_product.merge(df_map_product_type, on='product_type', how='left')
    df_product = df_product.rename(columns={'map': 'category'})

    # Konvertiere jede Liste in 'tags' zu einem kommagetrennten String
    df_product['tags'] = df_product['tags'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)

    df_product['published_at'] = pd.to_datetime(df_product['published_at'], utc=True, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df_product['created_at'] = pd.to_datetime(df_product['created_at'], utc=True, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df_product['updated_at'] = pd.to_datetime(df_product['updated_at'], utc=True, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    df_product = df_product.where(pd.notnull(df_product), None)

    return df_product

# %%
# Transformation der Variant-Tabelle
def TransformVariant(df_variant:pd.DataFrame, df_map_color):
    df_variant[['size', 'color','main_color']] = None

    for i in range(1,4):
        df_variant[f'option{i}'] = df_variant[f'option{i}'].astype(str)
        top_values = df_variant[f'option{i}'].str.upper().value_counts().head(4).index.to_list()
        map_colors = df_map_color['color'].str.upper().to_list() 
        map_size = ['XXS','XS','S','M','L','XL','XXL','ONESIZE','ONE SIZE','30','32','34','36']

        if all(color in map_colors for color in top_values) == True and df_variant[f'option{i}'].isna().all() == False:
            df_variant['color'] = df_variant[f'option{i}'].str.upper()
            
        elif all(size in map_size for size in top_values) == True and df_variant[f'option{i}'].isna().all() == False:
            df_variant['size'] = df_variant[f'option{i}']
            
        
    if ~df_variant['color'].isna().all():
        df_variant = df_variant.merge(df_map_color, on='color', how='left')
        df_variant['main_color'] = df_variant['map']
        df_variant = df_variant.drop('map', axis=1)


    df_variant['price'] = pd.to_numeric(df_variant['price'], errors='coerce')
    df_variant['compare_at_price'] = pd.to_numeric(df_variant['compare_at_price'], errors='coerce')

    df_variant['sale'] = np.where(df_variant['price'] < df_variant['compare_at_price'], 1, 0)

    df_variant['discount'] = np.where(df_variant['sale'] == 1,
                          ((df_variant['compare_at_price'] - df_variant['price']) / df_variant['compare_at_price']),
                          0)

    df_variant['compare_at_price'] = df_variant['compare_at_price'].fillna(df_variant['price'])

    df_variant['main_color'] = df_variant['main_color'].replace({np.nan: None})
    
    df_variant['featured_image'] = df_variant['featured_image'].apply(lambda x: json.dumps(x.tolist()) if isinstance(x, np.ndarray) else json.dumps(x))

    df_variant['created_at'] = pd.to_datetime(df_variant['created_at'], utc=True, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df_variant['updated_at'] = pd.to_datetime(df_variant['updated_at'], utc=True, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    df_variant = df_variant.drop_duplicates()

    return df_variant

# %%
# Transformation der Image-Tabelle
def TransformImage(df_image:pd.DataFrame):

    df_image['created_at'] = pd.to_datetime(df_image['created_at'], utc=True, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df_image['updated_at'] = pd.to_datetime(df_image['updated_at'], utc=True, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    def safe_join(x):
        if isinstance(x, list):  # Prüfe, ob x eine Liste ist
            return ','.join(map(str, x))
        return ''

    df_image['variant_ids'] = df_image['variant_ids'].apply(safe_join)

    return df_image

# %%
# Transformieren, zusammenfügen und speichern der product, variant und image-Tabelle
def IterateAllCsv():
    df_brands, df_map_product_type, df_map_color = LoadHelperSheets()

    df_product_main = pd.DataFrame()
    df_variant_main = pd.DataFrame()
    df_image_main = pd.DataFrame()

    for index, row in df_brands.iterrows():
        company = row['company']
        url = row['url']

        df_product=pd.read_parquet(f'Marken/product/product_{company}.parquet', engine='pyarrow')
        df_variant=pd.read_parquet(f'Marken/variant/variant_{company}.parquet', engine='pyarrow')
        df_image=pd.read_parquet(f'Marken/image/image_{company}.parquet', engine='pyarrow')
        
        df_product = TransformProduct(df_product, company, df_map_product_type)
        df_variant = TransformVariant(df_variant, df_map_color)
        df_image = TransformImage(df_image)

        try:
            df_product_main = pd.concat([df_product_main, df_product], ignore_index=True)
            print(f'Shop: {company}             Anzahl Artikel: {df_product.shape[0]}')
        except Exception as e:
            print(f"Fehler beim Laden der Parquet-Daten für {company}: {e}")
            break
        df_variant_main = pd.concat([df_variant_main, df_variant], ignore_index=True)
        df_image_main = pd.concat([df_image_main, df_image], ignore_index=True)

    df_product_main.to_parquet(f'Marken/Transformed_Data/product.parquet', engine='pyarrow')
    df_variant_main.to_parquet(f'Marken/Transformed_Data/variant.parquet', engine='pyarrow')
    df_image_main.to_parquet(f'Marken/Transformed_Data/image.parquet', engine='pyarrow')

    return print('Task2 erfolgreich ausgeführt!')

# %% [markdown]
# ### Task3: Laden der transformierten, zusammengefügten Daten in MySQL Datenbank

# %%
# Erstellen einer Verbindung zur MySQL-Datenbank
def CreateConnection():
    MYSQL_ACCESS = os.getenv("MYSQL_ACCESS")    # Log-In Daten sollten in einer .env-Datei hinterlegt werden
    engine = create_engine(MYSQL_ACCESS)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    return engine,metadata
# %%
# Laden der Daten in die MySQL Datenbank
def AddToSQL():
    df_product= pd.read_parquet(f'Marken/Transformed_Data/product.parquet', engine='pyarrow')
    df_variant = pd.read_parquet(f'Marken/Transformed_Data/variant.parquet', engine='pyarrow')
    df_image = pd.read_parquet(f'Marken/Transformed_Data/image.parquet', engine='pyarrow')

    engine,metadata = CreateConnection()
    Session = sessionmaker(bind=engine)
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    def update_or_insert(df, table_name, special_condition=False):
        session = Session()
        table = Base.classes[table_name]

        try:
            # Bestehende IDs in der Datenbank ermitteln
            existing_ids = [id_[0] for id_ in session.query(table.id).all()]

            # Trenne den DataFrame in Zeilen zum Einfügen und Aktualisieren
            df_to_insert = df[~df['id'].isin(existing_ids)]
            df_to_update = df[df['id'].isin(existing_ids)]

            # Einfügen der neuen Zeilen
            if not df_to_insert.empty:
                session.bulk_insert_mappings(table, df_to_insert.to_dict(orient="records"))

            # Aktualisiere bestehende Zeilen
            if not df_to_update.empty:
                for index, row in df_to_update.iterrows():
                    stmt = update(table).where(table.id == row['id']).values(**row.to_dict())
                    session.execute(stmt)

            # Spezielle Bedingung, um Spalte `online` auf 0 zu setzen
            if special_condition and table_name == 'product':
                all_ids = df['id'].tolist()
                stmt = update(table).where(~table.id.in_(all_ids)).values(online=0)
                session.execute(stmt)

            session.commit()
            print(f"{table_name}-Daten erfolgreich aktualisiert und eingefügt.")

        except SQLAlchemyError as e:
            session.rollback()
            print(f"Ein Fehler ist aufgetreten bei der Tabelle {table_name}: {e}")

        finally:
            session.close()

    try:
        update_or_insert(df_product, 'product', special_condition=True)
        update_or_insert(df_variant, 'variant')
        update_or_insert(df_image, 'image')
        return print('Task3 erfolgreich ausgeführt!')

    except SQLAlchemyError as e:
        print(f"Ein Fehler ist aufgetreten: {e}")
