import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd

# Настройки подключения к базе данных
DB_CONFIG = {
    'dbname': 'stone_database',
    'user': 'terrastone_admin',
    'password': 'aGJ7yKtuG3NlKiULyKYGy3GUBLgQz4Yj',
    'host': 'dpg-cths8vlumphs73flnarg-a.frankfurt-postgres.render.com',
    'port': '5432'
}

def load_data_to_postgresql(dataframe, table_name):
    """
    Загружает данные из DataFrame в таблицу PostgreSQL.
    Если запись уже существует, обновляет её на более поздний вариант.
    :param dataframe: Pandas DataFrame для загрузки
    :param table_name: Имя таблицы в базе данных
    """
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Подготовка данных для вставки
        columns = list(dataframe.columns)
        values = [tuple(x) for x in dataframe.to_numpy()]
        update_query = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'ID'])
        insert_query = f"""
        INSERT INTO public.{table_name} ({', '.join(columns)}) 
        VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT (ID) DO UPDATE SET {update_query};
        """

        # Выполнение вставки данных
        execute_batch(cursor, insert_query, values)
        connection.commit()

        print(f"Данные успешно загружены в таблицу {table_name}")
    except Exception as e:
        print(f"Ошибка загрузки в таблицу {table_name}: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    # Загрузка CSV-файлов
    images_table = pd.read_csv("images_table.csv")
    main_table = pd.read_csv("main_table.csv")
    details_table = pd.read_csv("details_table.csv")

    # Переименование столбцов для соответствия структуре базы данных
    images_table.rename(columns={"URL картинки":"image_url", "Ссылка на изображение": "image_link"}, inplace=True)
    main_table.rename(columns={
        "Название": "name",
        "Тип камня": "stone_type",
        "Цвет камня": "color",
        "Цена (USD/㎡)": "price",
        "Ссылка на карточку": "card_url",
        "Сайт": "site_name",
        "Дата получения": "date_received"
    }, inplace=True)
    details_table.rename(columns={
        "Наличие (㎡)": "availability",
        "Толщина, (мм)": "thickness",
        "Отклонение толщины, (мм)": "thickness_deviation"
    }, inplace=True)

    # Проверка названий столбцов перед загрузкой
    print("Проверка названий столбцов:")
    print("Images table columns:", images_table.columns.tolist())
    print("Main table columns:", main_table.columns.tolist())
    print("Details table columns:", details_table.columns.tolist())

    # Загрузка в таблицы PostgreSQL
    load_data_to_postgresql(images_table, "images")
    load_data_to_postgresql(main_table, "main")
    load_data_to_postgresql(details_table, "details")

    print("Все данные успешно загружены в базу данных.")