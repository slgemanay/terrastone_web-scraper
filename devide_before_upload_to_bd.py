import pandas as pd
import uuid

def prepare_final_structures(dataframe):
    """
    Разделяет DataFrame на структуры для загрузки в SQL базу данных.
    :param dataframe: Pandas DataFrame
    :return: Словарь с разделенными таблицами
    """
    # Проверка и переименование столбцов
    column_mapping = {
        "URL картинки": "image_url",
        "Ссылка на изображение": "image_link",
        "Название": "name",
        "Тип камня": "stone_type",
        "Цвет камня": "color",
        "Цена (USD/㎡)": "price",
        "Ссылка на карточку": "card_url",
        "Сайт": "site_name",
        "Дата получения": "date_received",
        "Наличие (㎡)": "availability",
        "Толщина, (мм)": "thickness",
        "Отклонение толщины, (мм)": "thickness_deviation"
    }
    dataframe.rename(columns=column_mapping, inplace=True)

    # Добавление уникального ID для каждой записи
    dataframe['ID'] = [str(uuid.uuid4()) for _ in range(len(dataframe))]

    # Таблица изображений
    images_table = dataframe[['ID', 'image_url', 'image_link']].copy()

    # Основная таблица
    main_table = dataframe[[
        'ID', 'name', 'stone_type', 'color', 'price', 
        'card_url', 'site_name', 'date_received'
    ]].copy()

    # Таблица характеристик
    details_table = dataframe[[
        'ID', 'availability', 'thickness', 'thickness_deviation'
    ]].copy()

    return {
        'images': images_table,
        'main': main_table,
        'details': details_table
    }

if __name__ == "__main__":
    # Загрузка данных из processed_data.csv
    try:
        final_dataframe = pd.read_csv("processed_data.csv")
    except FileNotFoundError as e:
        print(f"Ошибка: файл processed_data.csv не найден. {e}")
        exit(1)

    # Разделение DataFrame на таблицы
    try:
        tables = prepare_final_structures(final_dataframe)
    except KeyError as e:
        print(f"Ошибка: отсутствуют столбцы {e}")

    # Сохранение таблиц в CSV для проверки
    for table_name, table_data in tables.items():
        table_data.to_csv(f"{table_name}_table.csv", index=False)
        print(f"Таблица {table_name} сохранена в {table_name}_table.csv")

    print("Данные успешно обработаны и сохранены для загрузки в базу данных.")