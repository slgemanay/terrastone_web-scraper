import pandas as pd
from io import BytesIO

# Установка библиотеки boto3, если она не установлена
try:
    import boto3
except ImportError:
    import os
    os.system('pip install boto3')
    import boto3

def upload_to_vk_cloud(image_data, bucket_name, object_name, access_key, secret_key, endpoint_url, overwrite=True):
    """
    Загружает файл изображения в VK Cloud.
    :param image_data: Данные изображения в формате bytes
    :param bucket_name: Название хранилища
    :param object_name: Путь и имя файла в хранилище
    :param access_key: Ключ доступа VK Cloud
    :param secret_key: Секретный ключ VK Cloud
    :param endpoint_url: URL конечной точки S3
    :param overwrite: Флаг для перезаписи файла
    :return: URL загруженного файла
    """
    try:
        # Настройка клиента S3 для VK Cloud
        session = boto3.session.Session()
        client = session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        if not overwrite:
            # Проверка, существует ли файл
            try:
                client.head_object(Bucket=bucket_name, Key=object_name)
                print(f"Файл {object_name} уже существует, пропуск загрузки.")
                return f"{endpoint_url}/{bucket_name}/{object_name}"
            except client.exceptions.ClientError:
                pass

        # Создание BytesIO из данных изображения
        image_file = BytesIO(image_data)
        client.upload_fileobj(image_file, bucket_name, object_name)
        return f"{endpoint_url}/{bucket_name}/{object_name}"
    except Exception as e:
        print(f"Ошибка загрузки изображения: {e}")
        return None

def add_image_links_to_dataframe(dataframe, bucket_name, access_key, secret_key, endpoint_url):
    """
    Загрузка изображений в VK Cloud и добавление ссылок в DataFrame.
    :param dataframe: Pandas DataFrame с колонкой "Файл изображения"
    :param bucket_name: Название хранилища
    :param access_key: Ключ доступа VK Cloud
    :param secret_key: Секретный ключ VK Cloud
    :param endpoint_url: URL конечной точки S3
    :return: Обновленный DataFrame
    """
    image_links = []
    errors = 0
    uploaded_count = 0

    if 'Файл изображения' not in dataframe.columns:
        raise KeyError("Колонка 'Файл изображения' отсутствует в DataFrame")

    total_rows = len(dataframe)
    for index, row in dataframe.iterrows():
        try:
            # Прерывание цикла, если загруженных строк >= общего количества строк
            if uploaded_count >= total_rows:
                print("Все строки успешно обработаны. Прерывание цикла.")
                break

            # Пропуск строк, где ссылка на изображение уже существует
            if 'Ссылка на изображение' in dataframe.columns and pd.notna(row.get('Ссылка на изображение', None)):
                image_links.append(row['Ссылка на изображение'])
                uploaded_count += 1  # Увеличиваем счетчик даже при пропуске
                print(f"[{index + 1}/{total_rows}] Ссылка уже существует, пропуск: {row['Ссылка на изображение']} | Успешно: {uploaded_count} | Ошибки: {errors}")
                continue

            if row['Файл изображения']:
                # Генерируем уникальное имя файла на основе индекса
                object_name = f"images/{index + 1}.jpg"
                
                # Получаем данные изображения в формате bytes
                image_data = row['Файл изображения'].getvalue()

                # Реализация повторных попыток загрузки
                attempts = 3
                image_url = None
                for attempt in range(attempts):
                    try:
                        image_url = upload_to_vk_cloud(
                            image_data, bucket_name, object_name, access_key, secret_key, endpoint_url
                        )
                        if image_url:
                            break
                    except Exception as e:
                        print(f"Попытка {attempt + 1} не удалась для строки {index + 1}: {e}")

                if image_url:
                    image_links.append(image_url)
                    uploaded_count += 1
                    print(f"[{index + 1}/{total_rows}] Успешно загружено: {image_url} | Успешно: {uploaded_count} | Ошибки: {errors}")
                else:
                    errors += 1
                    uploaded_count += 1
                    print(f"[{index + 1}/{total_rows}] Ошибка загрузки после {attempts} попыток. | Успешно: {uploaded_count} | Ошибки: {errors}")
                    image_links.append(None)
            else:
                image_links.append(None)
                uploaded_count += 1  # Увеличиваем счетчик при пустых данных

            # Дополнительное завершение цикла, если загружено больше, чем строк в DataFrame
            if uploaded_count > total_rows:
                print("Все строки успешно загружены. Прерывание цикла.")
                break

        except Exception as e:
            errors += 1
            print(f"[{index + 1}/{total_rows}] Ошибка: {e} | Успешно: {uploaded_count} | Ошибки: {errors}")
            image_links.append(None)

    # Добавляем ссылки на изображения в DataFrame
    dataframe['Ссылка на изображение'] = image_links

    # Удаляем бинарные данные изображений, чтобы сэкономить память
    dataframe.drop(columns=['Файл изображения'], inplace=True)

    print(f"Загрузка завершена. Всего строк: {total_rows} | Успешно: {uploaded_count} | Ошибки: {errors}")
    return dataframe

def set_dataframe_types(dataframe):
    """
    Устанавливает явные типы данных для всех столбцов DataFrame.
    :param dataframe: Pandas DataFrame
    :return: DataFrame с установленными типами данных
    """
    types_mapping = {
        'Дата получения': 'datetime64[ns]',
        'Название': 'string',
        'Тип камня': 'string',
        'Цвет камня': 'string',
        'Наличие (㎡)': 'float64',
        'Толщина, (мм)': 'float64',
        'Отклонение толщины, (мм)': 'float64',
        'Цена (USD/㎡)': 'float64',
        'Ссылка на карточку': 'string',
        'URL картинки': 'string',
        'Ссылка на изображение': 'string'
    }

    for column, dtype in types_mapping.items():
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].astype(dtype)

    return dataframe

# Настройки VK Cloud
bucket_name = "terrastone"
access_key = "uxiYrY2B21agARYLqdHxME"
secret_key = "bFRbixZScEUb2LSnhcoyLZDhFjFbBLBM5wQn9hHsQzQD"
endpoint_url = "https://hb.bizmrg.com"

# Проверяем наличие final_dataframe и его правильную структуру
if 'final_dataframe' not in locals():
    raise NameError("Переменная 'final_dataframe' не определена")

# Добавление ссылок на изображения в DataFrame
final_dataframe = add_image_links_to_dataframe(final_dataframe, bucket_name, access_key, secret_key, endpoint_url)

# Установка явных типов данных
final_dataframe = set_dataframe_types(final_dataframe)

# Сохранение обновленного DataFrame в CSV
final_dataframe.to_csv("processed_data.csv", index=False)
print("Данные успешно обработаны и сохранены в processed_data.csv")