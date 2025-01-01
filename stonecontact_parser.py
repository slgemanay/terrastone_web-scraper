import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random
from io import BytesIO
from PIL import Image
from collections import Counter
import logging

# Настройка логирования
logging.basicConfig(
    filename="image_color_analysis.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Функция для определения типа камня
def determine_stone_type(name):
    stone_types = {
        "Marble": "Мрамор",
        "Granite": "Гранит",
        "Onyx": "Оникс",
        "Quartzite": "Кварцит",
        "Sodalite": "Содалит",
        "Macaubas": "Макаубас",
        # Добавьте другие типы камней, если нужно
    }
    for key, value in stone_types.items():
        if key in name:
            return value
    return ""

# Функция для определения цвета камня по названию
def determine_stone_color(name):
    colors = {
        "White": "Белый",
        "Blue": "Синий",
        "Grey": "Серый",
        "Beige": "Бежевый",
        "Green": "Зеленый",
        "Multicolor": "Многоцветный",
        "Gold": "Золотой",
        "Brown": "Коричневый",
        "Silver": "Серебряный",
        "Purple": "Фиолетовый",
        "Black": "Черный",
        "Semi White": "Полубелый",
        "Bordeaux": "Бордовый",
        "Red": "Красный",
        "Lilac": "Лиловый",
        "Pink": "Розовый"
    }
    for key, value in colors.items():
        if key in name:
            return value
    return ""

# Функция для загрузки изображения
def download_image(image_url):
    try:
        response = requests.get(image_url, timeout=10)
        if response.status_code == 200:
            return BytesIO(response.content)
    except requests.exceptions.RequestException:
        return None

# Функция для определения доминирующего цвета изображения
def get_dominant_color(image_file):
    try:
        image = Image.open(image_file)
        image = image.resize((50, 50))  # Уменьшаем размер для упрощения обработки
        pixels = list(image.getdata())
        most_common = Counter(pixels).most_common(1)
        return most_common[0][0] if most_common else None
    except Exception:
        return None

# Сопоставление цвета с русским названием
def map_color_to_name(rgb):
    color_mapping = {
        "Белый": [(240, 240, 240), (255, 255, 255)],
        "Синий": [(0, 0, 255), (70, 130, 180)],
        "Серый": [(128, 128, 128), (169, 169, 169)],
        "Бежевый": [(245, 245, 220), (222, 184, 135)],
        "Зеленый": [(0, 128, 0), (34, 139, 34)],
        "Многоцветный": [(255, 0, 255)],
        "Золотой": [(255, 215, 0), (255, 223, 0)],
        "Коричневый": [(139, 69, 19), (160, 82, 45)],
        "Серебряный": [(192, 192, 192)],
        "Фиолетовый": [(128, 0, 128), (75, 0, 130)],
        "Черный": [(0, 0, 0)],
        "Полубелый": [(245, 245, 245)],
        "Бордовый": [(128, 0, 0)],
        "Красный": [(255, 0, 0)],
        "Лиловый": [(221, 160, 221)],
        "Розовый": [(255, 182, 193)]
    }
    closest_color = None
    min_distance = float('inf')
    for name, rgb_values in color_mapping.items():
        for rgb_value in rgb_values:
            distance = sum((rgb[i] - rgb_value[i]) ** 2 for i in range(3))  # Евклидово расстояние
            if distance < min_distance:
                min_distance = distance
                closest_color = name
    return closest_color if closest_color else ""

# Функция для получения названия сайта
def get_site_name(link):
    try:
        if link:
            start = link.find("www.") + 4
            end = link.find(".com")
            return link[start:end] if start != -1 and end != -1 else ""
    except Exception:
        return ""

# Функция для преобразования цены в числовое значение
def clean_price(price):
    try:
        return float(''.join([c for c in price if c.isdigit() or c == '.']))
    except ValueError:
        return None

# Функция для обработки значения наличия
def clean_stock(stock):
    try:
        return float(''.join([c for c in stock if c.isdigit() or c == '.']))
    except ValueError:
        return None

# Функция для разделения толщина и отклонения
def split_thickness(thickness):
    try:
        parts = thickness.split("(")
        base_thickness = float(parts[0].strip().replace("mm", "")) if parts[0] else None
        deviation = float(parts[1].strip().replace("mm)", "").replace("±", "")) if len(parts) > 1 else None
        return base_thickness, deviation
    except Exception:
        return None, None

# Функция для сбора данных
def scrape_stonecontact_to_dataframe(base_url):
    all_data = []  # Для хранения данных

    try:
        # Настройка заголовков для запроса
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": base_url
        }

        page = 1  # Начинаем с первой страницы
        prev_product_count = 0  # Количество товаров на предыдущей странице

        while True:
            url = f"{base_url}/{page}" if page > 1 else base_url

            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code != 200:
                    break
            except requests.exceptions.RequestException as e:
                break

            # Парсинг HTML
            soup = BeautifulSoup(response.content, "html.parser")
            product_list = soup.find("ul", class_="productlist")
            if not product_list:
                break

            products = product_list.find_all("li")

            # Проверка количества товаров на текущей странице
            current_product_count = len(products)
            if current_product_count == 0:
                break

            # Сбор данных
            for product in products:
                try:
                    name_tag = product.find("div", class_="pname")
                    name = name_tag.get_text(strip=True) if name_tag else ""

                    thickness_tag = product.find("p", class_="thickness")
                    thickness = thickness_tag.get_text(strip=True).replace("Thickness：", "") if thickness_tag else ""

                    price_tag = product.find("span", class_="price")
                    price_raw = price_tag.get_text(strip=True) if price_tag else ""

                    stock_tag = product.find("p", class_="size")
                    stock = stock_tag.get_text(strip=True) if stock_tag else ""

                    link_tag = product.find("a", title=True)
                    link = f"https://www.stonecontact.com{link_tag['href']}" if link_tag and "href" in link_tag.attrs else ""

                    site_name = get_site_name(link)

                    imgbox_tag = product.find("div", class_="imgbox")
                    image_tag = imgbox_tag.find("img") if imgbox_tag else None
                    image_url = image_tag["data-echo"] if image_tag and "data-echo" in image_tag.attrs else ""

                    image_file = download_image(image_url) if image_url else None

                    # Добавление данных в общий список
                    all_data.append([site_name, name, stock, thickness, price_raw, link, image_url, image_file])

                except Exception as e:
                    continue

            # Проверка на завершение парсинга
            if current_product_count != prev_product_count and page > 1:
                break

            prev_product_count = current_product_count
            page += 1

    except Exception as e:
        print(f"Ошибка: {e}")

    # Формирование сырого DataFrame
    dataframe = pd.DataFrame(all_data, columns=[
        "Сайт", "Название", "Наличие (㎡)", "Толщина, (мм)", "Цена (USD/㎡)", "Ссылка на карточку", "URL картинки", "Файл изображения"
    ])

    # Удаление дубликатов
    dataframe = dataframe.drop_duplicates(subset=["Ссылка на карточку"], keep="first")

    # Применение функций к DataFrame
    dataframe["Тип камня"] = dataframe["Название"].apply(determine_stone_type)
    dataframe["Цвет камня"] = dataframe["Название"].apply(determine_stone_color)

    # Дополнение цветов через анализ изображений
    for index, row in dataframe.iterrows():
        if not row["Цвет камня"] and row["Файл изображения"]:
            logging.info(f"Начинается анализ цвета для строки {index}.")
            dominant_color = get_dominant_color(row["Файл изображения"])
            if dominant_color:
                mapped_color = map_color_to_name(dominant_color)
                if mapped_color:
                    dataframe.at[index, "Цвет камня"] = mapped_color
                    logging.info(f"Для строки {index} определен цвет: {mapped_color}.")
                else:
                    logging.warning(f"Цвет {dominant_color} не сопоставлен для строки {index}.")
            else:
                logging.error(f"Не удалось определить доминирующий цвет для строки {index}.")

    dataframe["Цена (USD/㎡)"] = dataframe["Цена (USD/㎡)"].apply(clean_price)
    dataframe["Наличие (㎡)"] = dataframe["Наличие (㎡)"].apply(clean_stock)
    dataframe[["Толщина, (мм)", "Отклонение толщины, (мм)"]] = dataframe["Толщина, (мм)"].apply(
        lambda x: pd.Series(split_thickness(x))
    )

    # Добавление даты получения данных
    date_collected = datetime.now()
    dataframe.insert(0, "Дата получения", pd.to_datetime(date_collected))

    # Расставление столбцов в нужном порядке
    dataframe = dataframe[[
        "Дата получения", "Сайт", "Название", "Тип камня", "Цвет камня", "Наличие (㎡)",
        "Толщина, (мм)", "Отклонение толщины, (мм)", "Цена (USD/㎡)", "Ссылка на карточку", "URL картинки", "Файл изображения"
    ]]

    
    # Указание типов данных для каждого столбца
    dataframe = dataframe.astype({
        "Дата получения": "datetime64[ns]",
        "Сайт": "string",
        "Название": "string",
        "Тип камня": "string",
        "Цвет камня": "string",
        "Наличие (㎡)": "float64",
        "Толщина, (мм)": "float64",
        "Отклонение толщины, (мм)": "float64",
        "Цена (USD/㎡)": "float64",
        "Ссылка на карточку": "string",
        "URL картинки": "string"
    })

    # Преобразование файлов изображений в тип BytesIO
    dataframe["Файл изображения"] = dataframe["Файл изображения"].apply(
        lambda x: BytesIO(x.getvalue()) if isinstance(x, BytesIO) else None
    )
    return dataframe
# Базовый URL для парсинга
base_url = "https://www.stonecontact.com/mart/slab"

# Запуск парсинга и обработки
final_dataframe = scrape_stonecontact_to_dataframe(base_url)