import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
import json
import os

# --- 1. Налаштування та Глобальні Змінні ---
# Посилання на вашу Google Таблицю з конфігураціями магазинів
GOOGLE_SHEET_URL = 'https://docs.google.com/spreadsheets/d/16vCLf0Zo04zW50Wn8PlPFdLnzjYOdP4TZaeT4uP2tUc/edit?usp=sharing'

# Шлях до файлу з ТОП-300 альбомами (він має бути в тому ж репозиторії GitHub)
TOP_ALBUMS_CSV_PATH = 'top_albums.csv'

# --- 2. Функції для Взаємодії з Google Sheets та Авторизація ---

@st.cache_resource # Кешуємо об'єкт gc, щоб він створювався лише один раз
def authorize_gspread():
    """Авторизує gspread для доступу до Google Таблиць, використовуючи st.secrets."""
    try:
        gc = gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        st.success("Авторизація Google Таблиць успішна (через st.secrets).")
        return gc
    except KeyError as e:
        st.error(f"ПОМИЛКА АВТОРИЗАЦІЇ: Відсутній секрет 'gcp_service_account'.")
        st.info("Переконайтесь, що ви правильно налаштували секрети Google Cloud у Streamlit Cloud. "
                "Всі поля JSON-ключа мають бути вкладені під 'gcp_service_account', як показано в документації Streamlit.")
        st.stop()
    except Exception as e:
        st.error(f"ПОМИЛКА АВТОРИЗАЦІЇ Google Таблиць: {e}")
        st.info("Переконайтесь, що ви надали дозволи таблиці (доступ для service account) або коректно налаштували секрети.")
        st.stop()

gc = authorize_gspread() # Авторизуємо gspread при запуску додатка

@st.cache_data(ttl=3600, show_spinner="Завантажуємо конфігурацію магазинів...") # Кешуємо дані на 1 годину
def get_site_configs_from_sheet(sheet_url):
    """
    Отримує конфігурацію магазинів з Google Таблиці,
    роблячи заголовки колонок більш стійкими та обробляючи можливі помилки даних.
    """
    if not gc:
        st.error("Gspread не авторизовано. Неможливо завантажити конфігурацію.")
        return []
    
    try:
        spreadsheet = gc.open_by_url(sheet_url)
        worksheet = spreadsheet.sheet1
        
        # Отримуємо всі дані як список списків (без автоматичного визначення заголовків)
        all_data = worksheet.get_all_values()
        
        if not all_data:
            st.warning("Google Таблиця конфігурації порожня.")
            return []

        # Визначаємо заголовки з першого рядка
        headers = [header.strip() for header in all_data[0]]
        
        # Нормалізуємо заголовки для стійкості (нижній регістр, без пробілів)
        normalized_headers_map = {
            "name": "Name", "baseurl": "BaseURL", "paginationparam": "PaginationParam",
            "startpage": "StartPage", "endpage": "EndPage", "productcontainer": "ProductContainer",
            "titleelement": "TitleElement", "priceelement": "PriceElement",
            "linkelement": "LinkElement", "artistelement": "ArtistElement"
        }
        
        # Створюємо словник для швидкого доступу до нормалізованих заголовків
        header_to_col_index = {headers[i].strip().lower(): i for i in range(len(headers))}

        missing_headers = []
        for expected_lower, original_case_name in normalized_headers_map.items():
            if expected_lower not in header_to_col_index:
                missing_headers.append(original_case_name)
        
        if missing_headers:
            st.error(f"ПОМИЛКА: У Google Таблиці відсутні наступні необхідні колонки: {', '.join(missing_headers)}.")
            st.info("Переконайтесь, що заголовки колонок написані точно так, як потрібно (регістр не важливий, але назва має співпадати).")
            return []

        site_configs = []
        # Обробляємо кожен рядок даних, починаючи з другого (після заголовків)
        for row_index, row_data in enumerate(all_data[1:]):
            if not any(row_data): # Пропускаємо порожні рядки
                continue

            # Створюємо словник для поточного запису
            record = {}
            for header_name, col_index in header_to_col_index.items():
                if col_index < len(row_data): # Перевіряємо, чи є дані в цій колонці для поточного рядка
                    record[header_name] = row_data[col_index]
                else:
                    record[header_name] = "" # Якщо даних немає, ставимо порожній рядок

            try:
                config = {
                    'name': record.get('name'),
                    'base_url': record.get('baseurl'),
                    'pagination_param': record.get('paginationparam'),
                    'start_page': int(record.get('startpage', 1)),
                    'end_page': int(record.get('endpage', 1)),
                    'selectors': {
                        'product_container': record.get('productcontainer'),
                        'title_element': record.get('titleelement'),
                        'price_element': record.get('priceelement'),
                        'link_element': record.get('linkelement'),
                        'ArtistElement': record.get('artistelement')
                    }
                }
                # Перевірка на пусті значення для обов'язкових полів
                if all(config[key] for key in ['name', 'base_url']) and \
                   all(config['selectors'][key] for key in ['product_container', 'title_element', 'price_element', 'link_element']):
                    site_configs.append(config)
                else:
                    st.warning(f"Пропущено сайт у конфігурації (рядок {row_index+2}) через незаповнені обов'язкові поля: {record.get('name', 'N/A')}. Всі поля: {record}")
            except ValueError as ve:
                st.error(f"ПОМИЛКА: Некоректні дані в рядку {row_index+2} таблиці. Очікувалося число, але знайдено інше значення. Деталі: {ve}. Рядок: {record}")
            except Exception as ex:
                st.error(f"ПОМИЛКА: Неочікувана проблема з рядком {row_index+2} таблиці. Деталі: {ex}. Рядок: {record}")
        
        return site_configs
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"ПОМИЛКА: Таблицю за посиланням '{sheet_url}' не знайдено.")
        st.info("Переконайтесь, що посилання правильне і таблиця має потрібні дозволи ('Будь-хто, хто має посилання' -> 'Читач').")
        return []
    except Exception as e:
        st.error(f"ПОМИЛКА при читанні конфігурації з Google Таблиці: {e}")
        st.info("Схоже, є проблема з доступом до таблиці або її структурою. Переконайтесь, що перший рядок містить коректні заголовки.")
        st.error(f"Деталі помилки: {e}")
        return []

# --- 3. Функції для Скрейпінгу ---

@st.cache_data(ttl=3600, show_spinner="Виконуємо скрейпінг...") # Кешуємо дані та показуємо спінер
def scrape_single_site(site_config):
    """
    Збирає інформацію про вінілові платівки з одного сайту за його конфігурацією.
    """
    site_name = site_config['name']
    base_url = site_config['base_url']
    pagination_param = site_config['pagination_param']
    start_page = site_config['start_page']
    end_page = site_config['end_page']
    selectors = site_config['selectors']

    # Перевіряємо, чи селектори не порожні, перш ніж їх розділяти
    product_container_selectors = [s.strip() for s in selectors.get('ProductContainer', '').split(',') if s.strip()]
    title_element_selectors = [s.strip() for s in selectors.get('TitleElement', '').split(',') if s.strip()]
    price_element_selectors = [s.strip() for s in selectors.get('PriceElement', '').split(',') if s.strip()]
    link_element_selectors = [s.strip() for s in selectors.get('LinkElement', '').split(',') if s.strip()]
    artist_element_selectors = [s.strip() for s in selectors.get('ArtistElement', '').split(',') if s.strip()]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    site_vinyl_records = []

    st.write(f"--- Починаємо скрейпінг для: **{site_name}** ---")
    st.info(f"Буде зіскановано сторінок: {end_page - start_page + 1}")

    for page_num in range(start_page, end_page + 1):
        if page_num == 1:
            url = base_url
        elif pagination_param:
            url = f"{base_url}?{pagination_param}={page_num}"
        else:
            st.warning(f"Не вказано параметр пагінації для {site_name}. Скрейпінг буде лише для першої сторінки.")
            if page_num > 1: # Щоб уникнути нескінченного циклу, якщо немає пагінації
                break
                
        st.text(f"  Завантаження сторінки {page_num}: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            st.error(f"  Помилка при завантаженні сторінки {url}: {e}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')

        products = []
        for selector in product_container_selectors:
            found_products = soup.select(selector)
            if found_products:
                products.extend(found_products)
                break

        if not products:
            st.warning(f"  На сторінці {page_num} не знайдено товарів за жодним із селекторів '{selectors.get('product_container', 'N/A')}'.")
            if page_num > start_page:
                st.info(f"  Ймовірно, досягнуто кінця пагінації для {site_name}.")
                break
            continue

        for product in products:
            artist_name = ""
            album_name = "Невідомо"
            price_new = "Невідомо"
            product_link = ""

            if artist_element_selectors:
                for selector in artist_element_selectors:
                    artist_tag = product.select_one(selector)
                    if artist_tag:
                        artist_name = artist_tag.get_text(strip=True).replace('\xa0', ' ').strip()
                        break

            if not artist_name: # Якщо артиста не знайшли окремим селектором, спробуємо витягти з назви альбому
                for selector in title_element_selectors:
                    title_tag = product.select_one(selector)
                    if title_tag:
                        full_title = title_tag.get_text(strip=True).replace('\xa0', ' ').strip()
                        if ' - ' in full_title:
                            parts = full_title.split(' - ', 1)
                            artist_name = parts[0].strip()
                            album_name = parts[1].strip()
                        else:
                            album_name = full_title
                        break
            else: # Якщо артиста знайшли, то шукаємо тільки назву альбому
                for selector in title_element_selectors:
                    title_tag = product.select_one(selector)
                    if title_tag:
                        album_name = title_tag.get_text(strip=True).replace('\xa0', ' ').strip()
                        if artist_name and album_name.startswith(artist_name + ' - '): # Видаляємо артиста з початку назви альбому, якщо він там дублюється
                            album_name = album_name[len(artist_name + ' - '):].strip()
                        break


            for selector in price_element_selectors:
                price_tag = product.select_one(selector)
                if price_tag:
                    price_new = price_tag.get_text(strip=True)
                    break

            for selector in link_element_selectors:
                link_tag = product.select_one(selector)
                if link_tag and link_tag.get('href'):
                    product_link = requests.compat.urljoin(base_url, link_tag.get('href'))
                    break

            site_vinyl_records.append({
                'Магазин': site_name,
                'Гурт/Співак': artist_name,
                'Назва Альбому': album_name,
                'Ціна (Знижка)': price_new,
                'Посилання': product_link
            })

    st.success(f"--- Скрейпінг **{site_name}** завершено. Знайдено записів: **{len(site_vinyl_records)}** ---")
    return pd.DataFrame(site_vinyl_records)


@st.cache_data(ttl=3600, show_spinner="Порівнюємо з ТОП-300...") # Кешуємо та показуємо спінер
def recommend_vinyls(discount_df, top_df):
    """
    Порівнює вініли зі знижками з топом альбомів і видає рекомендації.
    """
    if discount_df.empty or top_df.empty:
        return pd.DataFrame()

    discount_df['Гурт/Співак_norm'] = discount_df['Гурт/Співак'].astype(str).str.lower().str.strip()
    discount_df['Назва Альбому_norm'] = discount_df['Назва Альбому'].astype(str).str.lower().str.strip()
    top_df['Гурт/Співак_norm'] = top_df['Гурт/Співак'].astype(str).str.lower().str.strip()
    top_df['Назва Альбому_norm'] = top_df['Назва Альбому'].astype(str).str.lower().str.strip()

    recommended_df = pd.merge(
        discount_df,
        top_df,
        on=['Гурт/Співак_norm', 'Назва Альбому_norm'],
        how='inner',
        suffixes=('_discount', '_top')
    )

    recommended_df = recommended_df[['Магазин', 'Гурт/Співак_discount', 'Назва Альбому_discount', 'Ціна (Знижка)', 'Посилання']]
    recommended_df = recommended_df.rename(columns={
        'Гурт/Співак_discount': 'Гурт/Співак',
        'Назва Альбому_discount': 'Назва Альбому'
    })
    recommended_df = recommended_df.drop_duplicates(subset=['Магазин', 'Гурт/Співак', 'Назва Альбому', 'Посилання'])
    recommended_df = recommended_df.sort_values(by=['Гурт/Співак', 'Назва Альбому']).reset_index(drop=True)
    return recommended_df


# --- 4. Завантаження ТОП-300 Альбомів ---
@st.cache_data(ttl=3600) # Кешуємо дані на 1 годину
def load_top_albums(path):
    """Завантажує список ТОП-300 альбомів."""
    try:
        top_albums_df = pd.read_csv(path)
        if top_albums_df.empty:
            st.warning("Файл 'top_albums.csv' порожній або не містить даних.")
        else:
            st.success(f"Завантажено {len(top_albums_df)} записів до списку ТОП-300.")
        return top_albums_df
    except FileNotFoundError:
        st.error("ПОМИЛКА: Файл 'top_albums.csv' не знайдено.")
        st.info("Переконайтесь, що 'top_albums.csv' завантажено у той самий репозиторій GitHub, що й 'streamlit_app.py'.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"ПОМИЛКА при завантаженні 'top_albums.csv': {e}")
        return pd.DataFrame()


# --- 5. Логіка Основного Додатку Streamlit ---
st.set_page_config(page_title="Пошук Вигідних Вінілів", layout="wide")
st.title("🎶 Бот для Пошуку Вигідних Вінілів 🎶")

# Завантажуємо конфігурацію магазинів (кешується)
site_configs = get_site_configs_from_sheet(GOOGLE_SHEET_URL)

if not site_configs:
    st.error("Не вдалося завантажити конфігурацію магазинів. Будь ласка, налаштуйте вашу Google Таблицю.")
    st.stop() # Зупиняємо виконання, якщо немає конфігурації

# Завантажуємо ТОП-300 альбомів (кешується)
top_albums_df = load_top_albums(TOP_ALBUMS_CSV_PATH)

if top_albums_df.empty:
    st.error("Не вдалося завантажити список ТОП-300 альбомів. Перевірте файл 'top_albums.csv'.")
    st.stop() # Зупиняємо виконання

# --- Вибір дії та магазинів ---
st.header("Оберіть дію")
action_choice_str = st.radio(
    "Яку дію ви хочете виконати?",
    ("Шукати вініли в одному магазині та порівняти з ТОП-300",
     "Шукати конкретні альбоми (з попереднього пошуку) в інших магазинах"),
    index=0 # Вибираємо першу опцію за замовчуванням
)

# Створюємо словник для зручного вибору магазину за ім'ям
shop_names = [config['name'] for config in site_configs]
shop_name_to_config = {config['name']: config for config in site_configs}

# --- Перша дія: Шукати в одному магазині та порівняти з ТОП-300 ---
if action_choice_str == "Шукати вініли в одному магазині та порівняти з ТОП-300":
    st.subheader("Пошук вінілів в обраному магазині")
    selected_shop_name = st.selectbox(
        "Виберіть магазин для пошуку:",
        shop_names,
        index=0, # За замовчуванням перший магазин
        key="primary_shop_select"
    )
    
    if st.button("Почати пошук та отримати рекомендації", key="start_primary_search"):
        selected_shop_config = shop_name_to_config[selected_shop_name]
        
        with st.spinner(f"Скануємо {selected_shop_name}... Це може зайняти деякий час."):
            current_shop_deals_df = scrape_single_site(selected_shop_config)

        if current_shop_deals_df.empty:
            st.info(f"На жаль, не знайдено вінілів зі знижкою в магазині '{selected_shop_name}'.")
        else:
            recommendations_df = recommend_vinyls(current_shop_deals_df, top_albums_df)
            st.session_state['last_recommendations'] = recommendations_df # Зберігаємо в session_state для наступної дії

            if not recommendations_df.empty:
                st.subheader(f"Знайдено {len(recommendations_df)} рекомендованих вінілів зі знижкою на {selected_shop_name}:")
                # Форматуємо посилання для Streamlit (клікабельні)
                display_df = recommendations_df.copy()
                display_df['Посилання'] = display_df['Посилання'].apply(lambda x: f"[Link]({x})" if x else "N/A")
                st.dataframe(display_df)
                st.info("Ви можете використати ці рекомендації для пошуку в інших магазинах, обравши дію 'Шукати конкретні альбоми...'")
            else:
                st.info(f"У магазині '{selected_shop_name}' не знайдено вінілів зі знижкою зі списку ТОП-300, які б збігалися.")

# --- Друга дія: Шукати конкретні альбоми в інших магазинах ---
elif action_choice_str == "Шукати конкретні альбоми (з попереднього пошуку) в інших магазинах":
    st.subheader("Порівняння цін вибраних альбомів")

    # Перевіряємо, чи є попередні рекомендації
    if 'last_recommendations' not in st.session_state or st.session_state['last_recommendations'].empty:
        st.warning("Спочатку виконайте пошук за першою дією, щоб отримати список рекомендованих альбомів.")
        st.stop()

    last_recommendations_df = st.session_state['last_recommendations']

    st.write("Ось ваші останні рекомендації:")
    # Додаємо колонку ID для вибору
    display_df_with_id = last_recommendations_df[['Гурт/Співак', 'Назва Альбому', 'Магазин']].reset_index().rename(columns={'index': 'ID'})
    st.dataframe(display_df_with_id)

    # Вибір ID альбомів
    st.info("Введіть ID альбомів, які ви хочете порівняти, через кому (наприклад, 0, 1, 5).")
    selected_ids_input = st.text_input("ID альбомів для порівняння:", key="album_ids_compare_input")

    selected_ids = []
    if selected_ids_input:
        try:
            selected_ids = [int(x.strip()) for x in selected_ids_input.split(',') if x.strip().isdigit()]
        except ValueError:
            st.error("Невірний формат ID. Будь ласка, введіть числа через кому.")

    albums_to_search = pd.DataFrame()
    if selected_ids:
        albums_to_search = last_recommendations_df.loc[last_recommendations_df.index.intersection(selected_ids)]
        if albums_to_search.empty:
            st.warning("Вибрані ID не відповідають жодному альбому з останніх рекомендацій.")
            # st.stop() # Не зупиняємо, щоб можна було виправити ввід
        else:
            st.write("Вибрані альбоми для порівняння:")
            st.dataframe(albums_to_search[['Гурт/Співак', 'Назва Альбому', 'Магазин']])

    # Вибір магазинів для порівняння
    available_other_shops = [
        config for config in site_configs 
        if config['name'] not in albums_to_search['Магазин'].unique() # Виключаємо магазин, звідки альбом вже є
    ]

    if not available_other_shops:
        st.warning("Немає інших магазинів для порівняння, або всі магазини вже були використані.")
        # st.stop() # Не зупиняємо
    
    other_shop_names = [config['name'] for config in available_other_shops]
    selected_other_shop_names = st.multiselect(
        "Виберіть додаткові магазини для порівняння цін:",
        other_shop_names,
        key="other_shops_multiselect"
    )

    shops_for_comparison = [shop_name_to_config[name] for name in selected_other_shop_names]

    if st.button("Порівняти ціни", key="compare_prices_button"):
        if albums_to_search.empty or not shops_for_comparison:
            st.warning("Будь ласка, оберіть альбоми та магазини для порівняння.")
        else:
            st.subheader("Розширений пошук та порівняння цін")
            all_comparison_results = []

            # Додаємо оригінальні пропозиції
            for idx, row in albums_to_search.iterrows():
                 all_comparison_results.append({
                    'Магазин': row['Магазин'],
                    'Гурт/Співак': row['Гурт/Співак'],
                    'Назва Альбому': row['Назва Альбому'],
                    'Ціна (Знижка)': row['Ціна (Знижка)'],
                    'Посилання': row['Посилання']
                 })


            for album_idx, album_row in albums_to_search.iterrows():
                st.write(f"Пошук для: **{album_row['Гурт/Співак']}** - *{album_row['Назва Альбому']}*")
                
                found_in_any_other_shop = False
                for shop_config in shops_for_comparison:
                    st.text(f"  Перевіряємо {shop_config['name']}...")
                    
                    temp_deals_df = scrape_single_site(shop_config) 

                    if not temp_deals_df.empty:
                        temp_deals_df['Гурт/Співак_norm'] = temp_deals_df['Гурт/Співак'].astype(str).str.lower().str.strip()
                        temp_deals_df['Назва Альбому_norm'] = temp_deals_df['Назва Альбому'].astype(str).str.lower().str.strip()

                        target_artist_norm = album_row['Гурт/Співак'].lower().strip()
                        target_album_norm = album_row['Назва Альбому'].lower().strip()

                        cond_artist_matches = (
                            (temp_deals_df['Гурт/Співак_norm'].str.contains(target_artist_norm, na=False, regex=False)) 
                            if target_artist_norm else (pd.Series([True] * len(temp_deals_df), index=temp_deals_df.index))
                        )
                        cond_album_matches = temp_deals_df['Назва Альбому_norm'].str.contains(target_album_norm, na=False, regex=False)
                        
                        final_match_condition = cond_artist_matches & cond_album_matches

                        if not target_artist_norm:
                            exact_album_match_if_no_artist = (temp_deals_df['Назва Альбому_norm'] == target_album_norm)
                            final_match_condition = final_match_condition | exact_album_match_if_no_artist

                        found_album_in_shop = temp_deals_df[final_match_condition]
                        
                        if not found_album_in_shop.empty:
                            for idx, found_row in found_album_in_shop.iterrows():
                                all_comparison_results.append({
                                    'Магазин': found_row['Магазин'],
                                    'Гурт/Співак': found_row['Гурт/Співак'],
                                    'Назва Альбому': found_row['Назва Альбому'],
                                    'Ціна (Знижка)': found_row['Ціна (Знижка)'],
                                    'Посилання': found_row['Посилання']
                                })
                            found_in_any_other_shop = True
                        else:
                            st.text(f"    Не знайдено в {shop_config['name']}.")
                    else:
                        st.text(f"    Не вдалося зібрати дані з {shop_config['name']}.")

                if not found_in_any_other_shop:
                    st.warning(f"  Альбом '{album_row['Назва Альбому']}' не знайдено в жодному з вибраних додаткових магазинів.")

            if all_comparison_results:
                final_comparison_df = pd.DataFrame(all_comparison_results).drop_duplicates(subset=['Магазин', 'Гурт/Співак', 'Назва Альбому', 'Посилання'])
                final_comparison_df['Parsed_Price'] = pd.to_numeric(
                    final_comparison_df['Ціна (Знижка)'].astype(str).str.replace('₴', '').str.replace(',', '.').str.replace(' ', ''),
                    errors='coerce'
                )
                final_comparison_df.dropna(subset=['Parsed_Price'], inplace=True)
                
                if not final_comparison_df.empty:
                    st.subheader("Результати порівняння цін:")
                    for (artist, album), group in final_comparison_df.groupby(['Гурт/Співак', 'Назва Альбому']):
                        st.write(f"**{artist}** - *{album}*")
                        # Форматуємо посилання для Streamlit
                        group_display = group.copy() # Створюємо копію для уникнення SettingWithCopyWarning
                        group_display['Посилання'] = group_display['Посилання'].apply(lambda x: f"[Link]({x})" if x else "N/A")
                        st.dataframe(group_display[['Магазин', 'Гурт/Співак', 'Назва Альбому', 'Ціна (Знижка)', 'Посилання']].sort_values(by='Parsed_Price'))
                else:
                    st.info("Не вдалося знайти жодного з вибраних альбомів у додаткових магазинах.")

            else:
                st.info("Не вдалося знайти жодного з вибраних альбомів у додаткових магазинах.")
