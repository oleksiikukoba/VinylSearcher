import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
import json # Для роботи з JSON-ключем сервісного акаунту
import os # Для змінних оточення

# --- 1. Налаштування та Глобальні Змінні ---
# Посилання на вашу Google Таблицю з конфігураціями магазинів
GOOGLE_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1rQw7-Lj42IODQ_lO_c1bV8G_bNuvUwkyc3bcs4xX0Fg/edit?usp=sharing'

# Шлях до файлу з ТОП-300 альбомами на GitHub (в тому ж репозиторії)
TOP_ALBUMS_CSV_PATH = 'top_albums.csv'

# --- 2. Функції для Взаємодії з Google Sheets та Авторизація ---

@st.cache_resource # Кешуємо об'єкт gc, щоб він створювався лише один раз
def authorize_gspread():
    """Авторизує gspread для доступу до Google Таблиць."""
    try:
        # Для Streamlit Community Cloud або інших хостингів:
        # Ключ сервісного акаунту зберігається як секрет STREAMLIT_GCP_SERVICE_ACCOUNT
        # у форматі JSON-рядка.
        gcp_service_account_info = os.getenv('STREAMLIT_GCP_SERVICE_ACCOUNT')
        if gcp_service_account_info:
            # Якщо ключ є у змінних оточення, використовуємо його
            info = json.loads(gcp_service_account_info)
            gc = gspread.service_account_from_dict(info)
            st.success("Авторизація Google Таблиць успішна (через секрети).")
        else:
            # Для локальної розробки або Colab, якщо ви використовуєте credentials.json
            # (цей шлях зазвичай не використовується для розгортання)
            # Якщо ви розгортаєте на Streamlit Community Cloud, ви повинні
            # налаштувати секрет STREAMLIT_GCP_SERVICE_ACCOUNT!
            st.warning("STREAMLIT_GCP_SERVICE_ACCOUNT не знайдено. "
                       "Переконайтесь, що ви налаштували секрети Streamlit Cloud "
                       "або запускаєте локально з файлом ключа.")
            st.info("Спробуємо авторизацію gspread.service_account() без параметрів. "
                    "Це може вимагати файлу 'gcp_credentials.json' поруч з додатком.")
            # Це працює, якщо файл `gcp_credentials.json` лежить поруч з додатком
            # (що не рекомендується для публічних репозиторіїв)
            gc = gspread.service_account() # Спробуємо стандартний спосіб

        return gc
    except Exception as e:
        st.error(f"ПОМИЛКА АВТОРИЗАЦІЇ Google Таблиць: {e}")
        st.info("Переконайтесь, що ви надали дозволи таблиці або коректно налаштували секрети/файл ключа.")
        return None

gc = authorize_gspread()


@st.cache_data(ttl=3600) # Кешуємо дані на 1 годину
def get_site_configs_from_sheet(sheet_url):
    """Отримує конфігурацію магазинів з Google Таблиці."""
    if not gc:
        st.error("Gspread не авторизовано. Неможливо завантажити конфігурацію.")
        return []
    
    try:
        spreadsheet = gc.open_by_url(sheet_url)
        worksheet = spreadsheet.sheet1
        records = worksheet.get_all_records()

        site_configs = []
        for record in records:
            config = {
                'name': record.get('Name'),
                'base_url': record.get('BaseURL'),
                'pagination_param': record.get('PaginationParam'),
                'start_page': int(record.get('StartPage', 1)),
                'end_page': int(record.get('EndPage', 1)),
                'selectors': {
                    'product_container': record.get('ProductContainer'),
                    'title_element': record.get('TitleElement'),
                    'price_element': record.get('PriceElement'),
                    'link_element': record.get('LinkElement'),
                    'ArtistElement': record.get('ArtistElement')
                }
            }
            if all(config[key] for key in ['name', 'base_url']) and \
               all(config['selectors'][key] for key in ['product_container', 'title_element', 'price_element', 'link_element']):
                site_configs.append(config)
            else:
                st.warning(f"Пропущено сайт у конфігурації через незаповнені обов'язкові поля: {record}")
        return site_configs
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"ПОМИЛКА: Таблицю за посиланням '{sheet_url}' не знайдено.")
        st.info("Переконайтесь, що посилання правильне і таблиця має потрібні дозволи ('Будь-хто, хто має посилання' -> 'Читач').")
        return []
    except Exception as e:
        st.error(f"ПОМИЛКА при читанні конфігурації з Google Таблиці: {e}")
        st.info("Переконайтесь, що заголовки колонок у таблиці написані точно так: Name, BaseURL, PaginationParam, StartPage, EndPage, ProductContainer, TitleElement, PriceElement, LinkElement, ArtistElement")
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

    product_container_selectors = [s.strip() for s in selectors['product_container'].split(',') if s.strip()]
    title_element_selectors = [s.strip() for s in selectors['title_element'].split(',') if s.strip()]
    price_element_selectors = [s.strip() for s in selectors['price_element'].split(',') if s.strip()]
    link_element_selectors = [s.strip() for s in selectors['link_element'].split(',') if s.strip()]
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
            st.warning(f"  На сторінці {page_num} не знайдено товарів за жодним із селекторів '{selectors['product_container']}'.")
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

            for selector in title_element_selectors:
                title_tag = product.select_one(selector)
                if title_tag:
                    album_name = title_tag.get_text(strip=True).replace('\xa0', ' ').strip()
                    break

            if not artist_name and ' - ' in album_name:
                parts = album_name.split(' - ', 1)
                artist_name = parts[0].strip()
                album_name = parts[1].strip()

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
action_choice = st.radio(
    "Яку дію ви хочете виконати?",
    ("Шукати вініли в одному магазині та порівняти з ТОП-300",
     "Шукати конкретні альбоми (з попереднього пошуку) в інших магазинах"),
    index=0 # Вибираємо першу опцію за замовчуванням
)

# Створюємо словник для зручного вибору магазину за ім'ям
shop_names = [config['name'] for config in site_configs]
shop_name_to_config = {config['name']: config for config in site_configs}

if action_choice == "Шукати вініли в одному магазині та порівняти з ТОП-300":
    st.subheader("Пошук вінілів в обраному магазині")
    selected_shop_name = st.selectbox(
        "Виберіть магазин для пошуку:",
        shop_names,
        index=0 # За замовчуванням перший магазин
    )
    
    if st.button("Почати пошук та отримати рекомендації"):
        selected_shop_config = shop_name_to_config[selected_shop_name]
        
        with st.spinner(f"Скануємо {selected_shop_name}... Це може зайняти деякий час."):
            current_shop_deals_df = scrape_single_site(selected_shop_config)

        if current_shop_deals_df.empty:
            st.info(f"На жаль, не знайдено вінілів зі знижкою в магазині '{selected_shop_name}'.")
        else:
            recommendations_df = recommend_vinyls(current_shop_deals_df, top_albums_df)
            st.session_state['last_recommendations'] = recommendations_df # Зберігаємо для наступної дії

            if not recommendations_df.empty:
                st.subheader(f"Знайдено {len(recommendations_df)} рекомендованих вінілів зі знижкою на {selected_shop_name}:")
                # Форматуємо для Streamlit (посилання клікабельні)
                recommendations_df['Посилання'] = recommendations_df['Посилання'].apply(lambda x: f"[Link]({x})" if x else "N/A")
                st.dataframe(recommendations_df)
                st.info("Ви можете використати ці рекомендації для пошуку в інших магазинах, вибравши іншу дію вище.")
            else:
                st.info(f"У магазині '{selected_shop_name}' не знайдено вінілів зі знижкою зі списку ТОП-300, які б збігалися.")

elif action_choice == "Шукати конкретні альбоми (з попереднього пошуку) в інших магазинах":
    st.subheader("Порівняння цін вибраних альбомів")

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
    selected_ids_input = st.text_input("Введіть ID альбомів:", key="album_ids_input")

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
            st.stop()
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
        st.stop()

    other_shop_names = [config['name'] for config in available_other_shops]
    selected_other_shop_names = st.multiselect(
        "Виберіть додаткові магазини для порівняння цін:",
        other_shop_names,
        key="other_shops_multiselect"
    )

    shops_for_comparison = [shop_name_to_config[name] for name in selected_other_shop_names]

    if st.button("Порівняти ціни"):
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
                    
                    # Запускаємо скрейпінг сторінок магазину (не пошук по сайту)
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
                
                # Перетворюємо ціну на число для сортування
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
