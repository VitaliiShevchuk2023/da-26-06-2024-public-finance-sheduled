import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import schedule
import time
import os
import logging

# Налаштування логування
logging.basicConfig(filename='output.log', level=logging.INFO)

# Функція для отримання даних з API
def get_transaction_function(date_to_process):
    logging.info(f"Processing for date: {date_to_process}")

    # Форматування дати для API запиту
    start_date = date_to_process.strftime("%Y-%m-%d")
    end_date = date_to_process.strftime("%Y-%m-%d")

    url = "https://api.spending.gov.ua/api/v2/api/transactions/"
    params = {"startdate": start_date, "enddate": end_date}
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Викидає помилку для статусів HTTP 4xx/5xx

        data = response.json()

        # Якщо дані відсутні, пропустити обробку
        if not data:
            logging.info("No data received for the specified date range.")
            return None

        new_data = pd.DataFrame(data)

        # Визначення колонок, які потрібно видалити
        columns_to_drop = [
            'id', 'doc_vob', 'doc_vob_name', 'doc_number', 'doc_date', 'doc_v_date',
            'amount_cop', 'currency', 'payer_account', 'payer_mfo',
            'payer_bank', 'payer_edrpou_fact', 'payer_name_fact', 'recipt_account',
            'recipt_mfo', 'recipt_bank', 'recipt_edrpou_fact', 'recipt_name_fact',
            'doc_add_attr', 'region_id', 'payment_type', 'payment_data', 'source_id',
            'source_name',  'kpk', 'contractId', 'contractNumber', 'budgetCode',
            'system_key', 'system_key_ff'
        ]

        # Видалення колонок, якщо вони існують у даних
        columns_to_drop_existing = [col for col in columns_to_drop if col in new_data.columns]
        df = new_data.drop(columns=columns_to_drop_existing)

        # Фільтрація даних за кодами ЄДРПОУ
        edrpou_codes = [
            '04358000', '33800777', '04360623', '04376624', '04369848', '25299709', '04054903', '04363662',
            '04362489', '04054636', '26376375', '04360586', '04358477', '26376300', '34627780', '04363834',
            '04054866', '04054628', '04359732', '04363509', '04358508', '04359146', '04360617', '04359152',
            '04363647', '04359488', '04412395', '04361491', '04363876', '04360296', '04363538', '04054613',
            '04359873', '04363886', '04363811', '04359287', '04359904', '04362697', '04360600', '04362183',
            '04358619', '04363343', '04054984', '04359867', '35161650', '04054978', '04359620', '04361723',
            '04527520', '04359643', '40883878', '04358218', '04358997', '04054961', '26425731', '04360913',
            '04358916', '04054955', '04361605', '42096329', '04361284', '04054990', '35161509', '34446857',
            '04362148', '04363225', '04361628', '04362160', '05408823'
        ]

        # Фільтрування DataFrame за кодами ЄДРПОУ платника або отримувача
        filtered_df = df[(df['payer_edrpou'].isin(edrpou_codes)) | (df['recipt_edrpou'].isin(edrpou_codes))]

        return filtered_df

    except requests.RequestException as e:
        logging.error("Error during API request: %s", e)
        return None

# Функція для збереження даних у CSV
def save_data_to_csv(df, file_path):
    try:
        if os.path.exists(file_path):
            # Якщо файл існує, зчитаємо існуючі дані
            existing_df = pd.read_csv(file_path)
            # Об'єднуємо нові дані з існуючими
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_csv(file_path, index=False)
        else:
            # Якщо файл не існує, зберігаємо нові дані як новий файл
            df.to_csv(file_path, index=False)
        logging.info("Data successfully saved to %s", file_path)
    except Exception as e:
        logging.error("Error saving data to CSV: %s", e)

# Запланована функція для виконання ETL щодня о 20:00
def scheduled_etl():
    date_to_process = datetime.now() - timedelta(days=1)
    new_data = get_transaction_function(date_to_process)

    if new_data is not None and not new_data.empty:
        logging.info(f"Number of records fetched: {len(new_data)}")
        save_data_to_csv(new_data, "transactiondata_full.csv")
    else:
        logging.info("No new data to process.")

# Налаштування розкладу виконання ETL
schedule.every().day.at("20:00").do(scheduled_etl)

# Web-інтерфейс Streamlit
def app():
    st.title("Data Analytics Dashboard")
    
    # Завантаження даних з CSV
    if os.path.exists("dataset/transactiondata_full.csv"):
        df = pd.read_csv("dataset/transactiondata_full.csv")
        st.write(f"Total records: {len(df)}")
        
        # Відображення таблиці даних
        st.dataframe(df.head(10))
        
        # Фільтрація даних за вибраним ЄДРПОУ
        edrpou_filter = st.text_input("Enter EDRPOU code to filter", "")
        if edrpou_filter:
            filtered_df = df[(df['payer_edrpou'] == edrpou_filter) | (df['recipt_edrpou'] == edrpou_filter)]
            st.write(f"Total filtered records: {len(filtered_df)}")
            st.dataframe(filtered_df)
            
        # Візуалізація даних
        st.write("### Data Visualization")
        st.bar_chart(df['amount'])
    else:
        st.write("Data file not found. Please run the ETL process first.")
    
if __name__ == "__main__":
    app()
    
    # Запуск планувальника
    while True:
        schedule.run_pending()
        time.sleep(60)  # Перевірка розкладу кожні 60 секунд

