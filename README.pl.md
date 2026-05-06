# Mini Data Warehouse For E-commerce with Apache Airflow
[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/README.md)
[![pl](https://img.shields.io/badge/lang-pl-green.svg)](https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow/blob/main/README.pl.md)

Projekt ten jest kompeltnym pipeline ETL (Extract, Transform, Load), który pobiera dane o produktach, koszykach i użytkownikach ze strony **dummyjson.com**, przetwarza je i ładuje do bazy danych PostgreSQL. Pipeline jest zaaranożowany przy użyciu Apache Airflow.

## Przegląd projektu
Projekt implementuje mini hurtownię danych dla danych e-commerce z wykorzystaniem Apache Airflow.

Pipeline pobiera dane produktowe z publicznego API, przekształca je do formatu analitycznego i ładuje do bazy danych PostgreSQL.

Zestaw danych obejmuje:

#### Produkty
- **informacje o produkcie** (product_id, title, description, category, price, discount_percentage, overall_rating, stock, brand, final_price, price_bucket, rating_bucket, value_score)
- **recenzje produktów** (review_id, product_id, review_comment, review_date, rating, review_year, review_month, review_bucket)
- **tagi produktów** (tag_id, product_id, tag)
#### Koszyki
- **informacje o koszykach** (cart_id, cart_total, cart_total_discounted, user_id, cart_total_quantity, cart_size_bucket_cart_value_bucket)
- **elementy koszyka** (cart_id, product_id, product_price, product_quantity, product_total, product_total_discounted)
#### Użytkownicy
- informacje o użytkownikach (user_id, first_name, last_name, gender, birth_date, city, state, state_code, postal_code, country, age_group)

Głównym celem projektu jest symulacja rzeczywistego workflow data engineeringowego, w tym:
- projektowanie pipeline’u ETL
- czyszczenie i walidacja danych
- feature engineering na potrzeby analityki
- zaaranżowanie workflow przy użyciu Apache Airflow

Dodatkowym celem jest analiza pozyskanych danych w celu wydobycia istotnych wniosków oraz wsparcia podejmowania decyzji opartych na danych.

## Struktura projektu
```
mini_data_warehouse/
|
├── dags/                               # Zawiera DAGi Apache Airflow odpowiedzialne za orkiestrację workflow
|   ├── create_table.py                 # Inicjalizuje schemat bazy danych i tworzy tabele
|   ├── etl_carts.py                   # Definiuje workflow ETL dla danych koszyków
|   └── etl_products_users.py          # Definiuje workflow ETL dla danych produktów i użytkowników
├── data/                               # Przechowuje dane używane w pipeline
|   ├── raw                            # Surowe dane pobrane z API (format JSON)
|   └── transformed                    # Oczyszczone i przetworzone dane gotowe do załadowania (CSV)
├── etl/                                # Implementuje logikę ETL
|   ├── transform/                      # Obsługuje logikę transformacji danych
|   |   ├── carts_products_transform.py     # Transformuje dane elementów koszyka
|   |   ├── carts_transform.py              # Transformuje dane koszyków
|   |   ├── products_reviews_transform.py   # Transformuje dane recenzji produktów
|   |   ├── products_tags_transform.py      # Transformuje dane tagów produktów
|   |   ├── products_transform.py           # Transformuje dane produktów
|   |   └── users_transform.py              # Transformuje dane użytkowników
|   ├── extract.py                      # Pobiera dane z zewnętrznych źródeł
|   └── load.py                         # Ładuje dane do bazy PostgreSQL
├── sql/                                # Przechowuje zapytania SQL
|   ├── create/                         # Zapytania tworzące tabele
|   └── insert/                         # Zapytania wstawiające dane
├── utils/
|   └── files_io.py                     # Funkcje pomocnicze do obsługi plików
├── .env                                # Zmienne środowiskowe
├── docker-compose.yaml                 # Definicja środowiska (Airflow + PostgreSQL)
├── key_generator.py                    # Generuje FERNET_KEY i SECRET_KEY
├── requirements.txt                    # Lista zależności Pythona
└── README.md                           # Dokumentacja projektu
```

## Wykorzystane technologie
- SQL
- PostgreSQL
- Python (ETL scripts)
- Docker
- Git / Github

## DAgi Airflow
Projekt wykorzystuje Apache Airflow do zaaranżowania workflow danych za pomocą DAGów (Directed Acyclic Graphs). Każdy DAG definiuje sekwencję zadań oraz ich zależności, umożliwiając automatyczne i powtarzalne przetwarzanie danych.
- __create_table.py__ – odpowiada za inicjalizację schematu bazy danych
- __etl_carts.py__ – główny DAG realizujący pipeline ETL dla koszyków (uruchamiany co godzinę)
- __etl_products_users.py__ – główny DAG realizujący pipeline ETL dla produktów i użytkowników (uruchamiany raz dziennie)

## Pipeline ETL
Proces ETL składa się z:

#### 1. Extract
- Pobiera dane przy użyciu zapytań HTTP
- Pobiera surowe dane z zewnętrznego API
- Zapisuje dane jako JSON (dla śledzalności)

#### 2. Transform
- Dane są odczytywane z pliku JSON przy użyciu Pandas
- Zagnieżdżone struktury są spłaszczane
- Dane są normalizowane
- Obsługiwane są brakujące wartości i usuwane niespójności logiczne
- Tworzone są dodatkowe kolumny analityczne
- Dane zapisywane są jako CSV

#### Load
- Dane są odczytywane z CSV przy użyciu Pandas
- Połączenie realizowane przez Airflow PostgresHook
- Dane są wstawiane do tabeli w PostgreSQL

## Instrukcja uruchomienia
#### 1. Sklonuj repozytorium
```
git clone https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow.git
cd Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow
```
#### 2. Stwórz środowisko wirtualne
Upewnij się, że masz Pythona, następnie stwórz i aktywuj środowisko witrualne:
```
python -m venv venv
source venv/bin/activate    # On macOS/Linux
venv\Scripts\activate       # On Windows
```
#### 3. Zainstaluj zależności
Zainstaluj wymagane biblioteki Pythona:
```
pip install -r requirements.txt
```
#### 4. Stwórz zmienne środowiskowe
Stwórz plik `.env` w głownym folderze projektu i przypisz następujące wartości:
```
POSTGRES_USERNAME= 
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DATABASE_NAME=

AIRFLOW_UID=                            # 1000 on macOS/Linux or 50000 on Windows
AIRFLOW_WWW_USER_USERNAME=
AIRFLOW_WWW_USER_PASSWORD=
FERNET_KEY=
SECRET_KEY=

WEBSITE_PATH="https://dummyjson.com/"
```
`FERNET_KEY` oraz `SECRET_KEY` powinny zostać wygenerowane. Możesz do tego wykorzystać plik `key_generator.py` zawarty w projekcie.

#### 5. Uruchomienie środowiska
Uruchom Docker compose aby włączyć Airflow i PostgreSQL
```
docker compose up -d
```
Ta komenda aktywuje:
- Airflow webserver
- Airflow scheduler
- PostgreSQL database

#### 6. Dostęp do Airflow UI
Otwórz przeglądarkę i przejdź pod adres:
```
http://localhost:8080
```
lub _(jeśli powyższy nie działa)_:
```
http://127.0.0.1:8080
```
Zaloguj się przy użyciu nazwy użytkownika i hasła ustawionego w zmiennych środowiskowych.

#### 7. Uruchomienie pipeline
1. Włącz DAG `create_product_table`
2. Uruchom go ręcznie aby stworzyć tabele SQL
3. Włącz DAG `etl_carts` oraz `etl_products_users`
4. Uruchom je ręcznie lub poczekaj na scheduler

#### 8. Verify Results
- Dane surowe - `data/raw/`
- Dane przetworzone - `data/transformed/`
- Tabela w bazie danych:
    ```
    docker exec -it postgres bash

    psql -U YOUR_POSTGRES_USERNAME -d YOUR_POSTGRES_DATABASE

    SELECT * FROM products LIMIT(10);
    ```
### Zatrzymanie środowiska
Aby zatrzymać wszystkie aktywowane serwisy wywołaj komendę:
```
docker compose down
```
### Reset środowiska
Aby usunąć wszelkie dane i zacząć z świeżym środowiskiem wywołaj komendę:
```
docker compose down -v
```
## Możliwe usprawnienia do późniejszej implementacji
- Dodanie logowania dla lepszego monitoroeania i debugowania
- Integracja z narzędziami BI (Power BI / Tableau)
- Wdrożenie pipeline'u do chmury
