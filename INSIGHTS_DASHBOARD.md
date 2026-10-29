# 📊 Insights & Dashboard — Weather × Hospitality Pipeline

> تحليل شامل لبيانات الطقس والضيافة في مصر، مولّد من ELT Pipeline (Airflow → SQL Server → Snowflake → dbt)

---

## 1. نظرة عامة على المشروع (Architecture)

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Open-Meteo  │     │   TomTom     │     │  Hotels CSV  │     │ Restaurants  │
│  (Weather)   │     │ (Landmarks)  │     │              │     │    CSV       │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │                    │
       ▼                    ▼                    ▼                    ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    Airflow: ingestion_dag                              │
  │              (Bronze layer in SQL Server)                               │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  │ ExternalTaskSensor
                                  ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                  Airflow: transformation_dag                            │
  │          (Bronze → Silver in SQL Server via pandas)                     │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  │ ExternalTaskSensor
                                  ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    Airflow: loader_dag                                  │
  │          (Silver → Snowflake RAW_DATA via write_pandas)                  │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  │
                                  ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    dbt: Staging (silver_layer)                           │
  │  stg_daily_weather · stg_hourly_weather · stg_current_weather            │
  │  stg_hotel_data · stg_restaurant_data · stg_famous_landmark_data         │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  │
                                  ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    dbt: Marts (gold_layer)                               │
  │  mart_city_destination_profile · mart_hotel_analysis                     │
  │  mart_city_tourism_index                                                 │
  └─────────────────────────────────────────────────────────────────────────┘
```

### العلاقات بين الجداول (Data Relationships)

| المصدر | الجدول Bronze (SQL Server) | الجدول Silver (SQL Server) | الجدول Snowflake (RAW_DATA) | dbt Staging | dbt Mart |
|--------|---------------------------|---------------------------|------------------------------|-------------|----------|
| Open-Meteo API | Bronze_weather_raw | Silver_weather_daily_data | Bronze_daily_weather_data | stg_daily_weather_data | mart_city_destination_profile |
| Open-Meteo API | Bronze_weather_raw | Silver_weather_hourly_data | Bronze_hourly_weather_data | stg_hourly_weather_data | — |
| Open-Meteo API | Bronze_weather_raw | Silver_weather_current_data | Bronze_current_weather_data | stg_current_weather_data | mart_city_destination_profile |
| TomTom API | Bronze_famous_landmark_data | Silver_famous_landmark_data | Silver_famous_landmark_data | stg_famous_landmark_data | mart_city_destination_profile |
| Hotels CSV | Bronze_hotel_data | (مباشر) | Bronze_hotel_data | stg_hotel_data | mart_hotel_analysis |
| Restaurants CSV | Bronze_restaurant_data | (مباشر) | Bronze_restaurant_data | stg_restaurant_data | mart_city_destination_profile |

### مفتاح الربط بين الجداول

- **`city`**: المخزّن بالعربية في بيانات الطقس والمعالم (القاهرة، الجيزة، الإسكندرية، الأقصر)
- **`city`**: المخزّن بالإنجليزية في بيانات المطاعم (Cairo, Giza, Alexandria, Hurghada, Sharm El Sheikh)
- **الحل**: `city_name_mapping` seed في dbt يعمل كـ lookup table لربط الاسمين

---

## 2. المدن المغطاة والبيانات المتاحة

| المدينة | الطقس (عربي) | المعالم (عربي) | المطاعم (إنجليزي) | الفنادق | الإحداثيات |
|---------|------------|---------------|-------------------|---------|------------|
| القاهرة (Cairo) | ✅ | ✅ | ✅ | ✅ | 30.04, 31.24 |
| الجيزة (Giza) | ✅ | ✅ | ✅ | ✅ | 29.98, 31.13 |
| الإسكندرية (Alexandria) | ✅ | ✅ | ✅ | ✅ | 31.20, 29.92 |
| الأقصر (Luxor) | ✅ | ✅ | ❌ | ✅ | 25.69, 32.64 |
| الغردقة (Hurghada) | ❌ | ❌ | ✅ | ✅ | — |
| شرم الشيخ (Sharm El Sheikh) | ❌ | ❌ | ✅ | ✅ | — |

> **Insight**: الغردقة وشرم الشيخ موجودان في المطاعم والفنادق لكن غير موجودين في بيانات الطقس والمعالم — يمكن إضافتهما لاحقاً.

---

## 3. Dashboard 1: City Tourism Index (مؤشر السياحة لكل مدينة)

### المنطق: Tourism Index (0–100)

| المكوّن | الوزن | الوصف |
|---------|-------|-------|
| Weather Comfort Score | 35% | مدى قرب الحرارة من 22°م (النطاق المثالي 18–26°م) |
| Wind Score | 15% | سرعة الرياح (أقل من 20 كم/سا = ممتاز) |
| Landmark Density Score | 25% | عدد المعالم السياحية (مُعَدَّل نسبياً) |
| Restaurant Variety Score | 15% | تنوع المطاعم (مُعَدَّل نسبياً) |
| Hotel Value Score | 10% | متوسط قيمة الفنادق (التقييم/السعر) |

الأوزان المتاحة فقط تُعاد موازنتها عند غياب مصدر، لذلك لا يتحول نقص البيانات تلقائياً إلى درجة صفر. يعرض `data_coverage_pct` نسبة الأوزان التي لها بيانات، ويحدد `index_methodology_version` إصدار المعادلة. درجات المعالم والمطاعم وقيمة الفنادق نسبية إلى المدن المتاحة في نفس البناء؛ لا تقارنها بين تشغيلات مختلفة دون مراعاة تغيّر مجموعة البيانات.

### الكود المرجعي (SQL في Snowflake):

```sql
-- نتيجة mart_city_tourism_index
SELECT
    city,
    current_temp_c,
    landmark_count,
    restaurant_count,
    hotel_count,
    tourism_index,
    destination_tier
FROM mart_city_tourism_index
ORDER BY tourism_index DESC;
```

### النتيجة المتوقعة:

| city | current_temp_c | landmark_count | restaurant_count | tourism_index | destination_tier |
|------|---------------|---------------|-----------------|---------------|-----------------|
| Cairo | ~24 | ~30 | ~200 | ~85 | Top Destination |
| Giza | ~25 | ~40 | ~50 | ~78 | Top Destination |
| Alexandria | ~22 | ~15 | ~300 | ~72 | Good Destination |
| Luxor | ~28 | ~35 | 0 | ~55 | Good Destination |

---

## 4. Dashboard 2: Hotel Analysis (تحليل الفنادق)

### الأعمدة الرئيسية في `mart_hotel_analysis`:

| العمود | الوصف |
|--------|-------|
| `hotel_name` | اسم الفندق/الوحدة |
| `type` | hotel أو vacation rental |
| `final_price` | السعر النهائي لليلة (بعد الضرائب) |
| `base_price` | السعر الأساسي (قبل الضرائب) |
| `total_taxes` | الفارق = final_price - base_price |
| `tax_pct` | نسبة الضرائب من السعر الأساسي |
| `weighted_score` | التقييم المرجّح (reviews × rating) |
| `value_score` | القيمة مقابل السعر = weighted_score / final_price |
| `price_tier` | Budget / Mid-range / Upper-mid / Luxury |
| `amenity_count` | عدد المرافق |
| `recommended_rank` | ترتيب التوصية (1 = الأفضل) |
| `nearest_city` | أقرب مدينة (عن طريق الإحداثيات) |
| `weather_max_temp_c` | أقصى حرارة في أقرب مدينة |
| `weather_min_temp_c` | أدنى حرارة في أقرب مدينة |

### Insights SQL:

```sql
-- أفضل 10 فنادق من حيث القيمة مقابل السعر
SELECT hotel_name, nearest_city, final_price, weighted_score, value_score, price_tier
FROM mart_hotel_analysis
WHERE final_price > 0
ORDER BY value_score DESC
LIMIT 10;

-- متوسط الأسعار والتقييمات حسب المدينة
SELECT
    nearest_city,
    COUNT(*) AS hotel_count,
    ROUND(AVG(final_price), 0) AS avg_price,
    ROUND(AVG(weighted_score), 1) AS avg_score,
    ROUND(AVG(amenity_count), 1) AS avg_amenities
FROM mart_hotel_analysis
GROUP BY nearest_city
ORDER BY avg_score DESC;

-- توزيع الفنادق حسب الفئة السعرية
SELECT
    nearest_city,
    price_tier,
    COUNT(*) AS count
FROM mart_hotel_analysis
GROUP BY nearest_city, price_tier
ORDER BY nearest_city, price_tier;
```

---

## 5. Dashboard 3: Weather × Tourism (الطقس والسياحة)

```sql
-- مقارنة ظروف الطقس بين المدن
SELECT
    city,
    daily_max_temp_c,
    daily_min_temp_c,
    max_wind_speed,
    current_temp_c,
    current_humidity_pct,
    ROUND((daily_max_temp_c + daily_min_temp_c) / 2, 1) AS avg_daily_temp,
    CASE
        WHEN daily_max_temp_c > 35 THEN 'Hot'
        WHEN daily_max_temp_c BETWEEN 25 AND 35 THEN 'Warm'
        WHEN daily_max_temp_c BETWEEN 18 AND 25 THEN 'Pleasant'
        WHEN daily_max_temp_c BETWEEN 10 AND 18 THEN 'Cool'
        ELSE 'Cold'
    END AS weather_category
FROM mart_city_destination_profile
ORDER BY avg_daily_temp DESC;

-- أفضل وقت للزيارة (بناءً على توقعات الطقس اليومية)
SELECT
    city,
    daily_date,
    max_temp AS max_temp_c,
    min_temp AS min_temp_c,
    weather_code,
    CASE
        WHEN max_temp BETWEEN 18 AND 28 THEN '✅ Ideal'
        WHEN max_temp BETWEEN 28 AND 35 THEN '⚠️ Warm'
        WHEN max_temp > 35 THEN '🔥 Hot'
        WHEN max_temp < 15 THEN '🧊 Cold'
        ELSE 'Check'
    END AS visit_recommendation
FROM stg_daily_weather_data
ORDER BY city, daily_date;
```

---

## 6. Dashboard 4: Restaurant & Dining (المطاعم والطعام)

```sql
-- تنوع المطاعم حسب المدينة والفئة
SELECT
    city,
    category,
    COUNT(*) AS count
FROM stg_restaurant_data
GROUP BY city, category
ORDER BY city, count DESC;

-- المدن الأكثر تنوعاً في المطاعم
SELECT
    city,
    COUNT(*) AS total_restaurants,
    COUNT(DISTINCT category) AS unique_categories
FROM stg_restaurant_data
GROUP BY city
ORDER BY total_restaurants DESC;
```

---

## 7. Dashboard 5: Landmark Analysis (تحليل المعالم)

```sql
-- أكثر المعالم حسب المدينة والفئة
SELECT
    city,
    category,
    COUNT(*) AS landmark_count,
    ROUND(AVG(score), 1) AS avg_score
FROM stg_famous_landmark_data
GROUP BY city, category
ORDER BY city, landmark_count DESC;

-- أعلى المعالم تقييماً
SELECT
    landmark_name,
    city,
    score,
    category,
    address
FROM stg_famous_landmark_data
ORDER BY score DESC
LIMIT 20;
```

---

## 8. Key Insights (رؤى رئيسية)

### 1. 🌡️ الطقس والسياحة
- **القاهرة والجيزة**: درجات حرارة مثالية للزيارة (22–28°م في معظم الأوقات)
- **الأقصر**: أحر المدن — يحتاج تخطيط للزيارة في الأشهر الباردة
- **الإسكندرية**: الأكثر اعتدالاً بفضل البحر

### 2. 🏨 الفنادق والقيمة
- هناك 947 وحدة فندقية في البيانات (خليط من hotels و vacation rentals)
- `value_score` يكشف عن الفنادق ذات القيمة الأعلى مقابل السعر
- `tax_pct` يوضح نسبة الضرائب المضافة على السعر الأساسي

### 3. 📍 المعالم السياحية
- بيانات TomTom توفر معالم سياحية في نطاق 40km من مركز كل مدينة
- الأقصر والجيزة من المتوقع أن يكون لهما أعلى كثافة معالم

### 4. 🍽️ المطاعم
- الإسكندرية والقاهرة لديها أكبر تنوع في أنواع المطاعم
- مطاعم شرم الشيخ والغردقة مرتبطة بالمنتجعات السياحية

### 5. ⚠️ فجوات البيانات
- **الغردقة وشرم الشيخ**: لا توجد بيانات طقس/معالم (يمكن إضافتهما لاحقاً)
- **الأقصر**: لا توجد بيانات مطاعم في الـ CSV
- **أسماء المدن**: تم حل مشكلة التطابق عبر `city_name_mapping` seed

---

## 9. dbt Model Graph

```
                    city_name_mapping (seed)
                           /        |       \
                         /          |         \
    stg_daily_weather    stg_current_weather   stg_famous_landmark
    stg_hourly_weather   stg_hotel_data         stg_restaurant_data
           \                |   \                    /
            \               |    \                  /
    mart_city_destination_profile    mart_hotel_analysis
                    \                    /
                     \                  /
                mart_city_tourism_index
```

---

## 10. كيفية تشغيل التحليلات

```bash
# 1. شغّل الـ Airflow pipeline
astro dev start

# 2. شغّل dbt (بعد اكتمال الـ loader_dag)
cd include/dbt_project
dbt seed          # يحمّل city_name_mapping.csv
dbt run          # يبني staging + marts
dbt test         # يشغّل كل الـ tests

# 3. استعلم عن الـ dashboards في Snowflake
USE WAREHOUSE WEATHER_WH;
USE DATABASE RENTAL_WEATHER_DB;
USE SCHEMA GOLD_LAYER;

-- Tourism index
SELECT * FROM mart_city_tourism_index ORDER BY tourism_index DESC;

-- Hotel analysis
SELECT * FROM mart_hotel_analysis WHERE final_price > 0 ORDER BY value_score DESC LIMIT 20;

-- City profile
SELECT * FROM mart_city_destination_profile;
```
