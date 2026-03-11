# Data Engineering Technical

# Assessment - Mid-Level

## Echovita data integration

```
Format: Take-home assignment
Language: Python and SQL.
```
## Background

```
As part of the Veritas project's efforts to achieve maximum coverage of the
death index, new data sources have been identified and assigned to the
engineering team for integration into the system.
```
```
One of these sources is Echovita https://www.echovita.com, which provides
obituaries of deceased individuals that could be used to enrich the FOD
data.
```
```
To incorporate this new data source, two main tasks must be performed:
```
1. Obtain data on deceased individuals from the website
2. Consolidate the historical results for these individuals.

## Part 1: Web Scraping

```
Design a program that extracts obituary information from this endpoint:
https://www.echovita.com/us/obituaries
```
```
Requirements:
```
1. **Technology** : You must use the Scrapy
    https://docs.scrapy.org/en/latest/ framework for this task.
2. **Data Extraction** : Your spider should extract the following fields
    from each obituary:
       ○ Full name of the deceased


```
○ Date of birth
○ Date of death
○ Obituary text (full text)
```
3. **Pagination** : Implement pagination handling. The spider should crawl
    a maximum of 5 pages back from the main obituaries page.
4. **Missing Data** : If any field is not available or cannot be
    extracted, use null in the output.
5. **S3 integration:** Each of the scraped items must be uploaded in JSON
https://www.json.org/json-en.html format to an S3 bucket. 6. **GCS
integration:** Each of the scraped items must be uploaded in JSON
https://www.json.org/json-en.html format to a GCP bucket. 7. **Local
export:** When the spider finishes, all scraped results must be
exported in JSONL format https://jsonlines.org/ to the root of the
project

**Notes:**

```
● Focus on code quality and design over feature completeness
```
```
● The S3 and GCS buckets implementation should be mocked - we don't
expect actual AWS or GCP integration
● Think about extensibility - how easy would it be to add a new data
source to the project
```
## Part 2: Data consolidation

```
In the future, all changes to the geographic data of deceased persons from
the echovita source will be stored in a historical table, following an SCD
( Slowly Changing Dimensions) model as shown below:
person_ id name state city valid_from valid_to
```
```
1 John Doe TX Houson 2020-01-
1
```
### 2022-07-

```
1 John Doe TX Dallas 2022-07-
5
```
### 2023-08-

```
1 John Doe TX null 2023-08-
9
```
```
null
```

```
2 Richard smith CA San 2022-04-
```
```
3 Max
Mustermann
```
### CA 2000-07-

### 2

```
null
```
You have been asked to consolidate these records into a single table with
the following columns:

```
● person_id: The person's ID
● distinct_cities: How many cities have they lived in
● first_city: The first city where they lived
● last_city: The last city where they lived
● last_non_null_city: The last non-null city where they lived
```
## Part 3: Workflow Orchestration with Apache Airflow

```
All components of this solution must be orchestrated using an Apache
Airflow DAG, ensuring the full pipeline is production-ready, modular, and
extensible.
```
```
The DAG should coordinate:
```
```
● Web scraping execution (Scrapy spider)
```
```
● Upload validation (mocked S3 and GCS uploads)
```
```
● Local JSONL export verification
```
```
● Data consolidation step
```
```
Notes:
```
```
● You are free to choose whichever SQL processing engine you want
```
```
for this section. However, I suggest using an analytics engine
```
```
(such as https://duckdb.org/)
```

● DAG must be scheduled daily (8:00 am UTC)

● Retries enabled

● Logging enabled

● Clear task dependencies

● Idempotent behavior


