**Dataset Title \-** OpenStax Tutor Learning Platform Export Datasets

**Dataset Description** \- Comprehensive datasets from OpenStax Tutor platform including 
student task interactions, exercise responses, content annotations, reading assignments, and 
performance metrics spanning multiple academic subjects and courses with temporal learning analytics.

**Dataset ID \-** openstax_tutor_2018-2024

**Owner/Maintainer Name \-** OpenStax Research Data Team

**Owner/Maintainer Contact \-** research@openstax.org

**Owner/Maintainer ORC ID/OSF ID (optional)** \- TBD

**DLP Research Organization Registry (ROR) ID (Optional)** \- https://ror.org/05qghxh33

**Last Updated \-** 2024-12-30

**Update cadence** \- term (semester-based updates with daily incremental processing)

**Version** \- v1.0.0

**Data Format** \- Parquet files (compressed columnar format optimized for analytics)

**Accessing Data within Enclave** \- Instructions to be developed with the Rice SafeInsights Team

**Demo/Sample Access \-** Yes \- 1000-row sample datasets available for preview and schema exploration

**Documentation Quality \-** Comprehensive

**Cost Estimation for Queries** \- TBD

* Control costs by using date filters on `created_at` columns (data is partitioned by date)
* Cost control tips \- use date ranges, select specific columns only, limit to specific courses/periods/semesters/academic years



## **Data Characteristics**

### **Table/File Inventory**

| Table/File Name | Description | Record Count | Primary Keys | Related Tables |
| ----- | ----- | ----- | ----- | :---- |
| `tasks_task_steps` | Individual steps within tasks (exercises, readings) | ~500M | `id` | `tasks_tasks`, `tasks_tasked_exercises`, `content_pages` |
| `tasks_tasked_exercises` | Exercise responses and grading | ~200M | `id` | `tasks_task_steps`, `content_exercises` |
| `tasks_tasks` | Student assignments and task metadata | ~50M | `id` | `tasks_task_steps`, `tasks_task_plans`, `course_profile_courses` |
| `content_notes` | Student highlights and annotations | ~10M | `id` | `content_pages`, `entity_roles` |
| `content_pages` | Textbook pages and content structure | ~100K | `id` | `content_books`, `tasks_task_steps` |
| `content_books` | Textbook metadata and structure | ~500 | `id` | `content_pages`, `content_ecosystems` |
| `entity_roles` | User roles and research identifiers | ~5M | `id` | `user_profiles`, `course_membership_students` |
| `course_membership_students` | Student enrollment and course participation | ~5M | `id` | `entity_roles`, `course_profile_courses` |
| `course_membership_periods` | Class periods within courses | ~50K | `id` | `course_profile_courses`, `course_membership_students` |
| `course_profile_courses` | Course definitions and metadata | ~25K | `id` | `catalog_offerings`, `content_ecosystems` |
| `openstax_accounts_accounts` | User account information | ~3M | `id` | `user_profiles` |
| `user_profiles` | User profile data | ~3M | `id` | `entity_roles`, `openstax_accounts_accounts` |

### 

### **Schema Information**

#### **Table \- `tasks_task_steps`**

**Description \-** Individual learning steps within student tasks including exercises, reading assignments, videos, and interactive content. Each step represents a granular learning activity with completion timestamps and performance tracking.

**Primary Use \-** Learning analytics, step completion analysis, time-on-task measurement, learning sequence analysis

**Sample data**

* Real sample \- Yes
* Synthetic sample \- No

**Link to sample data** \- ToDo

## **Data Lineage and Dependencies**

**Source Systems \-** OpenStax Tutor production platform (tutor.openstax.org), serving K-12 and college students globally with primary usage in United States educational institutions

**Transformation Process \-** Daily extraction from PostgreSQL production database, anonymization of PII, conversion to parquet format with date-based partitioning, quality validation, and research identifier assignment

**Dependencies \-** OpenStax Accounts system, OpenStax CNX content repository, institutional LMS integrations, course management systems

**Downstream Usage \-** Learning analytics research, adaptive learning algorithm development, educational effectiveness studies, institutional reporting dashboards

**AI features** \- Yes (adaptive question selection, personalized practice recommendations, automated content tagging)

**Example research questions (When possible)**
- How does time-on-task correlate with learning outcomes across different subjects?
- What content annotation patterns predict student success?
- How effective are different assignment types in promoting retention?
- What factors influence course completion rates?

| Column Name | Data Type | Description | Example Values | Null handling | Primary Key | Foreign Key |
| ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| `id` | integer | Unique step identifier | 1, 2, 3, 4567 | Not null | Yes | No |
| `tasks_task_id` | integer | Reference to parent task | 1001, 1002, 1003 | Not null | No | `tasks_tasks.id` |
| `tasked_id` | integer | Polymorphic reference to step content | 501, 502, 503 | Not null | No | Various (exercises, readings, etc.) |
| `tasked_type` | varchar | Type of learning content | Tasks::Models::TaskedExercise, Tasks::Models::TaskedReading | Not null | No | No |
| `number` | integer | Step sequence within task | 1, 2, 3, 4 | Not null | No | No |
| `first_completed_at` | datetime | First completion timestamp | 2023-09-15 14:30:00 | NULL if not completed | No | No |
| `last_completed_at` | datetime | Most recent completion timestamp | 2023-09-15 16:45:00 | NULL if not completed | No | No |
| `group_type` | integer | Step grouping category (0=core, 1=spaced_practice, 2=personalized) | 0, 1, 2 | Defaults to 0 | No | No |
| `content_page_id` | integer | Associated content page | 201, 202, 203 | NULL for non-content steps | No | `content_pages.id` |
| `is_core` | boolean | Whether step is required core content | true, false | Not null | No | No |
| `created_at` | datetime | Record creation timestamp | 2023-09-01 08:00:00 | Not null | No | No |
| `updated_at` | datetime | Record last update timestamp | 2023-09-15 16:45:00 | Not null | No | No |

**File Formats Provided**

* CSV/Excel - Yes (via R/Python export scripts)
* JSON/XML - No
* Database formats - Yes (Parquet optimized for analytics)
* Video formats - No
* Audio formats - No
* Image formats - No
* Text formats - Yes (JSON fields containing exercise content and responses)

**Temporal Granularity Options**

* Real-time/continuous - No
* Per interaction/click - Yes (step completions, exercise submissions)
* Per session/class - Yes (aggregated via task completion data)
* Daily - Yes (daily export and partitioning)
* Weekly - Yes (aggregatable from daily data)
* Monthly - Yes (aggregatable from daily data)
* Semester/term - Yes (course-based temporal boundaries)
* Academic year - Yes (multi-semester analysis possible)
* Multi-year - Yes (historical data spanning multiple years)

**Unit of Analysis Levels**

* Individual learner - Yes (student-level learning analytics)
* Event level - Yes (individual step completions, exercise responses)
* Learning group/team - Yes (via period/section membership)
* Classroom/cohort - Yes (course membership period analysis)
* Course/program - Yes (course-level aggregations)
* Institution - Yes (school-level analysis via course data)
* District/system - No (school district data not systematically tracked)
* Regional/national - No (geographic aggregation not available)

## **Unit-of-Analysis Map** 

*Provide one row per entity.*

| entity\_name | primary\_key | parent\_entity | can\_be\_missing (Yes/No) | expected\_cardinality (per parent) |
| ----- | ----- | ----- | ----- | ----- |
| student | entity\_role\_id | — | No | — |
| course | course\_profile\_course\_id | — | No | — |
| period | course\_membership\_period\_id | course | No | many |
| task | tasks\_task\_id | student | No | many |
| step | tasks\_task\_steps\_id | task | No | many |
| exercise | tasks\_tasked\_exercise\_id | step | Yes | one |
| content\_page | content\_page\_id | — | No | — |
| content\_book | content\_book\_id | — | No | — |
| content\_note | content\_notes\_id | student | Yes | many |
| research\_study | research\_study\_id | — | Yes | — |

Note. Research studies represent controlled experiments or A/B tests conducted within the platform for educational research purposes.

## **Technical Specifications**

**Dataset Size and Complexity**

* Number of Participants - XM+ unique students across institutional and individual accounts
* Number of Variables - 800+ columns across 50+ core tables
* Number of Records - 800M+ total records across all tables
* Data Volume - X GB (compressed parquet), X GB (uncompressed)
* Missing Data Percentage - XX% overall (varies by table, higher in optional features)
* Contextual Notes - COVID-19 remote learning surge (2020-2021), adaptive algorithm updates (2022), LMS integration expansion (2023)

### **Data Structure**

* Total Tables/Files - 88 tables in full schema, 12 core tables in research export
* Total Records - XXM+ across all tables (XXM+ task steps, XXM+ exercises)
* Dataset Size - XX GB compressed parquet format
* Update Frequency - Daily (incremental), Full refresh quarterly
* Partitioning - Date-based partitioning by created_at, clustered by course and student

**Temporal Coverage**

* Date Range - 201X-XX-XX to 2024
* Academic Years - 201X through 2024
* Reporting Periods - Semester-based with daily granularity

#### **Table-Specific Statistics**

**Table - `tasks_task_steps`**

* Total Records - XXM+ steps
* Unique Entities - XX+ students, XXX+ courses, XXX+ content pages
* Time Period - 201X-0X-0X to 2024-0X-0X
* Key Numerical Variables -
  * step number - Min: 1, Max: 50+, Mean: 8.5, Median: 7, Missing: 0%
  * completion time (minutes) - Min: 0.1, Max: 240, Mean: 12.3, Median: 6.8, Missing: 35%
* Key Categorical Variables -
  * tasked_type - TaskedExercise: 60%, TaskedReading: 30%, TaskedVideo: 8%, Other: 2%
  * group_type - Core: 70%, Spaced Practice: 20%, Personalized: 10%
  * is_core - True: 70%, False: 30%

### **Data Availability**

**Privacy Level**

* Fully anonymized (All identifying details have been removed) - No
* De-identified (Direct identifiers have been removed) - No
* Pseudonymized (Direct identifiers have been replaced with pseudonyms) - Yes
* Identifiable (restricted) - No
* Other - No

### **Query Example**

#### **Single Table Queries**

```sql
-- Task step completion analysis
SELECT 
    tasked_type,
    COUNT(*) as total_steps,
    COUNT(first_completed_at) as completed_steps,
    AVG(EXTRACT(EPOCH FROM (last_completed_at - created_at))/60) as avg_time_minutes
FROM tasks_task_steps 
WHERE created_at >= '2023-01-01'
GROUP BY tasked_type;
```

#### **Multi-Table Queries**

```sql
-- Student performance with book titles (recreated from R script logic)
SELECT 
    er.research_identifier,
    cb.title as book_title,
    cp.title as section_title,
    COUNT(tts.id) as total_steps,
    COUNT(tts.first_completed_at) as completed_steps,
    AVG(tte.published_grader_points) as avg_grade
FROM tasks_task_steps tts
INNER JOIN tasks_tasks tt ON tt.id = tts.tasks_task_id
INNER JOIN tasks_taskings tsg ON tsg.tasks_task_id = tt.id
INNER JOIN entity_roles er ON er.id = tsg.entity_role_id
LEFT JOIN content_pages cp ON cp.id = tts.content_page_id
LEFT JOIN content_books cb ON cb.id = cp.content_book_id
LEFT JOIN tasks_tasked_exercises tte ON tte.id = tts.tasked_id 
    AND tts.tasked_type = 'Tasks::Models::TaskedExercise'
WHERE tt.created_at BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY er.research_identifier, cb.title, cp.title;
```

#### **Data preprocessing example**

```r
# R script example based on recreate_tutor_query_optimized.R
library(tidyverse)
library(arrow)
library(lubridate)

# Load task steps data with date filtering
BASE_PATH <- "~/data/tutor"
task_steps_ds <- open_dataset(file.path(BASE_PATH, "public.tasks_task_steps"))

# Filter and process task steps data
task_steps_filtered <- task_steps_ds %>%
  filter(created_at >= as_datetime("2023-01-01")) %>%
  collect() %>%
  mutate(
    completion_time_minutes = as.numeric(
      difftime(last_completed_at, first_completed_at, units = "mins")
    ),
    is_completed = !is.na(first_completed_at)
  )

# Basic completion statistics
completion_stats <- task_steps_filtered %>%
  group_by(tasked_type) %>%
  summarise(
    total_steps = n(),
    completed_steps = sum(is_completed),
    completion_rate = mean(is_completed),
    avg_time_minutes = mean(completion_time_minutes, na.rm = TRUE)
  )
```

#### **Table \- `[table_name_2]`**

**Description \-** Detailed description of what this table contains

**Primary Use \-** Main purpose/analysis this table supports

| Column Name | Data Type | Description | Example Values | Nullable | Primary Key | Foreign Key |
| ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| `(field_name)` | `(varchar/int/date/etc)` | Clear description | `(Sample values)` | (Yes/No) | (Yes/No) | `(Reference if applicable)` |

**Temporal Coverage \-** 

* Date Range \- (YYYY-MM-DD to YYYY-MM-DD)  
* Academic Years \- (e.g., 2020-21 through 2023-24)  
* Reporting Periods \- (Annual/Semester/Quarter/Monthly/Other (TEXT))

**Geographic Coverage \-** 

* Geographic Scope \- (National/State/District/School level)  
* Number of Regions \- (Count of regions covered)  
* Coverage Gaps \- (Any notable geographic exclusions)

#### **Table-Specific Statistics**

**Table \- `[table_name_2]`**

* Total Records \- (Exact count)  
* Unique Entities \- (e.g., students, schools, districts)  
* Time Period \- (Specific period this table covers)  
* Key Numerical Variables \-   
  * (Variable Name) \- Min \- (X), Max \- (Y), Mean \- (Z), Median \- (W), Missing \- (N%)  
* Key Categorical Variables \-   
  * (Variable Name) \- (Category 1 \- N records (X%), Category 2 \- N records (Y%))

### **Table Relationships**

#### **Join Patterns**

* `tasks_task_steps` connects to `tasks_tasks` via `tasks_task_id`
* `tasks_task_steps` connects to `content_pages` via `content_page_id`
* `content_pages` connects to `content_books` via `content_book_id`
* `tasks_taskings` connects to `entity_roles` via `entity_role_id`
* `entity_roles` connects to `user_profiles` via `user_profile_id`
* `course_membership_students` connects to `course_profile_courses` via `course_profile_course_id`

#### **Cross-Table Analysis Notes**

* XX%+ task steps have valid content page references for reading/exercise content
* Course enrollment completeness varies by semester (XX-XX% complete records)
* Temporal alignment\- all tables share created_at/updated_at for time-based analysis
* User profiles maintain referential integrity with XX% join success rate

#### **Common Multi-Table Queries**

* Student learning progressions across courses and time periods
* Content effectiveness analysis by book/chapter/exercise performance
* Course comparison analysis with completion rates and engagement metrics
* Longitudinal student performance tracking across semesters

### **Data Quality Indicators**

* Completeness - XX% (high for core learning data, lower for optional features like notes)
* Accuracy - High quality with automated validation, <X% data inconsistencies identified
* Consistency - Standardized schemas with foreign key constraints, temporal consistency enforced
* Uniqueness - Primary key constraints prevent duplicates, research identifiers unique per student
* Timeliness - Daily updates with <24 hour latency, real-time for active learning sessions 
