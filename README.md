# Political-Tweets-ETL
Political-Tweets ETL is a FastAPI-based application that extracts political tweets, classifies their emotional tone using a Hugging Face model, optionally detects stance using a lightweight LLM, and stores everything in Elasticsearch for easy retrieval and analysis.

## Data Analysis (`analisis_datos.ipynb`)
The Jupyter notebook `analisis_datos.ipynb` performs several steps to clean, analyze, and visualize the tweet data.

### 1. Importing Libraries
The notebook begins by importing necessary Python libraries for data manipulation and visualization:
- `pandas` for data handling.
- `matplotlib.pyplot` for plotting.
- `seaborn` for enhanced visualizations.

### 2. Data Loading
- Tweet data is loaded from the `tweets_dataset.json` file into a pandas DataFrame.

### 3. Data Transformation
- The nested JSON structure of the raw data is flattened.
- Relevant columns are selected: `id`, `user.handle`, `meta.created_at`, `meta.hashtags`, and `payload.tweet.content`.
- These columns are renamed for clarity and ease of use (e.g., `user.handle` to `handle`, `meta.created_at` to `created_at`, `meta.hashtags` to `hashtags`, `payload.tweet.content` to `content`).

### 4. Initial Data Exploration
- The data types of the columns are checked.
- The dataset is inspected for missing (null) values.

### 5. Data Cleaning
- It's confirmed that there are no null values in the selected columns.
- The `created_at` column is converted from a string format to a datetime object, which facilitates time-based analysis.

### 6. Data Visualization
- **Hashtag Analysis**:
    - The occurrences of each hashtag are counted.
    - A bar plot is generated to visualize the frequency of the most common hashtags.

### 7. Tweet Length Analysis
- The length of the content for each tweet is calculated.
- Statistical measures (mean, minimum, maximum) of tweet lengths are computed and displayed.
- A histogram is plotted to show the distribution of tweet lengths.

### 8. Data Export
- The cleaned and processed DataFrame is exported to a CSV file named `tweets_dataset.csv`.
- **Note**: The notebook encountered a `ModuleNotFoundError: No module named 'pandas.io.formats.csvs'` during this step, which may indicate an issue with the pandas version or environment setup that needs to be resolved for the export to succeed.
