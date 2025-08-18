import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import numpy as np
import requests
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, lit
import faiss
from sparknlp.base import LightPipeline
from pyspark.ml import PipelineModel

import logging
import streamlit as st
import requests
import numpy as np

st.markdown(
    """
    <style>

    .stApp {
        position: relative;
        background: transparent !important;
        color: #F5F5F5;
        min-height: 100vh;
    }

    .stApp::before {
        content: "";
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background-image: url("https://www.notebookcheck.net/fileadmin/Notebooks/News/_nc3/netflixteaser.png");
        background-repeat: no-repeat;
        background-position: center center;
        background-size: cover;
        filter: blur(3px);
        z-index: -1;
    }
    .st-emotion-cache-1w723zb {
        width: 100%;
        padding: 6rem 1rem 10rem;
        max-width: 900px;
    }
    .st-emotion-cache-1dp5vir {
        background: none !important;
    }

    .st-emotion-cache-1eyfjps {
        background: transparent !important;
    }

    .st-emotion-cache-1r4qj8v, .stTabs, .stTextInput, .stSelectbox {
        background: rgb(45 41 41 / 50%);
        border-radius: 8px;
        padding: 0.8rem;
        position: relative;
        z-index: 1;
    }

    img {
        border-radius: 8px;
        transition: transform 0.3s ease;
        max-width: 100%;
        border: 1px solid #FFE680;
    }

    img:hover {
        transform: scale(1.03);
        box-shadow: 0 2px 8px rgba(255, 230, 128, 0.4);
    }

    h1, h2, h3, h4 {
        color: #FFE680;
        text-shadow: 0.5px 0.5px 1px rgba(0, 0, 0, 0.2);
        font-weight: 500;
    }

    p, div, span, label {
        text-shadow: 0.5px 0.5px 1px rgba(0, 0, 0, 0.15);
        font-weight: 400;
    }

    ::-webkit-scrollbar {
        width: 6px;
    }

    ::-webkit-scrollbar-thumb {
        background: #FFE680;
        border-radius: 8px;
    }

    .st-emotion-cache-1wivap2 {
        padding: 0 !important;
    }

    .stTextInput > div > div > input,
    .stSelectbox > div > div > select {
        background: rgba(255, 255, 255, 0.05);
        # color: #F5F5F5;
        border: 1px solid #FFE680;
        border-radius: 4px;
    }

    [data-testid="stTextInput"] label,
    [data-testid="stSelectbox"] label {
        color: #FFFFFF !important;      
    }

    .stTabs > div > div > div > button {
        # background: rgba(255, 255, 255, 0.1);
        color: #FFE680;
        border-radius: 4px;
        margin-right: 0.4rem;
        font-weight: 400;
    }

    .stTabs > div > div > div > button:hover {
        background: rgba(255, 230, 128, 0.2);
        color: #F5F5F5;
    }
    .css-pxxe24 {
visibility: hidden;
}

    .stButton>button {
        background-color: #4CAF50; /* Màu nền xanh lá cây */
        color: white; /* Màu chữ trắng */
        border-radius: 5px; /* Bo tròn góc */
        border: none; /* Không có viền */
        font-size: 16px; /* Kích thước chữ */
        cursor: pointer; /* Con trỏ chuột biến thành hình bàn tay */
        transition: background-color 0.3s ease; /* Hiệu ứng chuyển màu nền */
    }

    .stButton>button:hover {
        background-color: #45a049; /* Màu nền khi hover (di chuột vào) */
    }


    
    </style>
    """,
    unsafe_allow_html=True
)

# Setup Spark (keep the Spark setup as it is)
@st.cache_resource
def get_spark():
    return SparkSession.builder \
    .appName("HybridRecommenderMaxPerformance") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "1") \
    .config("spark.default.parallelism", "8") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "4g") \
    .config("spark.jars", "jars/spark-nlp_2.12-5.1.3.jar,jars/onnxruntime-1.16.3.jar,jars/hadoop-aws-3.3.4.jar,jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,jars/aws-java-sdk-bundle-1.12.262.jar,jars/delta-core_2.12-2.2.0.jar,jars/delta-storage-2.2.0.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
    .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("delta.enable-non-concurrent-writes", "true") \
    .config("spark.sql.warehouse.dir", "s3a://lakehouse/") \
    .getOrCreate()


spark = get_spark()
# ratings = spark.read.format("delta").load("s3a://lakehouse/silver/ratings")
# df_list = spark.read.format("delta").load("s3a://lakehouse/data/bertTestHi")
# df_merge = spark.read.format("delta").load("s3a://lakehouse/gold/MergeData")
# CF = ALSModel.load("s3a://lakehouse/data/CF_als_best_model")
# nlp = PipelineModel.load("s3a://lakehouse/bert_pipeline1000")
# vec_df = spark.read.format("delta").load("s3a://lakehouse/data/bert2")
# machineDataBert = spark.read.format("parquet").load("s3a://lakehouse/gold/machineDataBert")
# light_pipeline = LightPipeline(nlp, parse_embeddings=True)
# all_movies = ratings.select('movieId').distinct()

@st.cache_resource
def load_everything():
    ratings = spark.read.format("delta").load("s3a://lakehouse/silver/ratings")
    df_list = spark.read.format("delta").load("s3a://lakehouse/data/bertTestHi")
    df_merge = spark.read.format("delta").load("s3a://lakehouse/gold/MergeData")
    vec_df = spark.read.format("delta").load("s3a://lakehouse/data/bert2")
    machineDataBert = spark.read.format("parquet").load("s3a://lakehouse/gold/machineDataBert")

    CF = ALSModel.load("s3a://lakehouse/data/CF_als_best_model")
    nlp = PipelineModel.load("s3a://lakehouse/bert_pipeline1000")
    light_pipeline = LightPipeline(nlp, parse_embeddings=True)

    all_movies = ratings.select('movieId').distinct()

    return {
        "ratings": ratings,
        "df_list": df_list,
        "df_merge": df_merge,
        "vec_df": vec_df,
        "machineDataBert": machineDataBert,
        "CF": CF,
        "nlp": nlp,
        "light_pipeline": light_pipeline,
        "all_movies": all_movies
    }

data = load_everything()
ratings = data["ratings"]
df_list = data["df_list"]
df_merge = data["df_merge"]
vec_df = data["vec_df"]
machineDataBert = data["machineDataBert"]
CF = data["CF"]
nlp = data["nlp"]
light_pipeline = data["light_pipeline"]
all_movies = data["all_movies"]

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@st.cache_data(show_spinner=False)
def get_poster_url(movie_id: int) -> str:
    headers = {
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmODFmNGU3ODhhZjU0NzVkMzg4ZDIxMzRiMmZlZGE2NiIsIm5iZiI6MTczMTkzNzAwMS4xOTkwMDAxLCJzdWIiOiI2NzNiNDJlOTgzYjY2NmE0ZTlhMmQ3NmMiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.Yp5aH_C1KA4iflmeDGQ7JrWi1NfwlKzRRUT20vJs47s",
        "accept": "application/json"
    }
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/images"
    try:
        response = requests.get(url, headers=headers, timeout=10)  # Tăng timeout lên 10 giây
        logger.info(f"Requesting poster for movie_id {movie_id}, status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            posters = data.get("posters", [])
            if posters:
                poster_path = posters[0]['file_path']
                logger.info(f"Poster found for movie_id {movie_id}: {poster_path}")
                return f"https://image.tmdb.org/t/p/w500{poster_path}"
            else:
                logger.warning(f"No posters found for movie_id {movie_id}")
        else:
            logger.error(f"API request failed for movie_id {movie_id}, status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error fetching poster for movie_id {movie_id}: {str(e)}")
    
    default_image = "https://static.vecteezy.com/system/resources/previews/004/141/669/non_2x/no-photo-or-blank-image-icon-loading-images-or-missing-image-mark-image-not-available-or-image-coming-soon-sign-simple-nature-silhouette-in-frame-isolated-illustration-vector.jpg"
    logger.info(f"Returning default image for movie_id {movie_id}")
    return default_image


st.markdown(
    """
    <h1 style="font-weight:700; font-size:2.5rem; color:#FFFFFF; text-shadow:0.5px 0.5px 1px rgba(0,0,0,0.2);">
        🎬 Movie Recommendation System
    </h1>
    """,
    unsafe_allow_html=True
)

rows = vec_df.select("id", "vecs").rdd \
    .map(lambda row: (row["id"], np.array(row["vecs"], dtype=np.float32))) \
    .collect()

ids = [r[0] for r in rows]
vectors = np.stack([r[1] for r in rows])
norms = np.linalg.norm(vectors, axis=1, keepdims=True)
normalized_vectors = vectors / norms

# 5️⃣ Build FAISS Index
index = faiss.IndexFlatIP(normalized_vectors.shape[1])
index.add(normalized_vectors)

def cosine_similarity(v1, v2):
    v1 = np.array(v1)
    v2 = np.array(v2)
    numerator = float(np.dot(v1, v2))
    denominator = float(np.linalg.norm(v1) * np.linalg.norm(v2))
    return numerator / denominator if denominator != 0 else 0.0


def has_rated_history(user_id):
    user_ratings = ratings.filter(ratings['userId'] == user_id).select('movieId')
    return user_ratings.count() > 0

def get_user_rated_movies(user_id,user_ratings):
    movies_with_correct_column = df_merge.withColumnRenamed("id", "movieId") 
    user_rated_movies = user_ratings.join(movies_with_correct_column, 'movieId').select("title", "genres", "director_names", "cast_names","popularity", "vote_average", "vote_count", "movieId")
    return user_rated_movies


def recommend_with_CF(user_id):
    single_user_ratings = ratings.filter(ratings['userId'] == user_id).select(['movieId', 'userId', 'rating'])
    #all_movies = ratings.select('movieId').distinct()
    user_movies = single_user_ratings.select('movieId').distinct()

    movies_to_recommend = all_movies.subtract(user_movies)
    recommendations = CF.transform(movies_to_recommend.withColumn('userId', lit(user_id)))
    recommendations = recommendations.filter(col('prediction') > 3.7) \
                                 .orderBy(col('prediction').desc()) 
                                 # .limit(10)
    movies_with_correct_column = df_merge.withColumnRenamed("id", "movieId")
    top_movie_ids = [row['movieId'] for row in recommendations.collect()]
    # recommended_movies = recommendations.join(movies_with_correct_column, 'movieId').select(
    #     "title", "genres", "director_names", "cast_names", "popularity", 
    #     "vote_average", "vote_count", "prediction", "movieId"
    # )
    recommended_movies = movies_with_correct_column.filter(col('movieId').isin(top_movie_ids))
    rated_movies = single_user_ratings.join(df_list, single_user_ratings.movieId == df_list.id)
    user_vec = rated_movies.select('vecs').rdd.map(lambda row: np.array(row['vecs'])).mean()
    broadcast_user_vec = spark.sparkContext.broadcast(user_vec)
    def cosine_similarity(v1, v2):
        v1 = np.array(v1)
        v2 = np.array(v2)
        numerator = float(np.dot(v1, v2))
        denominator = float(np.linalg.norm(v1) * np.linalg.norm(v2))
        return numerator / denominator if denominator != 0 else 0.0
    cosine_udf = F.udf(lambda vec: float(cosine_similarity(broadcast_user_vec.value, vec)), FloatType())
    cb_filtered = recommended_movies.join(df_list, recommended_movies.movieId == df_list.id)
    cb_filtered = cb_filtered.withColumn('similarity', cosine_udf(F.col('vecs')))
    cb_filtered = cb_filtered.orderBy(F.col('similarity').desc())
    return cb_filtered.select(
        "title", "genres", "director_names", "cast_names", 
        "popularity", "vote_average", "vote_count", 
        "similarity", "movieId"
    )


def recommend_with_FAISS(user_query):
    result = light_pipeline.fullAnnotate(user_query)
    query_vector = np.array(result[0]['sentence_embeddings'][0].embeddings, dtype=np.float32)
    query_vector /= np.linalg.norm(query_vector)
    query_vector = query_vector.reshape(1, -1)

    k = 10
    scores, indices = index.search(query_vector, k)

    matched_ids = [int(ids[idx]) for idx in indices[0]]
    result_data = [(matched_ids[i], float(scores[0][i])) for i in range(len(matched_ids))]
    result_df = spark.createDataFrame(result_data, ["movie_id", "cosine_score"])

    # Filter trước để giảm kích thước df_merge
    filtered_df = df_merge.filter(F.col("id").isin(matched_ids))

    # Join nhẹ để thêm cosine_score
    final_df = filtered_df.join(result_df, filtered_df["id"] == result_df["movie_id"], "inner") \
        .select("title", "genres", "director_names", "cast_names", "popularity",
                "vote_average", "vote_count", "id", "cosine_score") \
        .orderBy(F.col("cosine_score").desc())

    return final_df

def recommend_with_CB(movie_name):
    movie_df = df_merge.filter(F.col("title") == movie_name).select("id").limit(1)
    if movie_df.count() == 0:
        return recommend_with_FAISS(movie_name)
    else:
        m_id = movie_df.collect()[0]['id']
        input_vec = df_list.filter(F.col("id") == m_id).select("vecs").collect()[0][0]
        broadcast_input_vec = spark.sparkContext.broadcast(np.array(input_vec))
        def cosine_similarity(v1, v2):
            v1 = np.array(v1)
            v2 = np.array(v2)
            numerator = float(np.dot(v1, v2))
            denominator = float(np.linalg.norm(v1) * np.linalg.norm(v2))
            return numerator / denominator if denominator != 0 else 0.0

        cosine_udf = F.udf(lambda vec: float(cosine_similarity(broadcast_input_vec.value, vec)), FloatType())
        recommendations_spark_df = df_list.withColumn("score", cosine_udf(F.col("vecs"))).filter(F.col("id") != m_id)
        recommendations_spark_df = recommendations_spark_df.select(F.col("id").alias("movies_id"), "score").orderBy(F.col("score").desc()).limit(10)
        recommendations_spark_df = recommendations_spark_df.join(df_merge, F.col("movies_id") == F.col("id"), 'inner')
        return   recommendations_spark_df.select("title", "genres", "director_names", "cast_names","popularity", "vote_average", "vote_count", "score", "id"    ).orderBy(F.col("score").desc()).limit(10)

    

tab1, tab2 = st.tabs(["🎥 Recommend", "📂 Movie by Genre"])
with tab1:
    with st.container():
        
        user_id_input = st.text_input("Enter your User ID:")
        if user_id_input:
            user_id = int(user_id_input)  # Ensure the user_id is an integer


            if 'cached_user_id' in st.session_state and st.session_state.cached_user_id != user_id:
                st.session_state.pop('user_rated_movies_df', None)
                st.session_state.pop('recommended_page', None)
                st.session_state.pop('rated_page', None)
                st.session_state.pop('cbf_page', None)

            
            user_ratings = ratings.filter(ratings['userId'] == user_id).select('movieId')
            if user_ratings.count() > 0:
                st.markdown(f"👤 User {user_id} has rated movies before! 🎉")
                st.write("🎬 Movies you've rated:")
                if 'user_rated_movies_df' not in st.session_state or st.session_state.get("cached_user_id") != user_id:
                    user_rated_movies = get_user_rated_movies(user_id,user_ratings)
                    st.session_state.user_rated_movies_df = user_rated_movies.toPandas()
                    st.session_state.cached_user_id = user_id  # Để đảm bảo đổi user thì sẽ query lại
                
                pandas_df = st.session_state.user_rated_movies_df  # Dùng lại data đã cache
                #del st.session_state.user_rated_movies_df
               
                # Initialize session state for rated movies pagination (if not already initialized)
                if 'rated_page' not in st.session_state:
                    st.session_state.rated_page = 0
        
                # Number of movies to display per page
                movies_per_page = 5
                total_movies = len(pandas_df)
                total_pages = (total_movies + movies_per_page - 1) // movies_per_page
        
                # Navigation buttons for rated movies
                col_left, col_center, col_right = st.columns([1, 8, 1])
                with col_left:
                    if st.button("⬅️", key="prev_page_rated"):
                        if st.session_state.rated_page > 0:
                            st.session_state.rated_page -= 1
                with col_right:
                    if st.button("➡️", key="next_page_rated"):
                        if st.session_state.rated_page < total_pages - 1:
                            st.session_state.rated_page += 1
        
                # Calculate the start and end indices for the current page
                start_idx = st.session_state.rated_page * movies_per_page
                end_idx = min(start_idx + movies_per_page, total_movies)
                current_movies = pandas_df.iloc[start_idx:end_idx]
        
                # Display rated movies in a row of 5 columns
                cols = st.columns(5)
                for i, col1 in enumerate(cols):
                    if i < len(current_movies):  # Ensure we don't exceed the number of movies on the current page
                        movie = current_movies.iloc[i]
                        poster_url = get_poster_url(movie["movieId"])
                        with col1:
                            if poster_url:
                                st.markdown(f'<img src="{poster_url}" style="height:235px; width:auto; object-fit: cover;"/>', unsafe_allow_html=True)
        
                            # Movie title
                            st.markdown(f"""
                            <div style='width: 150px; height: 70px; overflow: hidden; text-overflow: ellipsis; display: flex; align-items: center; justify-content: center; text-align: center;'>
                                <strong>{movie['title']}</strong>
                            </div>
                            """, unsafe_allow_html=True)
        
                            # Genres
                            st.markdown(f"""
                            <div style='width: 150px; height: 70px; display: flex; align-items: center; justify-content: center; text-align: center;'>Genres: {movie['genres']}</div>
                            """, unsafe_allow_html=True)
        
                            # Director
                            st.markdown(f"""
                            <div style='width: 150px; height: 100px; display: flex; align-items: center; justify-content: center; text-align: center;'>Director: {movie['director_names']}</div>
                            """, unsafe_allow_html=True)
        
                            # Cast
                            st.markdown(f"""
                            <div style='width: 150px; height: 100px; display: flex; align-items: center; justify-content: center; text-align: center;'>Cast: {movie['cast_names']}</div>
                            """, unsafe_allow_html=True)
        
                # Display current page info for rated movies
                st.markdown(f"Page {st.session_state.rated_page + 1} of {total_pages}")    
        
        
                # **NEW PART: Recommended Movies Paging**
                recommended_movies = recommend_with_CF(user_id).toPandas()
                st.write("Top recommended movies based on Collaborative Filtering:")
        
                # Initialize session state for recommended movies pagination (if not already initialized)
                if 'recommended_page' not in st.session_state:
                    st.session_state.recommended_page = 0
        
                recommended_movies_per_page = 5
                total_recommended_movies = len(recommended_movies)
                total_recommended_pages = (total_recommended_movies + recommended_movies_per_page - 1) // recommended_movies_per_page
        
                # Navigation buttons for recommended movies
                col_left_rec, col_center_rec, col_right_rec = st.columns([1, 8, 1])
                with col_left_rec:
                    if st.button("⬅️", key="prev_page_recommended"):
                        if st.session_state.recommended_page > 0:
                            st.session_state.recommended_page -= 1
                with col_right_rec:
                    if st.button("➡️", key="next_page_recommended"):
                        if st.session_state.recommended_page < total_recommended_pages - 1:
                            st.session_state.recommended_page += 1
        
                # Calculate the start and end indices for the current page of recommended movies
                start_idx_rec = st.session_state.recommended_page * recommended_movies_per_page
                end_idx_rec = min(start_idx_rec + recommended_movies_per_page, total_recommended_movies)
                current_recommended_movies = recommended_movies.iloc[start_idx_rec:end_idx_rec]
        
                # Display recommended movies in a row of 5 columns
                cols = st.columns(5)
                for i, col1 in enumerate(cols):
                    if i < len(current_recommended_movies):  # Ensure we don't exceed the number of movies on the current page
                        movie = current_recommended_movies.iloc[i]
                        poster_url = get_poster_url(movie["movieId"])
                        with col1:
                            if poster_url:
                                st.markdown(f'<img src="{poster_url}" style="height:235px; width:auto; object-fit: cover;"/>', unsafe_allow_html=True)
        
                            # Movie title
                            st.markdown(f"""
                            <div style='width: 150px; height: 70px; overflow: hidden; text-overflow: ellipsis; display: flex; align-items: center; justify-content: center; text-align: center;'>
                                <strong>{movie['title']}</strong>
                            </div>
                            """, unsafe_allow_html=True)        
                            
                            # Genres
                            st.markdown(f"""
                            <div style='width: 150px; height: 70px; display: flex; align-items: center; justify-content: center; text-align: center;'>Genres: {movie['genres']}</div>
                            """, unsafe_allow_html=True)
        
                            # Director
                            st.markdown(f"""
                            <div style='width: 150px; height: 100px; display: flex; align-items: center; justify-content: center; text-align: center;'>Director: {movie['director_names']}</div>
                            """, unsafe_allow_html=True)
        
                            # Cast
                            st.markdown(f"""
                            <div style='width: 150px; height: 100px; display: flex; align-items: center; justify-content: center; text-align: center;'>Cast: {movie['cast_names']}</div>
                            """, unsafe_allow_html=True)
        


        
                # Display current page info for recommended movies
                st.markdown(f"Page {st.session_state.recommended_page + 1} of {total_recommended_pages}")

            else:
                st.markdown(f"👤 User {user_id_input} has not rated any movies yet. Let's suggest some based on a movie you like!")
                movie_name = st.text_input("Enter a movie name to base recommendations on:")
                if movie_name:
                    st.subheader(f"🎬 Content-Based Filtering Recommendations for '{movie_name}'")
                    recommended_movies = recommend_with_CB(movie_name).toPandas()
            
                    # Initialize session state for content-based paging
                    if 'cbf_page' not in st.session_state:
                        st.session_state.cbf_page = 0
            
                    cbf_movies_per_page = 5
                    total_cbf_movies = len(recommended_movies)
                    total_cbf_pages = (total_cbf_movies + cbf_movies_per_page - 1) // cbf_movies_per_page
            
                    # Navigation buttons
                    col_left_cbf, col_center_cbf, col_right_cbf = st.columns([1, 8, 1])
                    with col_left_cbf:
                        if st.button("⬅️", key="prev_page_cbf"):
                            if st.session_state.cbf_page > 0:
                                st.session_state.cbf_page -= 1
                    with col_right_cbf:
                        if st.button("➡️", key="next_page_cbf"):
                            if st.session_state.cbf_page < total_cbf_pages - 1:
                                st.session_state.cbf_page += 1
            
                    # Slice the dataframe for the current page
                    start_idx = st.session_state.cbf_page * cbf_movies_per_page
                    end_idx = min(start_idx + cbf_movies_per_page, total_cbf_movies)
                    current_cbf_movies = recommended_movies.iloc[start_idx:end_idx]
            
                    # Display the current page of movies
                    cols = st.columns(5)
                    for i, col in enumerate(cols):
                        if i < len(current_cbf_movies):
                            movie = current_cbf_movies.iloc[i]
                            poster_url = get_poster_url(movie["id"])
                            with col:
                                if poster_url:
                                    st.markdown(f'<img src="{poster_url}" style="height:235px; width:auto; object-fit: cover;"/>', unsafe_allow_html=True)
            
                                # Title
                                st.markdown(f"""
                                <div style='width: 150px; height: 70px; overflow: hidden; text-overflow: ellipsis; display: flex; align-items: center; justify-content: center; text-align: center;'>
                                    <strong>{movie['title']}</strong>
                                </div>
                                """, unsafe_allow_html=True)
            
                                # Genres
                                st.markdown(f"""
                                <div style='width: 150px; height: 70px; display: flex; align-items: center; justify-content: center; text-align: center;'>Genres: {movie['genres']}</div>
                                """, unsafe_allow_html=True)
            
                                # Director
                                st.markdown(f"""
                                <div style='width: 150px; height: 100px; display: flex; align-items: center; justify-content: center; text-align: center;'>Director: {movie['director_names']}</div>
                                """, unsafe_allow_html=True)
            
                                # Cast
                                st.markdown(f"""
                                <div style='width: 150px; height: 100px; display: flex; align-items: center; justify-content: center; text-align: center;'>Cast: {movie['cast_names']}</div>
                                """, unsafe_allow_html=True)
            
            
                    st.markdown(f"Page {st.session_state.cbf_page + 1} of {total_cbf_pages}")

                
 
with tab2:
    with st.container():
        st.subheader("📂 Browse Movies by Genre")

        # Lấy danh sách genres duy nhất
        all_genres = df_merge.select(F.explode(F.col("genres")).alias("genre")).distinct().toPandas()["genre"].tolist()
        selected_genre = st.selectbox("Chọn thể loại phim:", all_genres)

        if selected_genre:
            vote_counts = df_merge.filter(F.col("vote_count").isNotNull()).select("vote_count")
            vote_averages = df_merge.filter(F.col("vote_average").isNotNull()).select("vote_average")

            C = vote_averages.agg(F.mean("vote_average")).collect()[0][0]
            quantiles = vote_counts.approxQuantile("vote_count", [0.7], 0.001)
            m = quantiles[0]

            qualified = df_merge.filter(
                (F.array_contains(F.col("genres"), selected_genre)) & (F.col("vote_count") >= m) & F.col("vote_average").isNotNull()
            )
            qualified = qualified.withColumn("vote_count", F.col("vote_count").cast("int"))
            qualified = qualified.withColumn("vote_average", F.col("vote_average").cast("float"))

            weighted_rating_udf = F.udf(lambda v, R: (v/(v+m))*R + (m/(v+m))*C, FloatType())
            qualified = qualified.withColumn("weighted_rating", weighted_rating_udf(F.col("vote_count"), F.col("vote_average")))

            popular_movies = qualified.orderBy(F.col("weighted_rating").desc()).limit(5)
            popular_pd = popular_movies.toPandas()

            st.write(f"🎬 Top 5 movies in genre: {selected_genre}")
            cols2 = st.columns(5)
            for i, col in enumerate(cols2):
                movie = popular_pd.iloc[i]
                poster_url = get_poster_url(movie["id"])
                with col:
                    if poster_url:
                        # st.image(poster_url, use_container_width=True
                        st.markdown(f'<img src="{poster_url}" style="height:235px; width:auto; object-fit: cover;"/>', unsafe_allow_html=True)
                    # Tên phim
                    st.markdown(f"<div style='width: 150px; height: 70px; overflow: hidden; text-overflow: ellipsis; display: flex; align-items: center; justify-content: center; text-align: center;'><strong>{movie['title']}</strong></div>", unsafe_allow_html=True)
                    
                    # Rating
                    st.markdown(f"<div style='width: 150px; height: 50px; display: flex; align-items: center; justify-content: center; text-align: center;'>Rating: {movie['vote_average']:.3f}</div>", unsafe_allow_html=True)
                    
                    # Votes
                    st.markdown(f"<div style='width: 150px; height: 50px; display: flex; align-items: center; justify-content: center; text-align: center;'>Votes: {movie['vote_count']}</div>", unsafe_allow_html=True)
                    
                    # Popularity Score
                    st.markdown(f"<div style='width: 150px; height: 50px; display: flex; align-items: center; justify-content: center; text-align: center;'>Popularity Score: {movie['weighted_rating']:.2f}</div>", unsafe_allow_html=True)




