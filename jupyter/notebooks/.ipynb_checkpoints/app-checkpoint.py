import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import numpy as np
import time
import requests

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
    
    </style>
    """,
    unsafe_allow_html=True
)

# Setup Spark (giữ nguyên cấu hình bạn có)
@st.cache_resource
def get_spark():
    return SparkSession.builder \
    .appName("HybridRecommenderMaxPerformance") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "1") \
    .config("spark.default.parallelism", "8") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "4g") \
    .config("spark.jars", "jars/hadoop-aws-3.3.4.jar,jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,jars/aws-java-sdk-bundle-1.12.262.jar,jars/delta-core_2.12-2.2.0.jar,jars/delta-storage-2.2.0.jar") \
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

# Cache poster TMDB để giảm gọi API
@st.cache_data(show_spinner=False)
def get_poster_url(movie_id: int) -> str:
    headers = {
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmODFmNGU3ODhhZjU0NzVkMzg4ZDIxMzRiMmZlZGE2NiIsIm5iZiI6MTczMTkzNzAwMS4xOTkwMDAxLCJzdWIiOiI2NzNiNDJlOTgzYjY2NmE0ZTlhMmQ3NmMiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.Yp5aH_C1KA4iflmeDGQ7JrWi1NfwlKzRRUT20vJs47s",
        "accept": "application/json"
    }
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/images"
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            posters = response.json().get("posters", [])
            if posters:
                return f"https://image.tmdb.org/t/p/w500{posters[0]['file_path']}"
    except:
        return None
    return None

# st.title("🎬 Movie Recommendation System")
# st.title("🎬 Movie Recommendation System")  # Bỏ dòng này

st.markdown(
    """
    <h1 style="font-weight:700; font-size:2.5rem; color:#FFFFFF; text-shadow:0.5px 0.5px 1px rgba(0,0,0,0.2);">
        🎬 Movie Recommendation System
    </h1>
    """,
    unsafe_allow_html=True
)



df_merge = spark.read.format("delta").load("s3a://lakehouse/gold/MergeData").cache()

tab1, tab2 = st.tabs(["🎥 Recommend by Movie Name", "📂 Browse by Genre"])

with tab1:
    with st.container():
        movie_name = st.text_input("Enter movie title to get recommendations:")
        # st.markdown('<label style="font-size:20px; color:#FFFFFF; font-weight:600;">Enter movie title to get recommendations:</label>', unsafe_allow_html=True)
        # movie_name = st.text_input("")

        if movie_name:
            start_time = time.time()
            movie_df = df_merge.filter(F.col("title") == movie_name).select(
                "id", "title", "genres", "director_names", "cast_names",
                "popularity", "vote_average", "vote_count"
            ).limit(1)

            if movie_df.count() == 0:
                st.markdown(
                    """
                    <div style="color: #b22222; font-weight: 900;margin-bottom:1rem; font-size: 32px !important; border-radius: 5px;">
                        🚫 Movie not found in database.
                    </div>
                    """,
                    unsafe_allow_html=True
                )


            else:
                movie_info = movie_df.collect()[0]
                m_id = movie_info['id']

                st.subheader("🎥 Input Movie Info")
                left, right = st.columns([1, 2])
                poster_url = get_poster_url(m_id)
                with left:
                    if poster_url:
                        st.image(poster_url, use_container_width=True)
                with right:
                    st.markdown(f"**Title:** {movie_info['title']}")
                    st.markdown(f"**Genres:** {movie_info['genres']}")
                    st.markdown(f"**Directors:** {movie_info['director_names']}")
                    st.markdown(f"**Cast:** {movie_info['cast_names']}")
                    st.markdown(f"**Popularity:** {movie_info['popularity']}")
                    st.markdown(f"**Vote Average:** {movie_info['vote_average']}")
                    st.markdown(f"**Vote Count:** {movie_info['vote_count']}")

                # Tính cosine similarity
                df_list = spark.read.format("delta").load("s3a://lakehouse/data/bert").cache()
                input_vec = df_list.filter(F.col("id") == m_id).select("vecs").collect()[0][0]
                broadcast_input_vec = spark.sparkContext.broadcast(np.array(input_vec))

                def cosine_similarity(v1, v2):
                    v1 = np.array(v1)
                    v2 = np.array(v2)
                    numerator = float(np.dot(v1, v2))
                    denominator = float(np.linalg.norm(v1) * np.linalg.norm(v2))
                    return numerator / denominator if denominator != 0 else 0.0

                cosine_udf = F.udf(lambda vec: float(cosine_similarity(broadcast_input_vec.value, vec)), FloatType())

                recommendations_spark_df = df_list \
                    .withColumn("score", cosine_udf(F.col("vecs"))) \
                    .filter(F.col("id") != m_id) \
                    .select(F.col("id").alias("movies_id"), "score") \
                    .orderBy(F.col("score").desc()) \
                    .limit(5)

                recommendations_spark_df = recommendations_spark_df.join(
                    df_merge,
                    recommendations_spark_df["movies_id"] == df_merge["id"],
                    "inner"
                )

                st.subheader("🎯 Top 5 Recommended Movies")

                pandas_df = recommendations_spark_df.select(
                    "title", "genres", "director_names", "cast_names",
                    "popularity", "vote_average", "vote_count", "score", "id"
                ).orderBy(F.col("score").desc()).limit(5).toPandas()

                cols = st.columns(5)

                for i, col in enumerate(cols):
                    movie = pandas_df.iloc[i]
                    poster_url = get_poster_url(movie["id"])
                    with col:
                        if poster_url:
                            st.image(poster_url, use_container_width=True)
                        st.markdown(f"**Title:** {movie['title']}")
                        st.markdown(f"**Genres:** {movie['genres']}")
                        st.markdown(f"**Director:** {movie['director_names']}")
                        st.markdown(f"**Cast:** {movie['cast_names']}")

                # st.success(f"✅ Completed in {time.time() - start_time:.2f} seconds")

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
                        st.image(poster_url, use_container_width=True)
                    st.markdown(f"**{movie['title']}**")
                    st.markdown(f"Rating: {movie['vote_average']}, Votes: {movie['vote_count']}")
                    st.markdown(f"Popularity Score: {movie['weighted_rating']:.2f}")
