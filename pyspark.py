# pyspark - -jars / opt/spark/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar - -packages org.apache.spark: spark-sql-kafka-0-10_2.12: 3.3.2
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
from time import sleep
from socket import gethostname
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col, from_json, json_tuple
import pyspark.sql.functions as psf
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.functions import count
from pyspark.sql.functions import sum, when
from pyspark.sql.functions import rank, desc
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql import functions as F


df = spark.readStream.format("kafka").option('kafka.bootstrap.servers', 'localhost:9092').option('subscribe', 'music').option('startingOffsets', 'earliest').load()

schema = StructType([
    StructField("artist_name", StringType()),
    StructField("track_name", StringType()),
    StructField("track_price", StringType()),
    StructField("collectionPrice", StringType()),
    StructField("country", StringType()),
    StructField("release_date", StringType()),
    StructField("genre", StringType())
])

ds = df.select(col('value').cast('string')).select(from_json('value', schema).alias(
    'json_data')).writeStream.format("memory").queryName("records").start()

spark.sql("select * from records").show()

# +--------------------+
# |           json_data|
# +--------------------+
# |{Alan Cross, , , ...|
# |{Opie & Anthony, ...|
# |{Pete Bruen & Chr...|
# |{Talking Heads, R...|
# |{Nightly, Radiohe...|
# |{Talking Heads, R...|
# |{Wolfgang Muthspi...|
# |{Lift, Radio-Head...|
# |{Nicholas Cole, R...|
# |{Radiohead, Creep...|
# |{Thom Yorke, Hear...|
# |{Talking Heads, R...|
# |{Radiohead, Daydr...|
# |{Radiohead, Burn ...|
# |{Radiohead, Decks...|
# |{Radiohead, True ...|
# |{Radiohead, The N...|
# |{Radiohead, Prese...|
# |{Radiohead, Ident...|
# |{Radiohead, Ful S...|
# +--------------------+
# only showing top 20 rows

spark.sql("SELECT json_data.artist_name, json_data.track_name, json_data.track_price, json_data.release_date, json_data.genre FROM records").show()

# +--------------------+--------------------+-----------+--------------------+--------------------+
# |         artist_name|          track_name|track_price|        release_date|               genre|
# +--------------------+--------------------+-----------+--------------------+--------------------+
# |          Alan Cross|                    |           |2009-09-01T07:00:00Z|Biographies & Mem...|
# |      Opie & Anthony|                    |           |2012-06-28T07:00:00Z|Arts & Entertainment|
# |Pete Bruen & Chri...|                    |           |2010-12-21T08:00:00Z|          Nonfiction|
# |             Nightly|           Radiohead|       1.29|2023-03-17T12:00:00Z|         Alternative|
# |       Talking Heads|Radio Head (Tito ...|       1.29|2006-02-14T08:00:00Z|         Alternative|
# |       Talking Heads|          Radio Head|       1.29|1986-10-07T07:00:00Z|         Alternative|
# |Wolfgang Muthspie...|           Radiohead|       0.99|2008-05-01T12:00:00Z|                Jazz|
# |                Lift|          Radio-Head|       0.99|1997-01-01T12:00:00Z|                Rock|
# |       Nicholas Cole|           Radiohead|       1.29|2015-08-21T12:00:00Z|                Jazz|
# |           Radiohead|               Creep|       1.29|1992-09-21T07:00:00Z|         Alternative|
# |          Thom Yorke|      Hearing Damage|       -1.0|2009-10-09T12:00:00Z|          Soundtrack|
# |           Radiohead|         Daydreaming|       1.29|2016-05-06T07:00:00Z|         Alternative|
# |           Radiohead|      Burn the Witch|       1.29|2016-05-03T07:00:00Z|         Alternative|
# |           Radiohead|          Decks Dark|       1.29|2016-05-08T07:00:00Z|         Alternative|
# |           Radiohead|     True Love Waits|       1.29|2016-05-08T07:00:00Z|         Alternative|
# |       Talking Heads|Radio Head (feat....|       1.29|2018-11-23T12:00:00Z|         Alternative|
# |           Radiohead|         The Numbers|       1.29|2016-05-08T07:00:00Z|         Alternative|
# |           Radiohead|       Present Tense|       1.29|2016-05-08T07:00:00Z|         Alternative|
# |           Radiohead|           Identikit|       1.29|2016-05-08T07:00:00Z|         Alternative|
# |           Radiohead|            Ful Stop|       1.29|2016-05-08T07:00:00Z|         Alternative|
# +--------------------+--------------------+-----------+--------------------+--------------------+
# only showing top 20 rows

records = spark.sql("SELECT json_data.artist_name, json_data.track_name, json_data.track_price, json_data.collectionPrice, json_data.country, json_data.release_date, json_data.genre FROM records").show()

ranked_artists = (
    spark.sql("SELECT json_data.artist_name FROM records")
    .groupBy("artist_name")
    .agg(count("*").alias("track_count"))
    .orderBy("track_count", ascending=False)
)

ranked_artists.show()

# +--------------------+-----------+
# |         artist_name|track_count|
# +--------------------+-----------+
# |           Radiohead|         44|
# |          Thom Yorke|         22|
# |       Philip Selway|         16|
# |Vitamin String Qu...|         16|
# |     Jonny Greenwood|         12|
# | There Will Be Blood|         11|
# |     Atoms for Peace|          8|
# | Christopher O'Riley|          7|
# |        Liz Rollings|          6|
# |             2CELLOS|          4|
# |       Talking Heads|          3|
# |Jarvis Cocker, Ja...|          3|
# |           DreadFool|          3|
# |       Ramin Djawadi|          3|
# |        Flying Lotus|          2|
# |       Amanda Palmer|          2|
# |      Mark Pritchard|          2|
# |        Charlie Moon|          2|
# |         Other Lives|          2|
# |        John Raymond|          2|
# +--------------------+-----------+
# only showing top 20 rows

sp_ranked_genre = spark.sql("SELECT json_data.artist_name, json_data.genre, COUNT(*) AS num_tracks FROM records GROUP BY json_data.artist_name, json_data.genre ORDER BY num_tracks DESC").show()

sp_ranked_genre.show()

# +--------------------+-----------------+----------+
# |         artist_name|            genre|num_tracks|
# +--------------------+-----------------+----------+
# |           Radiohead|      Alternative|        44|
# |          Thom Yorke|      Alternative|        20|
# |       Philip Selway|Singer/Songwriter|        15|
# |     Jonny Greenwood|       Soundtrack|        12|
# |Vitamin String Qu...|             Rock|        12|
# | There Will Be Blood|       Soundtrack|        11|
# |     Atoms for Peace|       Electronic|         8|
# |        Liz Rollings|       Electronic|         6|
# | Christopher O'Riley|          New Age|         6|
# |Vitamin String Qu...|Adult Alternative|         4|
# |       Talking Heads|      Alternative|         3|
# |Jarvis Cocker, Ja...|       Soundtrack|         3|
# |           DreadFool|      Electronica|         3|
# |       Ramin Djawadi|       Soundtrack|         3|
# |       Amanda Palmer|      Alternative|         2|
# |      Mark Pritchard|       Electronic|         2|
# |          Thom Yorke|       Soundtrack|         2|
# |        Charlie Moon|       Vocal Jazz|         2|
# |        Flying Lotus|       Electronic|         2|
# |Scala & Kolacny B...|      Alternative|         2|
# +--------------------+-----------------+----------+
# only showing top 20 rows

records = spark.sql("SELECT json_data.artist_name, json_data.track_name, json_data.track_price, json_data.collectionPrice, json_data.country, json_data.release_date, json_data.genre FROM records")

rank_window = Window.partitionBy("country").orderBy(desc("total_collection_price"))

ranked_genres = (records
     .groupBy("genre", "country")
     .agg(sum("collectionPrice").alias("total_collection_price"))
     .withColumn("rank", rank().over(rank_window))
     .orderBy("country", "rank"))

ranked_genres.show()

# +-------------------+-------+----------------------+----+
# |              genre|country|total_collection_price|rank|
# +-------------------+-------+----------------------+----+
# |        Alternative|    USA|     807.3700000000003|   1|
# |         Soundtrack|    USA|                365.68|   2|
# |             Reggae|    USA|    220.83000000000004|   3|
# |               Rock|    USA|                186.38|   4|
# |         Electronic|    USA|                172.95|   5|
# |  Singer/Songwriter|    USA|                153.83|   6|
# |                Pop|    USA|                 78.88|   7|
# |               Jazz|    USA|                 73.59|   8|
# |            New Age|    USA|                 69.93|   9|
# |          Classical|    USA|                 41.89|  10|
# |  Adult Alternative|    USA|                 39.96|  11|
# |        Electronica|    USA|                 36.96|  12|
# |Classical Crossover|    USA|                 27.98|  13|
# |            Ambient|    USA|                 21.98|  14|
# |     Original Score|    USA|                 19.98|  15|
# |          Hard Rock|    USA|                 19.89|  16|
# |        Hip-Hop/Rap|    USA|    19.380000000000003|  17|
# |          Downtempo|    USA|                 17.91|  18|
# |         Vocal Jazz|    USA|    11.280000000000001|  19|
# |              Metal|    USA|                  9.99|  20|
# +-------------------+-------+----------------------+----+
# only showing top 20 rows

pivot_df = records.groupBy("artist_name", "track_name")\
    .pivot("country")\
    .agg(sum("track_price"))\
    .na.fill(0)

total_df = pivot_df.withColumn("total", reduce(lambda a, b: a+b, [F.col(
    col) for col in pivot_df.columns if col not in ["artist_name", "track_name"]]))

total_df.show()

# +--------------------+--------------------+------------------+------------------+
# |         artist_name|          track_name|               USA|             total|
# +--------------------+--------------------+------------------+------------------+
# | There Will Be Blood|     Henry Plainview|              0.99|              0.99|
# |          Audio Jane|           Radiohead|              0.99|              0.99|
# |           Radiohead|         The Numbers|              2.58|              2.58|
# |           Radiohead|Subterranean Home...|              1.29|              1.29|
# |        Modeselektor|The White Flash (...|              1.29|              1.29|
# |          Lucy Smyth|     Atoms For Peace|              0.99|              0.99|
# |           Radiohead|Climbing up the W...|              1.29|              1.29|
# |           Radiohead|      House of Cards|              1.29|              1.29|
# |         Other Lives|Tamer Animals (At...|2.2800000000000002|2.2800000000000002|
# |     Atoms for Peace|     Reverse Running|              1.29|              1.29|
# |Vitamin String Qu...|           Idioteque|              1.29|              1.29|
# |Jarvis Cocker, Ja...|   Do the Hippogriff|              -1.0|              -1.0|
# | Christopher O'Riley|Everything In Its...|              1.29|              1.29|
# | There Will Be Blood|      Future Markets|              0.99|              0.99|
# |     Atoms for Peace|             Dropped|              1.29|              1.29|
# |           Radiohead|        No Surprises|              2.58|              2.58|
# |Vitamin String Qu...|          All I Need|              1.29|              1.29|
# |     Atoms for Peace|Stuck Together Pi...|              1.29|              1.29|
# |               Dylab|     Energetic Earth|              -1.0|              -1.0|
# |Dwight D. Eisenhower|Atoms for Peace -...|              -1.0|              -1.0|
# +--------------------+--------------------+------------------+------------------+
# only showing top 20 rows
