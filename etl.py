import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

'''
Initiate a spark session 
'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''
Process Song_data data using spark and output as parquet to S3
'''

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    song_df = spark.read.json(song_data)
    
    # extract columns to create songs table
    print('Extract columns to create song table')
    artist_id = "artist_id"
    artist_latitude = "artist_latitude"
    artist_location = "artist_location"
    artist_longitude = "artist_longitude"
    artist_name = "artist_name"
    duration = "duration"
    num_songs = "num_songs"
    song_id = "song_id"
    title = "title"
    year = "year"

    # extract columns to create songs table
    song_table = song_df.select(song_id, title, artist_id, year, duration)
    
    #create temp view
    song_df_table = song_table.createOrReplaceTempView("song_df_table")
    
    #create sql query to partition on year and artist_id
    song_table = spark.sql(
                        """SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM song_df_table 
                        """
                      )
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artist_table = song_df.select(artist_id, artist_name, artist_location, artist_longitude, artist_latitude)
    
    #create temp view
    artist_df_table = artist_table.createOrReplaceTempView("artist_df_table")
    
    #create sql query to 
    artist_table = spark.sql(
                        """SELECT DISTINCT artist_id, artist_name, artist_location, artist_longitude, artist_latitude
                        FROM artist_df_table 
                        """
                           )
    
    # write artists table to parquet files
    artist_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


'''
Process log_data data using spark and output as parquet to S3
'''
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json") 

    # read log data file
    log_df = spark.read.json(log_data)
    
    # extract columns for users table  
	artist= 'artist'
	auth= 'auth'
	firstName= 'firstName'
	gender= 'gender'
	itemInSession= 'itemInSession'
	lastName= 'lastName'
	length= 'length'
	level= 'level'
	location= 'location'
	method= 'method'
	page= 'page'
	registration= 'registration'
	sessionId= 'sessionId'
	song= 'song'
	status= 'status'
	ts= 'ts'
	userAgent= 'userAgent'
	userId= 'userId'
	timestamp='timestamp'
	start_time='start_time'
	hour = 'hour'
	day='day'
	week='week'
	month='month'
	year='year'
	weekday='weekday'

    # extract columns for users table    
    users_table = log_df.select(firstName, lastName, gender, level, userId)
    
    #create the users temp_view 
    users_df_table = users_table.createOrReplaceTempView("users_df_table")
    
    #create sql query to 
    users_table = spark.sql(
                        """SELECT DISTINCT firstName, lastName, gender, level, userId
                        FROM users_df_table 
                        """
                      )

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.to_date(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table
    log_df = log_df.withColumn("hour", F.hour("timestamp"))
    log_df = log_df.withColumn("day", F.dayofweek("timestamp"))
    log_df = log_df.withColumn("week", F.weekofyear("timestamp"))
    log_df = log_df.withColumn("month", F.month("timestamp"))
    log_df = log_df.withColumn("year", F.year("timestamp"))
    log_df = log_df.withColumn("weekday", F.dayofweek("timestamp"))
   
    # extract columns to create time table
    time_table = log_df.select(start_time, hour, day, week, month, year, weekday)
    
    #create temp view
    time_table_df.createOrReplaceTempView("time_table_df")
    
    #create sql query to partition on year and artist_id
    time_table = spark.sql(
                            """SELECT DISTINCT start_time, hour, day, week, month, year, weekday
                               FROM time_table_df 
                            """
                            )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays table
    song_df_table=song_df.createOrReplaceTempView("song_df_table")
    
    # read in Log data to use for songplays table
    log_df_table=log_df.createOrReplaceTempView("log_df_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = = spark.sql("""SELECT DISTINCT log_df_table.start_time, log_df_table.userId, log_df_table.level,          
        log_df_table.sessionId,log_df_table.location,log_df_table.userAgent, 
        song_df_table.song_id, song_df_table.artist_id,time_table_df.year,time_table_df.month
        FROM log_df_table 
        INNER JOIN song_df_table 
        ON song_df_table.artist_name = log_df_table.artist 
        INNER JOIN time_table_df
        ON time_table_df.start_time = log_df_table.start_time
        """)
    
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-project-datalake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
