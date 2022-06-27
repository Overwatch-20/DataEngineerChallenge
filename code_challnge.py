from pyspark.sql.types import StructType,StringType,TimestampType
from pyspark.sql.functions import udf,split, hour, minute, hash, expr, to_date, unix_timestamp,count,countDistinct, regexp_replace, substring, last
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import yaml

import pyspark
spark = pyspark.sql.SparkSession.builder.enableHiveSupport().getOrCreate()

from ua_parser import user_agent_parser
uaParseCommon = user_agent_parser.Parse


def getdetails_from_useragent(ua_string):
    all_details = uaParseCommon(ua_string)
    details_ua = all_details.get('os').get('family') + " " + all_details.get('device').get('family') + " " + all_details.get('user_agent').get(
        'family')
    print(details_ua)
    
    return details_ua


usr_ag_dtl = udf(getdetails_from_useragent, StringType())

logSchema = StructType().add("loggedtimestamp", "string").add("elb", "string").add("client_port", "string").add("backend_port", "string") \
                        .add("request_processing_time", "string").add("backend_processing_time", "string").add("response_processing_time", "string") \
                        .add("elb_status_code", "string").add("backend_status_code", "string").add("received_bytes", "string") \
                        .add("sent_bytes", "string").add("request", "string").add("user_agent", "string") \
                        .add("ssl_cipher", "string").add("ssl_protocol", "string")

df_log = spark.read \
                .format("com.databricks.spark.csv")   \
                .option("header", "false") \
                .option("delimiter"," ") \
                .schema(logSchema) \
                .load('file:///home/saumyat/wlogs_sample.log')

#Get the UDF defined for user agent into  a dataframe
df_log_ = df_log.withColumn("UserAgent_dtl",usr_ag_dtl(df_log.user_agent))

#Get Ip and port separately
UserAgent_dtl = split(df_log_['UserAgent_dtl'], ' ')
srcip_port = split(df_log_['client_port'], ':')
bckip_port = split(df_log_['backend_port'], ':')
rqst = split(df_log_['request'], ' ')


#Select all the required fields to be stored into sessionTable
session_df = df_log_.select("loggedtimestamp",
                                    to_date("loggedtimestamp").alias("date"),
                                    hour("loggedtimestamp").alias("hour"),
                                    minute("loggedtimestamp").alias("min"),
                                    "elb",
                                    srcip_port.getItem(0).alias("srcIP"),
                                    srcip_port.getItem(1).alias("src_port"),
                                    bckip_port.getItem(0).alias("backIP"),
                                    bckip_port.getItem(1).alias("back_port"),
                                    "request_processing_time",
                                    "backend_processing_time",
                                    "elb_status_code",
                                    "backend_status_code",
                                    "received_bytes",
                                    "sent_bytes",
                                    rqst.getItem(1).alias("URL"),
                                    "user_agent",
                                    UserAgent_dtl.getItem(0).alias("os"),
                                    UserAgent_dtl.getItem(1).alias("device"),
                                    UserAgent_dtl.getItem(2).alias("browser"))


#Get the noofUniqueIp,noOfEvents by date and timestamp
session_df.groupBy("date", "hour", "min").agg(countDistinct("srcIP").alias("count_IP"), count("*").alias("total_event")).show()

#Get the number of hits by IP
session_df.groupBy("srcIP").agg(count("*").alias("total_hits")).show()


session_stat = session_df.withColumn("loggedtimestamp",(unix_timestamp("loggedtimestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX") + substring("loggedtimestamp", -7, 6).cast("float")/1000000).cast(TimestampType()))
session_stat.createOrReplaceTempView("session_stat")


session_stat_df = spark.sql("SELECT user_agent, srcIP, loggedtimestamp, latest_event, \
                                          CASE WHEN new_session == 1 THEN CONCAT(srcIP,'_',loggedtimestamp,'_',user_agent) \
                                               ELSE null END AS user_session_id \
                                   FROM (SELECT *, \
                                                CASE WHEN cast(loggedtimestamp as long) - cast(latest_event as long) >= (60 * 15) OR latest_event IS NULL \
                                                     THEN 1 ELSE 0 \
                                                END AS new_session \
                                         FROM (SELECT *, \
                                                      lag(loggedtimestamp,1) OVER (PARTITION BY srcIP ORDER BY loggedtimestamp) AS latest_event FROM session_stat))")

session_stat_df_1 = session_stat_df.withColumn("user_session_id", last(F.col("user_session_id"), ignorenulls=True).over(Window.orderBy("srcIP", "user_agent", "loggedtimestamp")))
session_stat_df_1.createOrReplaceTempView("session_stat_new")


new_session_stat = spark.sql("SELECT user_session_id, min(loggedtimestamp) as strt_time, max(loggedtimestamp) as end_time,\
                            cast(max(cast(loggedtimestamp as double)) - min(cast(loggedtimestamp as double)) AS double) AS session_interval, user_agent,\
                            count(*) AS event_count \
                                      FROM session_stat_new GROUP BY user_session_id,user_agent")

col_ip = split(new_session_stat['user_session_id'], '_')
sessions_fnl = new_session_stat.select("user_session_id", col_ip.getItem(0).alias("IP_addr"),
                      "user_agent","strt_time","end_time","session_interval","event_count")
sessions_fnl.createOrReplaceTempView("sessions_final")

spark.sql("SELECT IP_addr,avg(session_interval) AS avg_session_time,max(session_interval) AS max_time, min(session_interval) AS min_session_time \
                        FROM sessions_final \
                        GROUP BY IP_addr").show()