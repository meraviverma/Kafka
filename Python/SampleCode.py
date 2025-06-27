def create_kafka_producer():
    try:
        data=df.selectExpr("CAST(key AS STRING) AS key,"value")\
                .write\
                .format("kafka")
                .option("kafka.acks","all")
                .option("kafka.ssl.keystore.password",keystore_pwd)
                .option("kafka.ssl.truststore.password",truststore_pwd)
                .option("topic",topic)
                .options(**conf)
        return data
    except Exception as e:
        raise Exception(f"Unable to egt kafka connection to piush to topic: {e}")\
        
 
def publish_message(df,conf):
    try:
        keystore_conf=conf["config"]["authentication"]["keystore_key"]
        truststore_conf=conf["config"]["authentication"]["keystore_key"]
        keystore_pwd=dbutils.secrets.get(scope=keystore_conf["dbutils_scope"],key=keystore_conf["dbutils_key"])
        truststore_pwd=dbutils.secrets.get(scope=keystore_conf["dbutils_scope"],key=keystore_conf["dbutils_key"])
        
        stream_df=create_kafka_producer(df,conf["conf"]["spark_write"],keystore_pwd,truststore_pwd,topic)
      