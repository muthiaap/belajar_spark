from pyspark.sql import SparkSession
import os
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        spark = SparkSession.builder \
            .appName("Retail Analysis") \
            .config("spark.jars", os.getenv("SPARK_JARS", "/tmp/docker-desktop-root/mnt/host/c/Users/Muthia/Downloads/scrapt/dibimbing_spark_airflow-main/postgresql-42.2.18.jar")) \
            .getOrCreate()

        logger.info("Reading data from PostgreSQL")
        df = spark.read \
            .format("jdbc") \
            .option("url", os.getenv("DB_URL", "jdbc:postgresql://localhost:5432/postgres")) \
            .option("dbtable", "retail") \
            .option("user", os.getenv("DB_USER", "postgres")) \
            .option("password", os.getenv("DB_PASSWORD", "26111999Map")) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df.createOrReplaceTempView("retail")
        result = spark.sql("""
            SELECT
                product_id,
                COUNT(*) as total_sales,
                SUM(price) as total_revenue
            FROM retail
            GROUP BY product_id
        """)

        result.show()

        logger.info("Saving result to PostgreSQL")
        result.write \
            .format("jdbc") \
            .option("url", os.getenv("DB_URL", "jdbc:postgresql://localhost:5432/postgres")) \
            .option("dbtable", "retail_analysis_result") \
            .option("user", os.getenv("DB_USER", "postgres")) \
            .option("password", os.getenv("DB_PASSWORD", "26111999Map")) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    except Exception as e:
        logger.error("An error occurred during Spark job execution", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
