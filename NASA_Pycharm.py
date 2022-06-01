def spark_session():
    from pyspark.sql import SparkSession
    spark_s = (SparkSession
               .builder
               .appName("PyCharm NASA")
               .getOrCreate())
    return spark_s


def archivo(spark_s, ruta):
    df = spark_s.read.text(ruta)
    return df


def regular_ex(arch, ex, col):
    from pyspark.sql.functions import regexp_extract
    new_arch = arch.select(regexp_extract(col, ex, 1).alias("host"),
                           regexp_extract(col, ex, 2).alias("day"),
                           regexp_extract(col, ex, 3).alias("hours"),
                           regexp_extract(col, ex, 4).alias("minutes"),
                           regexp_extract(col, ex, 5).alias("seconds"),
                           regexp_extract(col, ex, 6).alias("request"),
                           regexp_extract(col, ex, 7).alias("resource"),
                           regexp_extract(col, ex, 8).alias("protocol"),
                           regexp_extract(col, ex, 9).alias("http_status_code"),
                           regexp_extract(col, ex, 10).alias("size")
                           )
    return new_arch


def cons_1(arch, column):
    from pyspark.sql import functions as F
    cons1 = (arch
     .select(column)
     .where(F.col(column) != "")
     .groupBy(column)
     .count())
    return cons1


def cons_2(arch, column):
    cons2 = (arch
             .select(column)
             .groupBy(column)
             .count()
             .orderBy("count", descending=False))
    return cons2


if __name__ == '__main__':
    ruta_ag = """C:/Users/nora.hafidi/Desktop/Big Data/NASA/NASA_access_log_Aug95/access_log_Aug95"""
    ruta_jul = "C:/Users/nora.hafidi/Desktop/Big Data/NASA/NASA_access_log_Jul95/access_log_Jul95"
    expresion = """(.*)[\\s-]{4}\\s\\[(\\d\\d)\\/.*\\/1995:(\\d\\d):(\\d\\d):(\\d\\d).*\\s\"\\s?([A-Z]*)\\s(\\/\\S*).*?(HTTP.*)?\"\\s?(\\d\\d\\d)\\s(\\d*)"""
    spark = spark_session()
    ag95 = archivo(spark, ruta_ag)
    #ag95.show()
    jul95 = archivo(spark, ruta_jul)
    #jul95.show()
    ag95_prima = regular_ex(ag95, expresion, 'value')
    #ag95_prima.show()
    jul95_prima = regular_ex(jul95, expresion, 'value')
    #jul95_prima.show()
    print('JULIO')
    cons1_j = cons_1(jul95_prima, 'protocol')
    cons1_j.show()
    cons2_j = cons_2(jul95_prima, 'http_status_code')
    cons2_j.show()
    print('AGOSTO')
    cons1_a = cons_1(ag95_prima, 'protocol')
    cons1_a.show()
    cons2_a = cons_2(ag95_prima, 'http_status_code')
    cons2_a.show()
