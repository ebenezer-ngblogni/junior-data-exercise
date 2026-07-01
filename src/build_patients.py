from pyspark.sql import SparkSession, functions as F

def build_spark() -> SparkSession:
    # On crée la session Spark
    return (
        SparkSession.builder
        .master("local")
        .appName("Patient FHIR")
        .getOrCreate()
    )

def read_csv(spark: SparkSession, name: str):
    # Fonction de lecture des csv
    return (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"resources/{name}.csv")
    )

def main():
    spark = build_spark()

    # Lecture des 4 fichiers csv
    df_patients = read_csv(spark, "patients")
    df_identifiants = read_csv(spark, "identifiants_ipp")
    df_adresses = read_csv(spark, "adresses")
    df_opposition = read_csv(spark, "opposition_recherche")

    for name, df in [
        ("patients", df_patients),
        ("identifiants", df_identifiants),
        ("adresses", df_adresses),
        ("opposition", df_opposition),
    ]:
        print(f"\n {name} ({df.count()} lignes)")
        df.printSchema()
        df.show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()