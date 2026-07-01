from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType

FORMATS_DATE = ["yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy", "dd-MM-yyyy"]

def build_spark() -> SparkSession:
    # On crée la session Spark
    return (
        SparkSession.builder
        .master("local")
        .appName("API FHIR R4")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
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

def parse_date(col):
    # On recherche le bon format de la date
    return F.coalesce(*[F.try_to_date(col, format) for format in FORMATS_DATE])

def normalise_gender(col):
    c = F.lower(F.trim(col))
    return (
        F.when(c.isin("m", "1", "homme", "male", "h"), "male")
        .when(c.isin("f", "2", "femme", "female"), "female")
        .otherwise("unknown")
    )

def normalize_patients(df):
    return(
        df.withColumn("ipp", F.trim("ipp"))
        .withColumn("nom_naissance", F.upper(F.trim("nom_naissance")))
        .withColumn("nom_usuel", F.upper(F.trim("nom_usuel")))
        .withColumn("prenoms", F.transform(F.from_json("prenoms", ArrayType(StringType())), lambda p: F.initcap(F.trim(p))))
        .withColumn("date_naissance", parse_date("date_naissance"))
        .withColumn("deceasedDateTime", parse_date("date_deces"))
        .withColumn("date_fin_validite", parse_date("date_fin_validite"))
        .withColumn("gender", normalise_gender("sexe"))
        .drop("_c8")
        .dropDuplicates() #on se débarasse des doublons
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

    patients = normalize_patients(df_patients)
    print(f"\n Patients normalisés ({patients.count()} lignes)")
    patients.printSchema()
    patients.show(truncate=False)

    
    spark.stop()

if __name__ == "__main__":
    main()