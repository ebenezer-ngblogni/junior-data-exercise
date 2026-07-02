from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType

FORMATS_DATE = ["yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy", "dd-MM-yyyy"]

def build_spark() -> SparkSession:
    """On crée la session Spark""" 
    return (
        SparkSession.builder
        .master("local")
        .appName("API FHIR R4")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )

def read_csv(spark: SparkSession, name: str):
    """Fonction de lecture des csv"""
    return (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"resources/{name}.csv")
    )

#Fonctions pour le traitement des patients

def parse_date(col):
    """On recherche le bon format de la date"""
    return F.coalesce(*[F.try_to_date(col, format) for format in FORMATS_DATE])

def normalise_gender(col):
    """Normalise le genre"""
    c = F.lower(F.trim(col))
    return (
        F.when(c.isin("m", "1", "homme", "male", "h"), "male")
        .when(c.isin("f", "2", "femme", "female"), "female")
        .otherwise("unknown")
    )

def normalize_patients(df):
    """Normalise la table patient"""
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

def remove_accent(col):
    """fonction pour retirer les accents"""
    return F.translate(
        col,
            'ãäöüẞáäčďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaacdeeillnoorstuuyzAOUSAACDEEILLNOORSTUUYZ',
    )

def find_ipp_active(patients, df_identifiants):
    """fonction qui ajoute ipp_actif à chaque patient"""
    ipp_deactivated = (
        df_identifiants.withColumn("statut_normalise", F.upper(remove_accent((F.trim("statut")))))
        .filter( (F.col("statut_normalise") == "DEPRECIE") & F.col("ipp_principal").isNotNull() )
        .select(
            F.trim("ipp").alias("ipp_deprecie"),
            F.trim("ipp_principal").alias("ipp_principal"),
        )
    )#.show(truncate=False)

    return (
        patients.join(ipp_deactivated, patients["ipp"] == ipp_deactivated["ipp_deprecie"], "left")
        .withColumn("ipp_actif", F.coalesce("ipp_principal", "ipp")) #on met dans ipp_actif (ipp_principal si non nul) ou on remet ipp
        .drop("ipp_deprecie", "ipp_principal")
    )

def merge_patients (patients):
    """on regroupe les lignes par patient réel"""
    ipp_par_patient = ( #on regroupe les ipp de chaque patient dans historique_ipp
        patients.groupBy("ipp_actif")
         .agg(F.collect_set("ipp").alias("historique_ipp"))
    )

    patients_actifs = patients.filter(F.col("ipp") == F.col("ipp_actif"))
    return patients_actifs.join(ipp_par_patient, "ipp_actif")

#Fonctions pour le traitement des adresses

def normalize_adresses(df):
    """Nettoie les adresses"""
    print("h")

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
        # df.printSchema()
        # df.show(truncate=False)

    patients = normalize_patients(df_patients)
    print(f"\n Patients normalisés ({patients.count()} lignes)")
    # patients.printSchema()
    # patients.show(truncate=False)

    patients = find_ipp_active(patients, df_identifiants)
    patients = merge_patients(patients)
    print(f"\n Patients fusionnés ({patients.count()} lignes)")
    patients.select("ipp_actif", "historique_ipp", "nom_naissance", "prenoms", "gender").show(truncate=False)
    
    
    
    spark.stop()

if __name__ == "__main__":
    main()