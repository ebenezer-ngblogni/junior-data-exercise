from pyspark.sql import SparkSession, functions as F, Window
from transformations import (
    normalize_patients, find_ipp_active, merge_patients,
    normalize_adresses, normalize_opposition, build_fhir,
)

def build_spark() -> SparkSession:
    """On crée la session Spark""" 
    return (
        SparkSession.builder
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
    print(f"\n Patients fusionnés ({patients.count()} ligne)")
    patients.select("ipp_actif", "historique_ipp", "nom_naissance", "prenoms", "gender").show(truncate=False)
    
    #on normalise les adresses
    adresses = normalize_adresses(df_adresses)

    #on ajoute les ipp actifs
    adresses = find_ipp_active(adresses, df_identifiants)
    adresses.show(truncate=False)

    # adresse d'un patient : on ne garde que le plus récent et l'historique est préservé.
    cle_doublon = Window.partitionBy(
        "ipp_actif", "ligne_adresse", "ville", "code_postal"
    ).orderBy(F.desc("date_debut"))
    adresses = (
        adresses
        .withColumn("rang", F.when(F.col("date_fin").isNull(), F.row_number().over(cle_doublon)))
        .filter(F.col("rang").isNull() | (F.col("rang") == 1))
        .drop("rang")
    )

    #on trouve l'adresse courante
    fenetre = Window.partitionBy("ipp_actif").orderBy(F.desc("date_debut"))
    adresses = (
        adresses
        .withColumn("rang", F.row_number().over(fenetre))
        .withColumn("use", F.when(F.col("rang") == 1, "home").otherwise("old"))
    )

    adresses.select("ipp_actif", "ville", "use", "date_debut", "date_fin").orderBy("ipp_actif", "rang").show(truncate=False)

    #formattage FHIR pour chaque adresse

    adresses_fhir = F.struct(
        F.col("use"),
        F.array(F.col("ligne_adresse")).alias("line"),
        F.col("ville").alias("city"),
        F.col("code_postal").alias("postalCode"),
        F.col("pays").alias("country"),
        F.struct(
            F.col("date_debut").cast("string").alias("start"),
            F.col("date_fin").cast("string").alias("end"),
        ).alias("period"),
    )
    
    #on regroupe les adresses par patient

    adresses_par_patient = (
        adresses
        .withColumn("adresse", adresses_fhir)
        .groupBy("ipp_actif")
        .agg(F.collect_list("adresse").alias("adresses"))
    )

    #on rattache l'adresse aux patients
    patients = patients.join(adresses_par_patient, "ipp_actif", "left")

    patients.select("ipp_actif", "nom_naissance", "adresses").show(truncate=False)

    #on normalise l'opposition puis on rattache l'ipp actif
    opposition = normalize_opposition(df_opposition)
    opposition = find_ipp_active(opposition, df_identifiants)

    # en cas de plusieurs recueils pour un même patient : on garde le PLUS RÉCENT
    fenetre_opp = Window.partitionBy("ipp_actif").orderBy(F.desc("date_recueil"))
    opposition = (
        opposition
        .withColumn("rang", F.row_number().over(fenetre_opp))
        .filter(F.col("rang") == 1)
        .select("ipp_actif", "opposition_bool")
    )

    patients = patients.join(opposition, "ipp_actif", "left")
    patients.select("ipp_actif", "nom_naissance", "opposition_bool").show(truncate=False)

    #On construis le FHIR des patients
    resultat = build_fhir(patients)

    #ecriture dans le fichier
    (
        resultat
        .coalesce(1)                      
        .write
        .mode("overwrite")                 
        .text("output/patients_fhir")      
    )
    print(f"\n {resultat.count()} ressources FHIR écrites dans output/patients_fhir/")

    
    spark.stop()

if __name__ == "__main__":
    main()