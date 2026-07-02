from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import Window

# UDF pour faire un title() d'un string
@F.udf(returnType=StringType())
def title_str(s):
    return s.strip().title() if s else s

# UDF pour faire un title() d'un tableau de string
@F.udf(returnType=ArrayType(StringType()))
def title_tab(tab):
    if tab is None:
        return None
    return [s.strip().title() for s in tab]


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
        .withColumn("prenoms", title_tab(F.from_json("prenoms", ArrayType(StringType()))))
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

def find_ipp_active(df, df_identifiants):
    """fonction qui ajoute ipp_actif à chaque ligne du df"""
    ipp_deactivated = (
        df_identifiants.withColumn("statut_normalise", F.upper(remove_accent((F.trim("statut")))))
        .filter( (F.col("statut_normalise") == "DEPRECIE") & F.col("ipp_principal").isNotNull() )
        .select(
            F.trim("ipp").alias("ipp_deprecie"),
            F.trim("ipp_principal").alias("ipp_principal"),
        )
    )#.show(truncate=False)

    return (
        df.join(ipp_deactivated, df["ipp"] == ipp_deactivated["ipp_deprecie"], "left")
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
    return (
        df.withColumn("ipp", F.trim("ipp"))
        .withColumn("ligne_adresse", F.trim("ligne_adresse"))
        .withColumn("code_postal", F.trim("code_postal"))
        .withColumn("ville", title_str(F.col("ville")))
        .withColumn("pays", title_str(F.col("pays")))
        .withColumn("date_debut", parse_date("date_debut"))
        .withColumn("date_fin", parse_date("date_fin"))
    )

#Fonctions pour le traitement des oppositions

def normalize_opposition(df):
    """Convertit l'opposition en booléen."""
    c = F.lower(remove_accent(F.trim(F.col("opposition"))))
    return (
        df.withColumn("ipp", F.trim("ipp"))
        .withColumn(
            "opposition_bool",
            F.when(c.isin("o", "oui", "true", "1", "oppose"), True)
             .when(c.isin("n", "non", "false", "0"), False)
             .otherwise(None), 
        )
        .withColumn("date_recueil", parse_date("date_recueil"))
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

    
    spark.stop()

if __name__ == "__main__":
    main()