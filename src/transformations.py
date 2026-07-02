from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

FORMATS_DATE = ["yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy", "dd-MM-yyyy"]

ABREVIATIONS_VOIE = {
    r"\bBd\b": "Boulevard",
    r"\bBld\b": "Boulevard",
    r"\bAv\b": "Avenue",
    r"\bR\.": "Rue",
}

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

#utilitaires

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

def remove_accent(col):
    """fonction pour retirer les accents"""
    return F.translate(
        col,
            'ãäöüẞáäčďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaacdeeillnoorstuuyzAOUSAACDEEILLNOORSTUUYZ',
    )

def normalise_voie(col):
    """Uniformise un libellé de voie"""
    c = title_str(col)                       
    for motif, remplacement in ABREVIATIONS_VOIE.items():
        c = F.regexp_replace(c, motif, remplacement)
    return c

#Fonctions pour le traitement des patients

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
        .withColumn("ligne_adresse", normalise_voie(F.col("ligne_adresse")))
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

def build_fhir(patients):
    """Assemble la ressource FHIR R4 Patient à partir des colonnes réconciliées."""

    # Création de la structure identifiants par IPP. L'actif prend use="usual", les dépréciés prennent use="old"
    identifiants = F.transform(
        "historique_ipp",
        lambda ipp: F.struct(
            F.lit("https://aphp.fr/ipp").alias("system"), 
            F.when(ipp == F.col("ipp_actif"), "usual").otherwise("old").alias("use"),
            ipp.alias("value"),
        ),
    )

    # Création du nom de naissance + nom usuel usual s'il existe
    nom_officiel = F.struct(
        F.lit("official").alias("use"),
        F.col("nom_naissance").alias("family"),
        F.col("prenoms").alias("given"),
    )
    nom_usuel = F.when(
        F.col("nom_usuel").isNotNull(),
        F.struct(
            F.lit("usual").alias("use"),
            F.col("nom_usuel").alias("family"),
            F.col("prenoms").alias("given"),
        ),
    )
    noms = F.filter(F.array(nom_officiel, nom_usuel), lambda x: x.isNotNull())

    # Ajout opposition recherche comme une extension
    extension = F.when(
        F.col("opposition_bool").isNotNull(),
        F.array(F.struct(
            F.lit("https://aphp.fr/opposition").alias("url"),
            F.col("opposition_bool").alias("valueBoolean"),
        )),
    )

    # Déterminons si la validité de la ligne
    active = F.col("date_fin_validite").isNull() | (F.col("date_fin_validite") >= F.current_date())

    # Assemblage de la ressource complète
    patient = F.struct(
        F.lit("Patient").alias("resourceType"),
        F.col("ipp_actif").alias("id"),
        identifiants.alias("identifier"),
        active.alias("active"),
        noms.alias("name"),
        F.col("gender").alias("gender"),
        F.col("date_naissance").cast("string").alias("birthDate"),
        F.col("deceasedDateTime").cast("string").alias("deceasedDateTime"),
        F.col("adresses").alias("address"),
        extension.alias("extension"),
    )

    return patients.withColumn("fhir", patient).select(F.to_json("fhir").alias("patient_fhir"))

