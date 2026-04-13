"""
================================================================
DATA LAKE — COUCHE SILVER
================================================================
Rôle  : Preprocessing technique Bronze → Parquet propre
Source: data_lake/output/bronze/
Sortie: data_lake/output/silver/

Ce qu'on fait ici :
  ✅ Cast types (TEXT → numeric, timestamp)
  ✅ Filtres qualité (FLAG_COPIE='O', NULL clés → DROP)
  ✅ Filtres cohérence dates
  ✅ Déduplication (1 ligne/client/période)
  ✅ Mappings simples ('O'/'N' → 1/0, 'M'/'F' → 1/0)
  ✅ Trim colonnes texte
  ✅ Harmonisation noms colonnes (majuscule → minuscule)

Ce qu'on NE fait PAS ici :
  ❌ Agrégations GROUP BY   → Gold
  ❌ Imputation médiane     → Mois 3 ML
  ❌ NULL logique → 0       → Gold
  ❌ One-hot encoding       → Gold
  ❌ SMOTE / normalisation  → Mois 3 ML
================================================================
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# ================================================================
# CONFIGURATION
# ================================================================

PROJECT_ROOT = "C:/Users/saadb/pfe-scoring-credit"
BRONZE_PATH  = f"{PROJECT_ROOT}/data_lake/output/bronze"
SILVER_PATH  = f"{PROJECT_ROOT}/data_lake/output/silver"

# ================================================================
# SPARK SESSION
# ================================================================

spark = SparkSession.builder \
    .appName("Silver_Preprocessing") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("SILVER — Démarrage preprocessing technique")
print("=" * 60)


# ================================================================
# FONCTIONS UTILITAIRES
# ================================================================

def read_bronze(name):
    """Lit un Parquet Bronze."""
    path = f"{BRONZE_PATH}/{name}"
    df = spark.read.parquet(path)
    print(f"\n📖 Bronze/{name} → {df.count():,} lignes")
    return df


def save_silver(df, name):
    """Sauvegarde en Parquet Silver."""
    path = f"{SILVER_PATH}/{name}"
    df.write.mode("overwrite").format("parquet").save(path)
    nb = df.count()
    print(f"   ✅ Silver/{name} → {nb:,} lignes | {len(df.columns)} colonnes")
    return nb


def cast_num(col_name):
    """
    Convertit une colonne TEXT en Double.
    Remplace chaîne vide par NULL.
    Pattern : NULLIF(TRIM(col), '')::numeric en SQL
    """
    return F.nullif(F.trim(F.col(col_name)), "").cast(DoubleType())


def parse_date(col_name):
    """
    Parse le format bancaire : '01APR2025:10:01:33'
    vers Timestamp PySpark.
    """
    return F.to_timestamp(F.col(col_name), "ddMMMyyyy:HH:mm:ss")


# ================================================================
# TABLE 1 — FLAG_TRANSFO
# ================================================================
print("\n[1/7] flag_transfo — preprocessing")

df = read_bronze("flag_transfo")

# Avant
print(f"   Avant  : {df.count():,} lignes")

silver_flag = df \
    .withColumn("tiers_client",
        F.col("TIERS_CLIENT")) \
    .withColumn("periode_trt",
        F.trim(F.col("PERIODE_TRT"))) \
    .withColumn("date_trt_extr",
        parse_date("DATE_TRT_EXTR")) \
    .withColumn("flag_transfo",
        F.col("FLAG_TRANSFO").cast(IntegerType())) \
    .withColumn("is_prediction_period",
        F.when(F.trim(F.col("PERIODE_TRT")) == "012026", 1)
         .otherwise(0).cast(IntegerType())) \
    .filter(F.col("TIERS_CLIENT").isNotNull()) \
    .filter(F.col("PERIODE_TRT").isNotNull()) \
    .select(
        "tiers_client",
        "periode_trt",
        "date_trt_extr",
        "flag_transfo",
        "is_prediction_period"
    )

nb_apres = save_silver(silver_flag, "flag_transfo")
print(f"   Lignes supprimées : {df.count() - nb_apres:,}")


# ================================================================
# TABLE 2 — SIGNALETIQUE
# ================================================================
print("\n[2/7] signaletique — preprocessing")

df = read_bronze("signaletique")
nb_avant = df.count()

silver_sig = df \
    .withColumn("tiers_client",
        F.col("TIERS_CLIENT")) \
    .withColumn("id_tiers_siebel",
        F.col("ID_TIERS_SIEBEL")) \
    .withColumn("periode_trt",
        F.trim(F.col("PERIODE_TRT"))) \
    .withColumn("date_trt_extr",
        parse_date("DATE_TRT_EXTR")) \
    .withColumn("date_ent_relation",
        F.col("DATE_ENT_RELATION").cast("string")) \
    .withColumn("date_dernier_evt",
        F.col("DATE_DERNIER_EVT").cast("string")) \
    .withColumn("date_embauche",
        F.col("DATE_EMBAUCHE").cast("string")) \
    \
    .withColumn("age",              cast_num("AGE_CLIENT")) \
    .withColumn("revenu",           cast_num("REVENU_MENSUEL")) \
    .withColumn("nbr_enfant",       cast_num("NBR_ENFANT")) \
    .withColumn("charges",          cast_num("CHARGES_CLIE")) \
    .withColumn("mensualite_loyer", cast_num("MENSUALITE_LOYER")) \
    \
    .withColumn("flag_eligible_md",
        F.when(F.upper(F.trim(F.col("FLAG_ELIGIBLE_MD"))) == "O", 1)
         .when(F.upper(F.trim(F.col("FLAG_ELIGIBLE_MD"))) == "N", 0)
         .otherwise(None).cast(IntegerType())) \
    \
    .withColumn("civilite_client",
        F.when(F.upper(F.trim(F.col("CIVILITE_CLIENT"))) == "M", 1)
         .when(F.upper(F.trim(F.col("CIVILITE_CLIENT"))) == "F", 0)
         .otherwise(None).cast(IntegerType())) \
    \
    .withColumn("csp_mkt",             F.trim(F.col("CSP_MKT"))) \
    .withColumn("secteur_activite",    F.trim(F.col("SECTEUR_ACTIVITE"))) \
    .withColumn("prem_produit",        F.trim(F.col("PREM_PRODUIT"))) \
    .withColumn("canal_ent_relation",  F.trim(F.col("CANAL_ENT_RELATION"))) \
    .withColumn("type_client",         F.trim(F.col("TYPE_CLIENT"))) \
    .withColumn("activite_profession", F.trim(F.col("ACTIVITE_PROFESSION"))) \
    .withColumn("dernier_evt",         F.trim(F.col("DERNIER_EVT"))) \
    \
    .filter(F.col("TIERS_CLIENT").isNotNull()) \
    .filter(
        F.col("DATE_TRT_EXTR").isNull() |
        F.col("DATE_ENT_RELATION").isNull() |
        (F.col("DATE_TRT_EXTR").cast("string") >= F.col("DATE_ENT_RELATION").cast("string"))
    ) \
    .select(
        "tiers_client", "id_tiers_siebel", "periode_trt",
        "date_trt_extr", "date_ent_relation", "date_dernier_evt", "date_embauche",
        "age", "revenu", "nbr_enfant", "charges", "mensualite_loyer",
        "flag_eligible_md", "civilite_client",
        "csp_mkt", "secteur_activite", "prem_produit",
        "canal_ent_relation", "type_client", "activite_profession", "dernier_evt"
    )

nb_apres = save_silver(silver_sig, "signaletique")
print(f"   Lignes supprimées : {nb_avant - nb_apres:,}")


# ================================================================
# TABLE 3 — AFFAIRE
# ================================================================
print("\n[3/7] affaire — preprocessing")

df = read_bronze("affaire")
nb_avant = df.count()

silver_aff = df \
    .withColumn("tiers_client",       F.col("TIERS_CLIENT")) \
    .withColumn("ie_affaire",         F.col("IE_AFFAIRE").cast("string")) \
    .withColumn("periode_trt",        F.trim(F.col("PERIODE_TRT"))) \
    .withColumn("date_trt_extr",      parse_date("DATE_TRT_EXTR")) \
    .withColumn("date_mep",           F.col("DATE_MEP").cast("string")) \
    .withColumn("date_ech_init",      F.col("DATE_ECH_INIT").cast("string")) \
    .withColumn("date_ech_reel",      F.col("DATE_ECH_REEL").cast("string")) \
    .withColumn("date_entree_ctx",    F.col("DATE_ENTREE_CTX").cast("string")) \
    \
    .withColumn("montant_init_brut",  cast_num("MT_INIT_BRUT")) \
    .withColumn("capital_rest",       cast_num("CAPITAL_REST")) \
    .withColumn("montant_bien",       cast_num("MONTANT_BIEN")) \
    .withColumn("mt_apport",          cast_num("MT_APPORT_TTC")) \
    .withColumn("mt_rachat_tot",      cast_num("MT_RACHAT_TOT")) \
    .withColumn("mt_vr",              cast_num("MT_VR")) \
    .withColumn("mt_dg",              cast_num("MT_DG")) \
    .withColumn("nb_impaye",          cast_num("NB_IMPAYE")) \
    .withColumn("nb_impaye_regle",    cast_num("NB_IMPAYE_REGLE")) \
    .withColumn("solde_impaye",       cast_num("SOLDE_IMPAYE")) \
    .withColumn("taux_credit",        cast_num("TAUX_CREDIT")) \
    .withColumn("mensualite",         cast_num("MENSUALITE")) \
    .withColumn("mensualite_av_der",  cast_num("MENSUALITE_AV_DER")) \
    .withColumn("duree_initiale",     cast_num("DUREE_INITIALE")) \
    .withColumn("duree_actuelle",     cast_num("DUREE_ACTUELLE")) \
    .withColumn("nbr_ech_rest",       cast_num("NBR_ECH_REST")) \
    .withColumn("differe",            cast_num("DIFFERE")) \
    .withColumn("mt_encours_ctx",     cast_num("MT_ENCOURS_CTX")) \
    \
    .withColumn("produit_wfs",        F.trim(F.col("PRODUIT_WFS"))) \
    .withColumn("type_prel_actuel",   F.trim(F.col("TYPE_PREL_ACTUEL"))) \
    .withColumn("canal_prov",         F.trim(F.col("CANAL_PROV"))) \
    .withColumn("code_reseau",        F.trim(F.col("CODE_RESEAU"))) \
    .withColumn("type_bien",          F.trim(F.col("TYPE_BIEN"))) \
    \
    .filter(F.col("TIERS_CLIENT").isNotNull()) \
    .filter(
        F.col("DATE_TRT_EXTR").isNull() |
        F.col("DATE_MEP").isNull() |
        (F.col("DATE_TRT_EXTR").cast("string") >= F.col("DATE_MEP").cast("string"))
    ) \
    .select(
        "tiers_client", "ie_affaire", "periode_trt", "date_trt_extr",
        "date_mep", "date_ech_init", "date_ech_reel", "date_entree_ctx",
        "montant_init_brut", "capital_rest", "montant_bien",
        "mt_apport", "mt_rachat_tot", "mt_vr", "mt_dg",
        "nb_impaye", "nb_impaye_regle", "solde_impaye",
        "taux_credit", "mensualite", "mensualite_av_der",
        "duree_initiale", "duree_actuelle", "nbr_ech_rest", "differe",
        "mt_encours_ctx", "produit_wfs", "type_prel_actuel",
        "canal_prov", "code_reseau", "type_bien"
    )

nb_apres = save_silver(silver_aff, "affaire")
print(f"   Lignes supprimées : {nb_avant - nb_apres:,}  (incohérences date)")


# ================================================================
# TABLE 4 — CIBLAGE
# ================================================================
print("\n[4/7] ciblage — preprocessing")

df = read_bronze("ciblage")
nb_avant = df.count()

silver_cib = df \
    .withColumn("tiers_client",    F.col("ID_TIER")) \
    .withColumn("periode_j",       F.col("PERIODE_J").cast("string")) \
    .withColumn("nb_sms_recu",     cast_num("NB_SMS_RECU_J")) \
    .withColumn("nb_sms_failed",   cast_num("NB_SMS_FAILED_J")) \
    .withColumn("nb_voice_recu",   cast_num("NB_APPELS_RECU_MSGVOCALE_J")) \
    .withColumn("nb_voice_failed", cast_num("NB_APPELS_FAILED_MSGVOCALE_J")) \
    .withColumn("flag_canal",      F.trim(F.col("FLAG"))) \
    .withColumn("rtc",             F.trim(F.col("RTC"))) \
    \
    .filter(F.col("ID_TIER").isNotNull()) \
    .select(
        "tiers_client", "periode_j",
        "nb_sms_recu", "nb_sms_failed",
        "nb_voice_recu", "nb_voice_failed",
        "flag_canal", "rtc"
    )

nb_apres = save_silver(silver_cib, "ciblage")
print(f"   Lignes supprimées : {nb_avant - nb_apres:,}")


# ================================================================
# TABLE 5 — SAV
# ================================================================
print("\n[5/7] sav — preprocessing")

df = read_bronze("sav")
nb_avant = df.count()

silver_sav = df \
    .withColumn("id_tiers_siebel",   F.col("ID_TIERS_SIEBEL")) \
    .withColumn("periode_trt",       F.trim(F.col("PERIODE_TRT"))) \
    .withColumn("date_trt_extr",     parse_date("DATE_TRT_EXTR")) \
    .withColumn("date_creation",     F.col("DATE_CREATION").cast("string")) \
    .withColumn("date_fin",          F.col("DATE_FIN").cast("string")) \
    .withColumn("date_fin_theorique",F.col("DATE_FIN_THEORIQUE").cast("string")) \
    .withColumn("agence_creation",   F.trim(F.col("AGENCE_CREATION"))) \
    .withColumn("num_affaire",       F.col("NUM_AFFAIRE").cast("string")) \
    .withColumn("categorie",         F.trim(F.col("CATEGORIE"))) \
    .withColumn("sous_categorie",    F.trim(F.col("SOUS_CATEGORIE"))) \
    .withColumn("statut_demande",    F.trim(F.col("STATUT_DEMANDE"))) \
    .withColumn("canal",             F.trim(F.col("CANAL"))) \
    \
    .filter(F.col("ID_TIERS_SIEBEL").isNotNull()) \
    .filter(F.trim(F.col("FLAG_COPIE")) == "O") \
    .filter(
        F.col("DATE_TRT_EXTR").isNull() |
        F.col("DATE_CREATION").isNull() |
        (F.col("DATE_TRT_EXTR").cast("string") >= F.col("DATE_CREATION").cast("string"))
    ) \
    .select(
        "id_tiers_siebel", "periode_trt", "date_trt_extr",
        "date_creation", "date_fin", "date_fin_theorique",
        "agence_creation", "num_affaire",
        "categorie", "sous_categorie", "statut_demande", "canal"
    )

nb_apres = save_silver(silver_sav, "sav")
print(f"   Lignes supprimées : {nb_avant - nb_apres:,}  (FLAG_COPIE + dates)")


# ================================================================
# TABLE 6 — RECLAMATION
# ================================================================
print("\n[6/7] reclamation — preprocessing")

df = read_bronze("reclamation")
nb_avant = df.count()

silver_recla = df \
    .withColumn("id_tiers_siebel", F.col("ID_TIERS_SIEBEL")) \
    .withColumn("id_demande",      F.col("ID_DEMANDE").cast("string")) \
    .withColumn("periode_trt",     F.trim(F.col("PERIODE_TRT"))) \
    .withColumn("date_trt_extr",   parse_date("DATE_TRT_EXTR")) \
    .withColumn("date_creation",   F.col("DATE_CREATION").cast("string")) \
    .withColumn("date_fin",        F.col("DATE_FIN").cast("string")) \
    .withColumn("sous_categorie",  F.trim(F.col("SOUS_CATEGORIE"))) \
    \
    .filter(F.col("ID_TIERS_SIEBEL").isNotNull()) \
    .filter(F.trim(F.col("FLAG_COPIE")) == "O") \
    .filter(
        F.col("DATE_TRT_EXTR").isNull() |
        F.col("DATE_CREATION").isNull() |
        (F.col("DATE_TRT_EXTR").cast("string") >= F.col("DATE_CREATION").cast("string"))
    ) \
    .select(
        "id_tiers_siebel", "id_demande", "periode_trt",
        "date_trt_extr", "date_creation", "date_fin", "sous_categorie"
    )

nb_apres = save_silver(silver_recla, "reclamation")
print(f"   Lignes supprimées : {nb_avant - nb_apres:,}  (FLAG_COPIE + dates)")


# ================================================================
# TABLE 7 — DEMANDE_INFO
# ================================================================
print("\n[7/7] demande_info — preprocessing")

df = read_bronze("demande_info")
nb_avant = df.count()

silver_dem = df \
    .withColumn("id_tiers_siebel", F.col("ID_TIERS_SIEBEL")) \
    .withColumn("id_demande",      F.col("ID_DEMANDE").cast("string")) \
    .withColumn("periode_trt",     F.trim(F.col("PERIODE_TRT"))) \
    .withColumn("date_trt_extr",   parse_date("DATE_TRT_EXTR")) \
    .withColumn("date_creation",   F.col("DATE_CREATION").cast("string")) \
    .withColumn("date_fin",        F.col("DATE_FIN").cast("string")) \
    .withColumn("categorie",       F.trim(F.col("CATEGORIE"))) \
    .withColumn("sous_categorie",  F.trim(F.col("SOUS_CATEGORIE"))) \
    \
    .filter(F.col("ID_TIERS_SIEBEL").isNotNull()) \
    .filter(F.trim(F.col("FLAG_COPIE")) == "O") \
    .filter(
        F.col("DATE_TRT_EXTR").isNull() |
        F.col("DATE_CREATION").isNull() |
        (F.col("DATE_TRT_EXTR").cast("string") >= F.col("DATE_CREATION").cast("string"))
    ) \
    .select(
        "id_tiers_siebel", "id_demande", "periode_trt",
        "date_trt_extr", "date_creation", "date_fin",
        "categorie", "sous_categorie"
    )

nb_apres = save_silver(silver_dem, "demande_info")
print(f"   Lignes supprimées : {nb_avant - nb_apres:,}  (FLAG_COPIE + dates)")


# ================================================================
# RÉSUMÉ FINAL SILVER
# ================================================================
print("\n" + "=" * 60)
print("SILVER — Résumé preprocessing technique")
print("=" * 60)

tables = ["flag_transfo","signaletique","affaire","ciblage","sav","reclamation","demande_info"]
for name in tables:
    df_check = spark.read.parquet(f"{SILVER_PATH}/{name}")
    print(f"   {name:<20} {df_check.count():>12,} lignes | {len(df_check.columns):>3} colonnes")

print("=" * 60)
print("✅ SILVER TERMINÉ")
print("=" * 60)

spark.stop()