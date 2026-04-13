"""
================================================================
DATA LAKE — COUCHE GOLD
================================================================
Rôle  : Preprocessing métier Silver → Dataset ML final
Source: data_lake/output/silver/
Sortie: data_lake/output/gold/dataset_ml_spark

Ce qu'on fait ici :
  ✅ Déduplication signaletique (1 ligne/client/période)
  ✅ Features temporelles (ancienneté, nb_jours_dernier_evt)
  ✅ Agrégations GROUP BY (affaire, sav, recla, demande_info)
  ✅ Flags binaires métier (flag_recouvrement, flag_impaye...)
  ✅ NULL logique → 0 (nb_sav, nb_campagnes...)
  ✅ One-hot encoding catégorielles (csp_mkt, prem_produit...)
  ✅ JOIN final (1 ligne par client par période)
  ✅ Sauvegarde Parquet Gold + PostgreSQL

Ce qu'on NE fait PAS ici :
  ❌ Imputation médiane   → Mois 3 ML (après split train/scoring)
  ❌ Normalisation/scaling → Mois 3 ML
  ❌ SMOTE               → Mois 3 ML
================================================================
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# ================================================================
# CONFIGURATION
# ================================================================

PROJECT_ROOT = "C:/Users/saadb/pfe-scoring-credit"
SILVER_PATH  = f"{PROJECT_ROOT}/data_lake/output/silver"
GOLD_PATH    = f"{PROJECT_ROOT}/data_lake/output/gold"
JDBC_JAR     = f"{PROJECT_ROOT}/drivers/postgresql-42.7.3.jar"

JDBC_URL   = "jdbc:postgresql://localhost:5433/pfe_credit_dw"
JDBC_PROPS = {
    "user"    : "postgres",
    "password": "Saad2002",
    "driver"  : "org.postgresql.Driver"
}

# ================================================================
# SPARK SESSION
# ================================================================

spark = SparkSession.builder \
    .appName("Gold_BusinessPreprocessing") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath", JDBC_JAR) \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("GOLD — Démarrage preprocessing métier")
print("=" * 60)


# ================================================================
# FONCTIONS UTILITAIRES
# ================================================================

def read_silver(name):
    path = f"{SILVER_PATH}/{name}"
    df = spark.read.parquet(path)
    print(f"\n📖 Silver/{name} → {df.count():,} lignes")
    return df


def parse_date(col_name):
    return F.to_timestamp(F.col(col_name), "ddMMMyyyy:HH:mm:ss")


# ================================================================
# ÉTAPE 1 — FLAG_TRANSFO (base du dataset)
# ================================================================
print("\n[1/7] flag_transfo — base dataset")

gold_flag = read_silver("flag_transfo")
print(f"   → Base : {gold_flag.count():,} lignes (1 ligne/client/période)")


# ================================================================
# ÉTAPE 2 — SIGNALETIQUE
# Feature engineering + déduplication
# ================================================================
print("\n[2/7] signaletique — features temporelles + déduplication")

df_sig = read_silver("signaletique")

# Parse dates pour calculs
df_sig = df_sig \
    .withColumn("date_entree_ts",
        parse_date("date_ent_relation")) \
    .withColumn("date_dernier_ts",
        parse_date("date_dernier_evt")) \
    .withColumn("date_embauche_ts",
        parse_date("date_embauche"))

# Features temporelles
today = F.current_date()
df_sig = df_sig \
    .withColumn("anciennete_annees",
        F.round(
            F.datediff(today, F.to_date("date_entree_ts")) / 365.25
        , 1)) \
    .withColumn("anciennete_emploi",
        F.round(
            F.datediff(today, F.to_date("date_embauche_ts")) / 365.25
        , 1)) \
    .withColumn("nb_jours_dernier_evt",
        F.datediff(today, F.to_date("date_dernier_ts")).cast(DoubleType()))

# One-hot encoding — csp_mkt
csp_values = ["SALARIE", "RETRAITE", "PROFESSION_LIBERALE",
              "COMMERCANT", "FONCTIONNAIRE", "SANS_EMPLOI"]
for val in csp_values:
    df_sig = df_sig.withColumn(
        f"csp_{val.lower()}",
        F.when(F.upper(F.col("csp_mkt")) == val, 1).otherwise(0).cast(IntegerType())
    )

# One-hot encoding — prem_produit (top produits)
prod_values = ["CREDIT_AUTO", "CREDIT_IMMO", "CREDIT_PERSO",
               "CREDIT_EQUIP", "AUTRE"]
for val in prod_values:
    df_sig = df_sig.withColumn(
        f"prod_{val.lower()}",
        F.when(F.upper(F.col("prem_produit")) == val, 1).otherwise(0).cast(IntegerType())
    )

# Déduplication : 1 ligne par (tiers_client, periode_trt)
# Garder la plus récente (date_entree_ts DESC)
w = Window \
    .partitionBy("tiers_client", "periode_trt") \
    .orderBy(F.col("date_entree_ts").desc_nulls_last())

df_sig = df_sig \
    .withColumn("_rank", F.row_number().over(w)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank", "date_entree_ts", "date_dernier_ts",
          "date_embauche_ts", "date_ent_relation",
          "date_dernier_evt", "date_embauche")

print(f"   → Après déduplication : {df_sig.count():,} lignes")


# ================================================================
# ÉTAPE 3 — AFFAIRE
# Agrégations GROUP BY tiers_client + periode_trt
# ================================================================
print("\n[3/7] affaire — agrégations + flags")

df_aff = read_silver("affaire")

# Parse dates pour calculs délais
df_aff = df_aff \
    .withColumn("date_mep_ts",      parse_date("date_mep")) \
    .withColumn("date_ech_init_ts", parse_date("date_ech_init")) \
    .withColumn("date_ech_reel_ts", parse_date("date_ech_reel"))

# Calcul délais AVANT agrégation (par ligne)
df_aff = df_aff \
    .withColumn("retard_echeance",
        F.when(
            F.col("date_ech_reel_ts").isNotNull() &
            F.col("date_ech_init_ts").isNotNull(),
            F.datediff(
                F.to_date("date_ech_reel_ts"),
                F.to_date("date_ech_init_ts")
            ).cast(DoubleType())
        )
    ) \
    .withColumn("delai_extraction",
        F.when(
            F.col("date_trt_extr").isNotNull() &
            F.col("date_mep_ts").isNotNull(),
            F.datediff(
                F.to_date("date_trt_extr"),
                F.to_date("date_mep_ts")
            ).cast(DoubleType())
        )
    )

# Agrégation
gold_aff = df_aff.groupBy("tiers_client", "periode_trt").agg(

    # Comptages
    F.countDistinct("ie_affaire")                                    .alias("nb_credits"),

    # Montants — NULL gardé (pas de crédit = NULL, pas 0)
    F.avg("montant_init_brut")                                       .alias("moy_mt_init_brut"),
    F.avg("capital_rest")                                            .alias("moy_mt_cap_rest"),
    F.avg("montant_bien")                                            .alias("moy_montant_bien"),

    # Montants — NULL logique → 0 (pas d'apport = 0)
    F.avg(F.coalesce(F.col("mt_apport"),     F.lit(0.0)))            .alias("moy_mt_apport"),
    F.avg(F.coalesce(F.col("mt_rachat_tot"), F.lit(0.0)))            .alias("moy_mt_rachat_tot"),
    F.avg(F.coalesce(F.col("mt_vr"),         F.lit(0.0)))            .alias("moy_mt_vr"),
    F.avg(F.coalesce(F.col("mt_dg"),         F.lit(0.0)))            .alias("moy_mt_dg"),

    # Impayés
    F.sum(F.coalesce(F.col("nb_impaye"),      F.lit(0.0)))           .alias("total_nb_impaye"),
    F.sum(F.coalesce(F.col("nb_impaye_regle"),F.lit(0.0)))           .alias("total_nb_impaye_regle"),
    F.sum(F.coalesce(F.col("solde_impaye"),   F.lit(0.0)))           .alias("total_solde_impaye"),
    F.max(F.coalesce(F.col("nb_impaye"),      F.lit(0.0)))           .alias("max_nb_impaye"),

    # Taux et durées — NULL gardé (erreur saisie)
    F.avg("taux_credit")                                             .alias("moy_taux_credit"),
    F.avg(F.coalesce(F.col("mensualite"),     F.lit(0.0)))           .alias("moy_mensualite"),
    F.avg(F.coalesce(F.col("mensualite_av_der"),F.lit(0.0)))         .alias("moy_mensualite_av_der"),
    F.avg("duree_initiale")                                          .alias("moy_duree_initiale"),
    F.avg("duree_actuelle")                                          .alias("moy_duree_actuelle"),
    F.avg("nbr_ech_rest")                                            .alias("moy_nbr_ech_rest"),
    F.avg(F.coalesce(F.col("differe"),        F.lit(0.0)))           .alias("moy_differe"),

    # Flags binaires métier
    F.max(F.when(F.coalesce(F.col("nb_impaye"),F.lit(0)) > 0, 1).otherwise(0))
                                                                     .alias("flag_impaye"),
    F.max(F.when(F.coalesce(F.col("mt_encours_ctx"),F.lit(0)) > 0, 1).otherwise(0))
                                                                     .alias("flag_contentieux"),
    F.max(F.when(F.upper(F.col("produit_wfs")).contains("AUTO"),       1).otherwise(0))
                                                                     .alias("flag_credit_auto"),
    F.max(F.when(F.upper(F.col("produit_wfs")).contains("EQUIPEMENT"), 1).otherwise(0))
                                                                     .alias("flag_credit_equip"),
    F.max(F.when(
        F.upper(F.col("produit_wfs")).contains("PRET") |
        F.upper(F.col("produit_wfs")).contains("PERSONNEL"), 1).otherwise(0))
                                                                     .alias("flag_credit_perso"),
    F.max(F.when(F.upper(F.col("type_prel_actuel")).contains("PRELEVEMENT"), 1).otherwise(0))
                                                                     .alias("flag_prel_prelevement"),

    # Contentieux
    F.sum(F.when(F.col("date_entree_ctx").isNotNull(), 1).otherwise(0))
                                                                     .alias("nb_credits_ctx"),

    # Délais
    F.avg("retard_echeance")                                         .alias("moy_retard_echeance"),
    F.avg("delai_extraction")                                        .alias("moy_delai_extraction"),

    # Modes (canal, réseau, bien)
    F.first("canal_prov")                                            .alias("canal_prov_principal"),
    F.first("code_reseau")                                           .alias("code_reseau_principal"),
    F.first("type_bien")                                             .alias("type_bien_principal"),
)

print(f"   → Après agrégation : {gold_aff.count():,} lignes")


# ================================================================
# ÉTAPE 4 — CIBLAGE
# Agrégation GROUP BY tiers_client UNIQUEMENT
# ================================================================
print("\n[4/7] ciblage — agrégations campagnes")

df_cib = read_silver("ciblage")

df_cib = df_cib.withColumn("periode_j_ts",
    F.to_timestamp(F.col("periode_j"), "ddMMMyyyy:HH:mm:ss"))

gold_cib = df_cib.groupBy("tiers_client").agg(

    F.countDistinct("rtc")                                           .alias("nb_campagnes"),
    F.count("*")                                                     .alias("nb_contacts_total"),
    F.countDistinct("periode_j")                                     .alias("nb_jours_cibles"),
    F.datediff(
        F.to_date(F.max("periode_j_ts")),
        F.to_date(F.min("periode_j_ts"))
    ).cast(DoubleType())                                             .alias("duree_ciblage_jours"),

    # SMS — NULL → 0 (pas reçu = 0)
    F.sum(F.coalesce(F.col("nb_sms_recu"),    F.lit(0.0)))           .alias("nb_sms_total"),
    F.sum(F.coalesce(F.col("nb_sms_failed"),  F.lit(0.0)))           .alias("nb_sms_failed"),
    F.sum(F.coalesce(F.col("nb_voice_recu"),  F.lit(0.0)))           .alias("nb_voice_total"),
    F.sum(F.coalesce(F.col("nb_voice_failed"),F.lit(0.0)))           .alias("nb_voice_failed"),

    F.max(F.when(F.upper(F.col("flag_canal")) == "VOICE", 1).otherwise(0))
                                                                     .alias("flag_canal_voice"),
)

print(f"   → Après agrégation : {gold_cib.count():,} lignes (sans période)")


# ================================================================
# ÉTAPE 5 — SAV
# Agrégation GROUP BY id_tiers_siebel + periode_trt
# ================================================================
print("\n[5/7] sav — agrégations + flags catégories")

df_sav = read_silver("sav")

df_sav = df_sav \
    .withColumn("date_creation_ts", parse_date("date_creation"))

df_sav = df_sav.withColumn("duree_traitement",
    F.when(
        F.col("date_fin").isNotNull() &
        F.col("date_trt_extr").isNotNull() &
        F.col("date_creation_ts").isNotNull(),
        F.datediff(
            F.to_date("date_trt_extr"),
            F.to_date("date_creation_ts")
        ).cast(DoubleType())
    )
)

gold_sav = df_sav.groupBy("id_tiers_siebel", "periode_trt").agg(

    F.count("*")                                                     .alias("nb_sav"),
    F.countDistinct("agence_creation")                               .alias("nb_agences"),
    F.countDistinct("num_affaire")                                   .alias("nb_affaires"),

    # Flags catégories
    F.max(F.when(F.upper(F.col("categorie")).contains("RECOUVREMENT"),  1).otherwise(0)).alias("flag_recouvrement"),
    F.max(F.when(F.upper(F.col("categorie")).contains("FIDELISATION"),  1).otherwise(0)).alias("flag_fidelisation"),
    F.max(F.when(F.upper(F.col("categorie")).contains("MODIFICATION"),  1).otherwise(0)).alias("flag_modification"),

    # Flags sous-catégories
    F.max(F.when(F.upper(F.col("sous_categorie")).contains("OPPOSITION"),   1).otherwise(0)).alias("flag_opposition"),
    F.max(F.when(F.upper(F.col("sous_categorie")).contains("CLOTURE"),      1).otherwise(0)).alias("flag_cloture"),
    F.max(F.when(F.upper(F.col("sous_categorie")).contains("ATTESTATION"),  1).otherwise(0)).alias("flag_attestation_fin"),
    F.max(F.when(F.upper(F.col("sous_categorie")).contains("RACHAT"),       1).otherwise(0)).alias("flag_rachat_sav"),
    F.max(F.when(
        F.upper(F.col("sous_categorie")).contains("REPORT") &
        F.upper(F.col("sous_categorie")).contains("ECHEANCE"),              1).otherwise(0)).alias("flag_report_echeance"),
    F.max(F.when(
        F.upper(F.col("sous_categorie")).contains("SITUATION") &
        F.upper(F.col("sous_categorie")).contains("CREDIT"),                1).otherwise(0)).alias("flag_situation_credit"),

    # Flags statuts
    F.max(F.when(F.upper(F.col("statut_demande")).isin(
        "OUVERTE","EN COURS","EN ATTENTE","INITIEE",
        "EN_COURS_TRAITEMENT","EN_ATTENTE_COMPLEMENT"),               1).otherwise(0)).alias("flag_sav_actif"),
    F.max(F.when(F.upper(F.col("statut_demande")).isin(
        "CLOTUREE","TRAITEE","TRAITE_AVEC_FORCAGE"),                  1).otherwise(0)).alias("flag_sav_cloture"),
    F.max(F.when(F.upper(F.col("statut_demande")).isin(
        "ANNULEE","REJETEE","EN_ECHEC","DOUBLON"),                    1).otherwise(0)).alias("flag_sav_annule"),

    F.max(F.when(F.upper(F.col("canal")).contains("TEL"), 1).otherwise(0)).alias("flag_canal_tel"),

    # Dates
    F.min("date_creation_ts")                                        .alias("date_premier_sav"),
    F.max("date_creation_ts")                                        .alias("date_dernier_sav"),

    # Métriques
    F.avg("duree_traitement")                                        .alias("moy_duree_traitement"),
    F.sum(F.when(F.col("date_fin").isNull(), 1).otherwise(0))        .alias("nb_sav_non_clotures"),
    F.sum(F.when(F.col("date_fin_theorique").isNull(), 1).otherwise(0)).alias("nb_sav_sans_delai"),
)

print(f"   → Après agrégation : {gold_sav.count():,} lignes")


# ================================================================
# ÉTAPE 6 — RECLAMATION
# ================================================================
print("\n[6/7] reclamation — agrégations")

df_recla = read_silver("reclamation")

df_recla = df_recla.withColumn("date_creation_ts", parse_date("date_creation"))

gold_recla = df_recla.groupBy("id_tiers_siebel", "periode_trt").agg(

    F.countDistinct("id_demande")                                    .alias("nb_reclamations"),
    F.max(F.when(
        F.upper(F.col("sous_categorie")).contains("DOUBLE") &
        F.upper(F.col("sous_categorie")).contains("PRELEV"),          1).otherwise(0))
                                                                     .alias("flag_double_prelevement"),
    F.min("date_creation_ts")                                        .alias("date_premiere_recla"),
    F.max("date_creation_ts")                                        .alias("date_derniere_recla"),
    F.sum(F.when(F.col("date_fin").isNull(), 1).otherwise(0))        .alias("nb_recla_non_clotures"),
)

print(f"   → Après agrégation : {gold_recla.count():,} lignes")


# ================================================================
# ÉTAPE 7 — DEMANDE_INFO
# ================================================================
print("\n[7/7] demande_info — agrégations + flags")

df_dem = read_silver("demande_info")

df_dem = df_dem.withColumn("date_creation_ts", parse_date("date_creation"))

gold_dem = df_dem.groupBy("id_tiers_siebel", "periode_trt").agg(

    F.countDistinct("id_demande")                                    .alias("nb_demandes"),
    F.max(F.when(F.upper(F.col("categorie")).contains("PRET"),        1).otherwise(0)).alias("flag_demande_pret"),
    F.max(F.when(F.upper(F.col("categorie")).contains("SAV"),         1).otherwise(0)).alias("flag_sav_demande"),
    F.max(F.when(F.upper(F.col("sous_categorie")).contains("SIMULATION"),   1).otherwise(0)).alias("flag_simulation"),
    F.max(F.when(F.upper(F.col("sous_categorie")).contains("RACHAT"),       1).otherwise(0)).alias("flag_rachat_credit"),
    F.max(F.when(
        F.upper(F.col("sous_categorie")).contains("REPORT") &
        F.upper(F.col("sous_categorie")).contains("ECHEANCE"),        1).otherwise(0)).alias("flag_report_echeance_dem"),
    F.min("date_creation_ts")                                        .alias("date_premiere_demande"),
    F.max("date_creation_ts")                                        .alias("date_derniere_demande"),
)

print(f"   → Après agrégation : {gold_dem.count():,} lignes")


# ================================================================
# ÉTAPE 8 — JOIN FINAL
# Base : flag_transfo (8 570 101 lignes)
# ================================================================
print("\n" + "=" * 60)
print("JOIN FINAL — assemblage dataset ML")
print("=" * 60)

# Renommer les colonnes periode_trt dans sav/recla/dem
# pour éviter ambiguïté dans le JOIN
gold_sav   = gold_sav.withColumnRenamed("periode_trt",   "periode_trt_sav")
gold_recla = gold_recla.withColumnRenamed("periode_trt", "periode_trt_recla")
gold_dem   = gold_dem.withColumnRenamed("periode_trt",   "periode_trt_dem")

dataset_ml = gold_flag \
    .join(df_sig,
          on=["tiers_client", "periode_trt"],
          how="left") \
    .join(gold_aff,
          on=["tiers_client", "periode_trt"],
          how="left") \
    .join(gold_cib,
          on="tiers_client",         # ← pas de période !
          how="left") \
    .join(gold_sav,
          on=[
              F.col("id_tiers_siebel") == F.col("id_tiers_siebel"),
              F.col("periode_trt") == F.col("periode_trt_sav")
          ],
          how="left") \
    .drop("periode_trt_sav") \
    .join(gold_recla,
          on=[
              F.col("id_tiers_siebel") == F.col("id_tiers_siebel"),
              F.col("periode_trt") == F.col("periode_trt_recla")
          ],
          how="left") \
    .drop("periode_trt_recla") \
    .join(gold_dem,
          on=[
              F.col("id_tiers_siebel") == F.col("id_tiers_siebel"),
              F.col("periode_trt") == F.col("periode_trt_dem")
          ],
          how="left") \
    .drop("periode_trt_dem")

# NULL logique → 0 après JOIN
# (client sans SAV = 0 SAV, pas NULL)
dataset_ml = dataset_ml \
    .withColumn("nb_sav",
        F.coalesce(F.col("nb_sav"),           F.lit(0)).cast(IntegerType())) \
    .withColumn("nb_reclamations",
        F.coalesce(F.col("nb_reclamations"),  F.lit(0)).cast(IntegerType())) \
    .withColumn("nb_demandes",
        F.coalesce(F.col("nb_demandes"),      F.lit(0)).cast(IntegerType())) \
    .withColumn("nb_campagnes",
        F.coalesce(F.col("nb_campagnes"),     F.lit(0)).cast(IntegerType())) \
    .withColumn("nb_credits",
        F.coalesce(F.col("nb_credits"),       F.lit(0)).cast(IntegerType()))


# ================================================================
# ÉTAPE 9 — CONTRÔLE QUALITÉ
# ================================================================
print("\n" + "=" * 60)
print("CONTRÔLE QUALITÉ")
print("=" * 60)

total    = dataset_ml.count()
flag1    = dataset_ml.filter(F.col("flag_transfo") == 1).count()
doublons = dataset_ml \
    .groupBy("tiers_client", "periode_trt") \
    .count() \
    .filter(F.col("count") > 1) \
    .count()
periodes = dataset_ml.select("periode_trt").distinct().count()

print(f"   Total lignes        : {total:>12,}  (attendu : 8 570 101)")
print(f"   Flag=1 training     : {flag1:>12,}  (attendu : ~4 070)")
print(f"   Doublons            : {doublons:>12,}  (attendu : 0)")
print(f"   Périodes distinctes : {periodes:>12,}  (attendu : 13)")
print(f"   Colonnes            : {len(dataset_ml.columns):>12,}")

print("\n   Distribution par période :")
dataset_ml.groupBy("periode_trt") \
    .agg(
        F.count("*").alias("nb_clients"),
        F.sum("flag_transfo").alias("nb_flag1")
    ).orderBy("periode_trt").show(15)


# ================================================================
# ÉTAPE 10 — SAUVEGARDE GOLD PARQUET
# Partitionné par is_prediction_period
# partition=0 → training | partition=1 → scoring
# ================================================================
print("\n💾 Sauvegarde Gold Parquet ...")

dataset_ml.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("is_prediction_period") \
    .save(f"{GOLD_PATH}/dataset_ml_spark")

print(f"   ✅ {GOLD_PATH}/dataset_ml_spark")
print(f"      partition=0 → training  (012025–122025)")
print(f"      partition=1 → scoring   (012026)")


# ================================================================
# ÉTAPE 11 — SAUVEGARDE POSTGRESQL
# Schéma spark_ → comparaison avec marts_marts.dataset_ml_v3
# ================================================================
print("\n💾 Écriture PostgreSQL schéma spark_ ...")

# Créer schéma si inexistant (via JDBC)
dataset_ml.repartition(50).write \
    .jdbc(
        url=JDBC_URL,
        table="spark_.dataset_ml_spark",
        mode="overwrite",
        properties=JDBC_PROPS
    )

print("   ✅ Table : spark_.dataset_ml_spark")
print("   → Prête pour comparaison avec marts_marts.dataset_ml_v3")


print("\n" + "=" * 60)
print("✅ GOLD TERMINÉ — Dataset ML prêt")
print("=" * 60)

spark.stop()