

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os



PROJECT_ROOT = "C:/Users/saadb/pfe-scoring-credit"
BRONZE_PATH  = f"{PROJECT_ROOT}/data_lake/output/bronze"
JDBC_JAR     = f"{PROJECT_ROOT}/drivers/postgresql-42.7.3.jar"


JDBC_URL   = "jdbc:postgresql://localhost:5433/pfe_credit_dw"
JDBC_PROPS = {
    "user"    : "postgres",
    "password": "Saad2002",
    "driver"  : "org.postgresql.Driver"
}



spark = SparkSession.builder \
    .appName("Bronze_Ingestion") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath", JDBC_JAR) \
    .config("spark.executor.extraClassPath", JDBC_JAR) \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")





def read_raw(table_name):
    
    print(f"\n Lecture raw.{table_name} ...")
    df = spark.read.jdbc(
        url=JDBC_URL,
        table=f'raw."{table_name}"',
        properties=JDBC_PROPS
    )
    nb = df.count()
    print(f"   → {nb:,} lignes | {len(df.columns)} colonnes")
    return df


def save_parquet(df, name, partition_col=None):
    
    path = f"{BRONZE_PATH}/{name}"
    writer = df.write.mode("overwrite").format("parquet")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(path)
    msg = f"partitionné par [{partition_col}]" if partition_col else "non partitionné"
    print(f"    Sauvegardé → {path}  ({msg})")


# ================================================================
# TABLE 1 — FLAG_TRANSFO
# 8 570 101 lignes | clé : TIERS_CLIENT + PERIODE_TRT
# ================================================================
print("\n[1/7] flag_transfo")
df = read_raw("flag_transfo")
# Copie brute — aucune transformation
save_parquet(df, "flag_transfo", partition_col="PERIODE_TRT")


# ================================================================
# TABLE 2 — TABLE_SIGNALETIQUE
# 8 758 138 lignes | clé : TIERS_CLIENT + ID_TIERS_SIEBEL
# ================================================================
print("\n[2/7] table_signaletique")
df = read_raw("table_signaletique")
save_parquet(df, "signaletique", partition_col="PERIODE_TRT")


# ================================================================
# TABLE 3 — TABLE_AFFAIRE
# 15 525 088 lignes | clé : TIERS_CLIENT + IE_AFFAIRE
# ================================================================
print("\n[3/7] table_affaire")
df = read_raw("table_affaire")
save_parquet(df, "affaire", partition_col="PERIODE_TRT")


# ================================================================
# TABLE 4 — TABLE_CIBLAGE
# 2 960 884 lignes | clé : ID_TIER (= TIERS_CLIENT) + PERIODE_J
# Pas de PERIODE_TRT → pas de partitionnement par période
# ================================================================
print("\n[4/7] table_ciblage")
df = read_raw("table_ciblage")
save_parquet(df, "ciblage")   # pas de partition


# ================================================================
# TABLE 5 — TABLE_SAV
# 2 739 307 lignes | clé : ID_TIERS_SIEBEL
# ================================================================
print("\n[5/7] table_sav")
df = read_raw("table_sav")
save_parquet(df, "sav", partition_col="PERIODE_TRT")


# ================================================================
# TABLE 6 — TABLE_RECLAMATION
# 19 073 lignes | clé : ID_TIERS_SIEBEL
# ================================================================
print("\n[6/7] table_reclamation")
df = read_raw("table_reclamation")
save_parquet(df, "reclamation", partition_col="PERIODE_TRT")


# ================================================================
# TABLE 7 — TABLE_DEMANDE_INFO
# 1 136 205 lignes | clé : ID_TIERS_SIEBEL
# ================================================================
print("\n[7/7] table_demande_info")
df = read_raw("table_demande_info")
save_parquet(df, "demande_info", partition_col="PERIODE_TRT")


# ================================================================
# RÉSUMÉ FINAL
# ================================================================
print("\n" + "=" * 60)
print("BRONZE — Résumé ingestion")
print("=" * 60)

tables = [
    ("flag_transfo",  "PERIODE_TRT"),
    ("signaletique",  "PERIODE_TRT"),
    ("affaire",       "PERIODE_TRT"),
    ("ciblage",       None),
    ("sav",           "PERIODE_TRT"),
    ("reclamation",   "PERIODE_TRT"),
    ("demande_info",  "PERIODE_TRT"),
]

total_lignes = 0
for name, partition in tables:
    path = f"{BRONZE_PATH}/{name}"
    df_check = spark.read.parquet(path)
    nb = df_check.count()
    total_lignes += nb
    part_info = f"partitionné [{partition}]" if partition else "non partitionné"
    print(f"   {name:<20} {nb:>12,} lignes   {part_info}")

print(f"\n   TOTAL : {total_lignes:,} lignes ingérées")
print("=" * 60)
print("✅ BRONZE TERMINÉ")
print("=" * 60)

spark.stop()