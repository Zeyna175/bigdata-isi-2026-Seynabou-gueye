"""
Module de transformations pures pour le nettoyage des données clients.

TP 3 — Big Data Engineering — Master 1 — ISI
Chaque fonction : DataFrame -> DataFrame, pure (pas de read/write/show).

Note : la colonne "ville" du générateur de données contient en réalité des
adresses complètes (numéro + rue), pas des noms de ville. La vraie ville se
trouve dans la colonne "region". La fonction normaliser_ville cible donc
"region" pour produire la colonne "ville_norm".
"""

from pyspark.sql import DataFrame, functions as F


def unifier_manquants(df: DataFrame) -> DataFrame:
    """Emails "" / "N/A" -> null."""
    e = F.trim(F.col("email"))
    return df.withColumn(
        "email",
        F.when(e.isin("", "N/A", "n/a", "NULL"), None).otherwise(e))


def normaliser_email(df: DataFrame) -> DataFrame:
    """Email en minuscules + trim ; drapeau de validite."""
    motif = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    return df.withColumn(
        "email_valide",
        F.when(F.col("email").isNull(), F.lit(None))
         .otherwise(F.col("email").rlike(motif)))


def normaliser_ville(df: DataFrame) -> DataFrame:
    """region (affichage) + ville_norm (cle sans accent), sans UDF.

    Note : la colonne 'ville' du generateur contient en realite des adresses
    completes ; la vraie ville se trouve dans la colonne 'region'.
    """
    accents = "àâäéèêëîïôöùûüçÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ"
    sans_accents = "aaaeeeeiioouuucAAAEEEEIIOOUUUC"

    df = df.withColumn("region", F.initcap(F.trim(F.col("region"))))
    return df.withColumn(
        "ville_norm",
        F.lower(F.translate(F.trim(F.col("region")), accents, sans_accents)))


def normaliser_telephone(df: DataFrame) -> DataFrame:
    """9 chiffres, prefixe 70/75/76/77/78 ; drapeau de validite."""
    tel = F.regexp_replace(F.col("telephone"), r"[^0-9]", "")
    tel = F.regexp_replace(tel, r"^221", "")
    return (df.withColumn("tel_norm", tel)
              .withColumn("tel_valide",
                  tel.rlike(r"^(70|75|76|77|78)\d{7}$")))


def valider_naissance(df: DataFrame) -> DataFrame:
    """Date plausible entre 1920 et aujourd'hui, sinon null."""
    d = F.to_date(F.col("date_naissance"), "yyyy-MM-dd")
    return df.withColumn(
        "date_naissance",
        F.when((d >= F.lit("1920-01-01")) & (d <= F.current_date()), d)
         .otherwise(None))


def dedupliquer_clients(df: DataFrame) -> DataFrame:
    """Doublons exacts puis 1 ligne par customer_id."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("customer_id").orderBy(
        F.col("date_inscription").desc())
    return (df.dropDuplicates()
              .withColumn("_r", F.row_number().over(w))
              .filter(F.col("_r") == 1)
              .drop("_r"))


def nettoyer_clients(df: DataFrame) -> DataFrame:
    """Pipeline complet de nettoyage des clients."""
    return (df
        .transform(unifier_manquants)
        .transform(normaliser_email)
        .transform(normaliser_ville)
        .transform(normaliser_telephone)
        .transform(valider_naissance)
        .transform(dedupliquer_clients))