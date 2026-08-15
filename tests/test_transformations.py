"""
Tests du module src/transformations.py.

Un test par fonction, avec un cas piège pour chacune, comme demandé au TP3.
"""

from src.transformations import (
    unifier_manquants,
    normaliser_email,
    normaliser_ville,
    normaliser_telephone,
    valider_naissance,
    dedupliquer_clients,
)


def test_unifier_manquants(spark):
    """Cas piège : une ligne 'N/A', une ligne '', une ligne valide -> deux null."""
    entree = spark.createDataFrame(
        [("C1", "N/A"), ("C2", ""), ("C3", "vraie@mail.com")],
        ["customer_id", "email"])
    res = unifier_manquants(entree)
    emails = {r["customer_id"]: r["email"] for r in res.collect()}

    assert emails["C1"] is None
    assert emails["C2"] is None
    assert emails["C3"] == "vraie@mail.com"


def test_normaliser_email(spark):
    """Email en minuscules + trim, avec drapeau de validite."""
    entree = spark.createDataFrame(
        [("C1", "  Test@Mail.COM  "), ("C2", "pas-un-email"), ("C3", None)],
        ["customer_id", "email"])
    res = normaliser_email(entree)
    rows = {r["customer_id"]: r for r in res.collect()}

    assert rows["C1"]["email"] == "test@mail.com"
    assert rows["C1"]["email_valide"] is True
    assert rows["C2"]["email_valide"] is False
    assert rows["C3"]["email_valide"] is None


def test_normaliser_ville(spark):
    """Cas piège : 'Thiès' accentué et 'THIES' -> meme cle ville_norm."""
    entree = spark.createDataFrame(
        [("C1", "  Thiès  "), ("C2", "THIES")],
        ["customer_id", "region"])
    res = normaliser_ville(entree)
    cles = [r["ville_norm"] for r in res.select("ville_norm").collect()]

    assert set(cles) == {"thies"}


def test_normaliser_telephone(spark):
    """Cas piège : +221 77..., 77-..., un numero invalide -> drapeau correct."""
    entree = spark.createDataFrame(
        [("C1", "+221 77 123 45 67"),
         ("C2", "77-234-56-78"),
         ("C3", "12 345 67 89")],
        ["customer_id", "telephone"])
    res = normaliser_telephone(entree)
    rows = {r["customer_id"]: r for r in res.collect()}

    assert rows["C1"]["tel_valide"] is True
    assert rows["C2"]["tel_valide"] is True
    assert rows["C3"]["tel_valide"] is False


def test_valider_naissance(spark):
    """Date hors bornes (avant 1920 ou future) -> null."""
    entree = spark.createDataFrame(
        [("C1", "1990-05-12"), ("C2", "1850-01-01"), ("C3", "2099-01-01")],
        ["customer_id", "date_naissance"])
    res = valider_naissance(entree)
    rows = {r["customer_id"]: r["date_naissance"] for r in res.collect()}

    assert rows["C1"] is not None
    assert rows["C2"] is None
    assert rows["C3"] is None


def test_dedupliquer_clients(spark):
    """Cas piège : un quasi-doublon (email en MAJUSCULES) -> une seule ligne
    apres normalisation prealable."""
    entree = spark.createDataFrame(
        [("C1", "TEST@MAIL.COM", "2024-01-01"),
         ("C1", "test@mail.com", "2024-02-01")],
        ["customer_id", "email", "date_inscription"])

    normalise = normaliser_email(unifier_manquants(entree))
    res = dedupliquer_clients(normalise)

    assert res.count() == 1
    assert res.collect()[0]["date_inscription"] == "2024-02-01"