# Qualité des données — customers.csv

**TP 3 — Big Data Engineering — Master 1 — ISI**

Mesures effectuées sur `customers.csv` (échelle 0.1, 5025 lignes), avant toute
correction, comme demandé : on mesure avant de corriger.

## Tableau de diagnostic

| Défaut | Mesure | Colonne |
|---|---|---|
| Emails manquants (null + "" + "N/A") | **150** | `email` |
| Villes/régions distinctes avant normalisation | **499** | `region` (voir note) |
| Villes/régions distinctes après normalisation | **19** | `region` → `ville_norm` |
| Doublons exacts (count - distinct) | **15** | (toutes colonnes) |
| Lignes brutes | **5025** | — |
| Lignes après nettoyage complet | **5000** | — |

## Note importante sur les colonnes

La colonne `ville` du générateur de données (`generate_data.py`) contient en
réalité des **adresses complètes** (ex: "4, Rue De Maréchal"), pas des noms
de ville. La vraie ville/région du client se trouve dans la colonne `region`
(ex: "DAKAR", "thiès", "THIÈS"). La fonction `normaliser_ville` du module
`src/transformations.py` a donc été adaptée pour cibler `region` et produire
la colonne `ville_norm`, plutôt que la colonne `ville`.

## Décisions prises

- **Emails manquants** : conservés (mis à `null`) plutôt que supprimés, avec
  un drapeau `email_valide` pour un filtrage ultérieur si besoin.
- **Villes/régions** : normalisation par `trim` + `initcap` (affichage) et
  suppression des accents via `F.translate` (clé `ville_norm`), sans UDF
  Python — voir note technique.
- **Téléphones** : validation par regex sur les préfixes sénégalais
  (70/75/76/77/78) après nettoyage des caractères non numériques et du
  préfixe international `221`.
- **Dates de naissance** : bornées entre 1920-01-01 et la date du jour ;
  hors de ces bornes, mises à `null`.
- **Déduplication** : effectuée **après** les normalisations (email, ville),
  pour détecter aussi les quasi-doublons.

## Note technique — UDF évitée

La fonction de retrait d'accents était initialement écrite comme UDF Python,
mais celle-ci provoquait un crash systématique du worker Python de Spark sur
cet environnement Windows. Elle a été remplacée par `F.translate()`, une
fonction Spark native — conforme à la règle « fonctions intégrées d'abord ».

## État des tests (pytest)

Le module `src/transformations.py` est fonctionnel : le pipeline complet
`nettoyer_clients` s'exécute avec succès dans `notebooks/TP3_nettoyage.ipynb`
(5025 → 5000 lignes après nettoyage, résultats cohérents et vérifiés).

Les tests unitaires (`tests/test_transformations.py`) sont écrits et
couvrent les 6 fonctions avec un cas piège chacune, comme demandé. 
**Les tests passent avec succès via `pytest`** ✓ : tous les cas de test
sont validés et les résultats correspondent aux attentes définies pour
chaque fonction du module de transformation.