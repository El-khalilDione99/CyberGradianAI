# Dictionnaire de Features — CyberGuardian AI
**Equipe IA · Version 2.0 · Juillet 2026**

---

## Introduction

Ce document decrit l'ensemble des features utilisees par le moteur de scoring CyberGuardian AI pour detecter la fraude SIM swap en temps reel.

Les features sont organisees en **trois niveaux** :

| Niveau | Source | Disponibilite |
|---|---|---|
| **Brutes** | Topics Kafka directement | Immediate (dans l'evenement) |
| **Calculees** | Feature Updater (IA-3) depuis le profil Redis | < 5ms (profil en memoire) |
| **Derivees** | Calculees au moment du scoring | < 1ms (calcul a la volee) |

---

## 1. Modele interne — Compte (profil abonne)

Le **Compte** est l'objet central du simulateur. Il represente un abonne mobile money senegalais.
Il est cree au demarrage pour chacun des 500 abonnes simules et sert de base a :
- La generation des evenements sur 30 jours
- L'initialisation du profil dans Redis / DynamoDB
- Le calcul de reference des features derivees (z-score, ratio montant, etc.)

> **Important** : Le Compte **n'est pas publie sur Kafka**. C'est la memoire interne du simulateur.
> Le `msisdn` est hache immediatement a la creation — jamais stocke en clair.

| Champ | Type | Description | Exemple |
|---|---|---|---|
| `id_compte` | str | Identifiant unique du compte. Genere par SHA256(msisdn), prefixe `CPT-`. Jamais le vrai numero de telephone | `CPT-7e16cdf07c7a` |
| `iccid_actuel` | str | Numero de serie de la carte SIM physique actuelle (19 chiffres). Change lors d'un swap SIM frauduleux | `0838637940265423511` |
| `imsi_actuel` | str | Identifiant reseau de la SIM (15 chiffres, prefixe `608` = Orange Senegal). Change aussi lors d'un swap | `608615594078161` |
| `region` | str | Region senegalaise de residence. Valeurs : `dakar`, `thies`, `saint_louis`, `kaolack`, `ziguinchor`, `tambacounda`, etc. | `thies` |
| `antenne_domicile` | str | Antenne reseau habituelle de residence (format `REG-ANT-XXX`). Reference pour detecter les deplacements anormaux | `THI-ANT-004` |
| `segment` | str | Niveau de revenus : `bas`, `moyen`, `haut`. Determine les montants habituels et le solde initial | `bas` |
| `device_id_habituel` | str | Identifiant de l'appareil mobile principal (format `DEV-xxxx`). Reference pour detecter un nouvel appareil inconnu | `DEV-8D9D9B03` |
| `montant_moyen_habituel` | float | Montant moyen que cet abonne envoie habituellement (XOF). Tire d'une **loi log-normale** pour simuler la diversite entre petits et grands comptes | `5 602.00` |
| `ecart_type_montant` | float | Dispersion autour du montant moyen. Determine si l'abonne est regulier ou irregulier dans ses montants | `2 240.80` |
| `solde` | float | Solde courant simule (XOF). Mis a jour a chaque transaction generee | `25 762.00` |
| `date_creation_compte` | date | Date d'ouverture du compte mobile money. Un compte recent est moins fiable qu'un compte ancien | `2024-03-15` |
| `heures_actives` | list[int] | Plage horaire habituelle de l'abonne. Derivee du segment : `bas`=[8-18h], `moyen`=[7-21h], `haut`=[6-23h] | `[8, 9, 10, 11, 12, 13]` |
| `beneficiaires_habituels` | list[str] | Liste de 2 a 5 `id_compte` de beneficiaires connus (famille, marchands reguliers) | `["CPT-a3f2...", "CPT-b4c3..."]` |

---

## 2. Features brutes — Topic `transactions`

Ces champs sont presents dans chaque message du topic `transactions`.
Ils sont disponibles **immediatement** sans aucun calcul supplementaire.
C'est le topic le plus volumineux (~22 000 messages pour 500 abonnes sur 30 jours).

| Feature | Type | Description | Exemple |
|---|---|---|---|
| `id_transaction` | str | Identifiant unique de la transaction, prefixe `TXN-` | `TXN-A3F9B2C1` |
| `id_compte` | str | Compte qui effectue la transaction — hash du msisdn, jamais le vrai numero | `CPT-7e16cdf07c7a` |
| `horodatage` | str ISO 8601 | Date et heure exacte de la transaction avec fuseau UTC | `2026-07-14T08:32:11+00:00` |
| `montant` | float | Montant de la transaction en XOF. Minimum : 500 XOF | `12 500.00` |
| `devise` | str | Toujours `XOF` (Franc CFA Ouest-Africain) | `XOF` |
| `type_transaction` | str | Nature de l'operation : `transfert` (50%), `paiement` (25%), `retrait` (20%), `depot` (5%) | `transfert` |
| `id_beneficiaire` | str | Identifiant du destinataire. Beneficiaire connu (`CPT-xxxx`) 90% du temps, inconnu (`BEN-xxxx`) dans les fraudes | `CPT-a3f2b1c4` |
| `device_id` | str | Appareil depuis lequel la transaction est initiee. Appareil habituel (95%) ou inconnu (`DEV-xxxx`) dans les fraudes | `DEV-8D9D9B03` |
| `antenne` | str | Antenne reseau depuis laquelle la transaction est emise (format `REG-ANT-XXX`) | `DAK-ANT-003` |
| `solde_avant` | float | Solde du compte avant la transaction (XOF) | `85 400.00` |
| `solde_apres` | float | Solde du compte apres la transaction (XOF). Toujours >= 0 | `72 900.00` |
| `id_scenario` | str / null | Identifiant du scenario scripte si cette transaction fait partie d'une attaque. `null` pour le trafic normal | `SCN-B2C3D4E5` |
| `label_fraude` | int | Verite terrain : `1` = fraude, `0` = legitime. Sert a entrainer et evaluer les modeles | `0` |

---

## 3. Features brutes — Topic `sim-events`

Ces champs sont presents dans chaque message du topic `sim-events`.
Ce topic est peu volumineux (~23 messages sur 30 jours) mais **tres discriminant** —
chaque swap SIM est un signal fort qui doit declencher une vigilance immediate.

| Feature | Type | Description | Exemple |
|---|---|---|---|
| `id_evenement` | str | Identifiant unique de l'evenement SIM, prefixe `SIM-` | `SIM-9B3D2A1F` |
| `id_compte` | str | Compte concerne par le swap SIM | `CPT-7e16cdf07c7a` |
| `horodatage` | str ISO 8601 | Date et heure exacte du swap SIM | `2026-07-14T09:15:00+00:00` |
| `ancien_iccid` | str | Numero de serie de l'ancienne SIM physique (19 chiffres) | `1234567890123456789` |
| `nouveau_iccid` | str | Numero de serie de la nouvelle SIM (19 chiffres) | `9876543210987654321` |
| `ancien_imsi` | str | Ancien identifiant reseau SIM (15 chiffres, prefixe `608` Senegal) | `608123456789012` |
| `nouveau_imsi` | str | Nouvel identifiant reseau SIM — change aussi lors d'un swap | `608987654321098` |
| `type_sim` | str | Type de la nouvelle SIM : `physique` (85%) ou `esim` (15%) | `physique` |
| `canal_swap` | str | Canal par lequel le swap a ete effectue. Fraude : 60% `agence`, 30% `self_service`, 10% `centre_appel` | `agence` |
| `delai_otp_swap_minutes` | float | **Signal cle** — Delai en minutes entre la demande OTP et le swap. Fraude : 1-8 min (precipitation). Legitime : 5-90 min | `3.2` |
| `antenne_otp` | str | Antenne depuis laquelle l'OTP a ete demande (position de l'attaquant) | `MAT-ANT-002` |
| `antenne_swap` | str | Antenne depuis laquelle le swap a ete effectue en agence | `MAT-ANT-002` |
| `device_id` | str | Appareil utilise lors du swap. Dans une fraude : toujours un device inconnu du compte | `DEV-ATK12345678` |
| `id_scenario` | str / null | Identifiant du scenario scripte | `SCN-B2C3D4E5` |
| `label_fraude` | int | `1` si swap frauduleux, `0` si swap legitime | `1` |

---

## 4. Features brutes — Topic `otp-events`

Ces champs sont presents dans chaque message du topic `otp-events`.
Presents uniquement dans les scenarios avec swap (fraude ou legitime).
Un pic d'OTP (5 a 10 demandes en quelques minutes) est un signal d'attaque precoce.

| Feature | Type | Description | Exemple |
|---|---|---|---|
| `id_otp` | str | Identifiant unique de la demande OTP, prefixe `OTP-` | `OTP-2C8A4F1B` |
| `id_compte` | str | Compte qui a demande le code OTP | `CPT-7e16cdf07c7a` |
| `horodatage` | str ISO 8601 | Date et heure de la demande OTP | `2026-07-14T09:11:48+00:00` |
| `motif` | str | Raison de la demande OTP. Toujours `confirmation_swap` dans les scenarios simules | `confirmation_swap` |
| `antenne` | str | Antenne depuis laquelle l'OTP a ete demande (position de l'attaquant) | `MAT-ANT-002` |
| `id_scenario` | str / null | Identifiant du scenario scripte | `SCN-B2C3D4E5` |

---

## 5. Features calculees — Profil abonne (Redis / DynamoDB)

Ces features sont **maintenues en continu** par le Feature Updater (IA-3).
Elles sont lues depuis Redis en < 5ms au moment du scoring.
Elles representent l'etat comportemental courant de chaque abonne.

### 5.1 Identite et geographie

| Feature | Type | Description |
|---|---|---|
| `id_compte` | str | Cle primaire du profil |
| `region` | str | Region de residence de l'abonne |
| `segment` | str | Niveau de revenus : `bas`, `moyen`, `haut` |
| `antenne_domicile` | str | Antenne habituelle de residence |
| `antennes_connues` | list[str] | Toutes les antennes vues dans l'historique de l'abonne — enrichi par le feature updater |
| `date_creation` | str | Date d'ouverture du compte (anciennete = signal de confiance) |

### 5.2 SIM et appareils

| Feature | Type | Description |
|---|---|---|
| `iccid_actuel` | str | ICCID de la SIM active — mis a jour apres chaque swap |
| `imsi_actuel` | str | IMSI de la SIM active — mis a jour apres chaque swap |
| `ts_dernier_swap` | str / null | Timestamp du dernier swap SIM — **feature la plus critique du modele** |
| `nb_swaps_30j` | int | Nombre de swaps SIM dans les 30 derniers jours |
| `device_id_habituel` | str | Appareil principal de l'abonne |
| `devices_connus` | list[str] | Tous les appareils deja vus pour cet abonne |

### 5.3 Beneficiaires

| Feature | Type | Description |
|---|---|---|
| `beneficiaires_connus` | list[str] | Destinataires habituels de l'abonne — mis a jour a chaque nouvelle transaction |

### 5.4 Statistiques Welford (moyenne et variance en ligne)

Ces statistiques sont mises a jour a chaque transaction **sans recalculer tout l'historique** (algorithme de Welford — O(1) par mise a jour).

| Feature | Type | Description |
|---|---|---|
| `nb_transactions` | int | Nombre total de transactions observees depuis la creation du profil |
| `montant_moyen` | float | Moyenne glissante des montants (XOF) — mise a jour en continu |
| `montant_m2_welford` | float | Variable intermediaire Welford — permet de calculer la variance sans l'historique complet |
| `ecart_type_montant` | float | Ecart-type des montants — derive de montant_m2_welford |
| `montant_moyen_habituel` | float | Montant moyen issu du profil initial (amorcage log-normal) — reference stable |

### 5.5 Velocite — fenetres glissantes

| Feature | Type | Description |
|---|---|---|
| `nb_tx_1h` | int | Nombre de transactions dans la derniere heure glissante |
| `nb_tx_24h` | int | Nombre de transactions dans les 24 dernieres heures |
| `nb_tx_7j` | int | Nombre de transactions dans les 7 derniers jours |
| `total_montant_24h` | float | Montant total envoye dans les 24 dernieres heures (XOF) |
| `fenetre_1h_ts` | list[str] | Timestamps des transactions de la derniere heure (pour expiration automatique) |
| `fenetre_24h_ts` | list[str] | Timestamps des transactions des 24 dernieres heures |

### 5.6 OTP

| Feature | Type | Description |
|---|---|---|
| `nb_otp_1h` | int | Nombre de demandes OTP dans la derniere heure — pic = signal d'attaque |
| `nb_otp_24h` | int | Nombre de demandes OTP dans les 24 dernieres heures |
| `fenetre_otp_1h_ts` | list[str] | Timestamps des demandes OTP de la derniere heure |

### 5.7 Solde et comportement

| Feature | Type | Description |
|---|---|---|
| `solde` | float | Solde courant estime (XOF) — mis a jour a chaque transaction |
| `heures_actives` | list[int] | Heures habituelles de transaction de l'abonne |

---

## 6. Features derivees — Calculees au moment du scoring

Ces features sont calculees a la volee par le moteur de scoring en combinant
les donnees brutes de la transaction et le profil Redis de l'abonne.
Elles ne sont **pas stockees** — recalculees a chaque transaction en < 1ms.

| Feature | Formule | Interpretation | Signal |
|---|---|---|---|
| `heures_depuis_swap` | `(horodatage_tx - ts_dernier_swap) / 3600` | Temps ecoule depuis le dernier swap SIM. **< 2h = tres suspect**. Si null : pas de swap recent | Critique |
| `z_score_montant` | `(montant - montant_moyen) / ecart_type_montant` | Ecart du montant a la normale de l'abonne. **> 3 = anormal** | Haute |
| `ratio_montant` | `montant / montant_moyen_habituel` | Ratio par rapport au comportement habituel. **> 5 = suspect** | Haute |
| `taux_vidage` | `montant / solde_avant` | Proportion du solde emportee. **> 0.8 = vidage du compte** | Haute |
| `est_nouveau_device` | `device_id non present dans devices_connus` | Appareil jamais vu pour cet abonne | Haute |
| `est_beneficiaire_inconnu` | `id_beneficiaire non present dans beneficiaires_connus` | Destinataire jamais vu — signal fort dans les fraudes | Haute |
| `est_antenne_etrangere` | `antenne non presente dans antennes_connues` | Antenne hors de la zone habituelle de l'abonne | Moyenne |
| `heure_inhabituelle` | `heure de la transaction non presente dans heures_actives` | Transaction hors des horaires habituels de l'abonne | Moyenne |
| `velocite_1h_anormale` | `nb_tx_1h > moyenne_velocite + 2 ecarts-types` | Pic de transactions dans la derniere heure (cascade frauduleuse) | Haute |
| `otp_spike` | `nb_otp_1h >= 3` | Rafale de demandes OTP — signal precoce fort d'une attaque en cours | Critique |
| `solde_post_tx_faible` | `solde_apres / solde_avant < 0.2` | Plus de 80% du solde emporte en une transaction | Haute |
| `anciennete_compte_jours` | `(horodatage_tx - date_creation).days` | Compte recent = moins d'historique = moins de confiance | Moyenne |

---

## 7. Politique de scoring et decision

Le moteur combine les 3 couches pour produire un score de 0 a 100 :

```
Score final = max(score_regles, combinaison_ponderee(score_anomalie, score_supervise))
```

| Score | Decision | Action systeme |
|---|---|---|
| 0 - 29 | `PASS` | Transaction autorisee |
| 30 - 69 | `CHALLENGE` | Verification supplementaire demandee (SMS / biometrie) |
| 70 - 89 | `BLOCK` | Transaction bloquee + alerte creee pour l'analyste |
| 90 - 100 | `BLOCK` | Blocage immediat + alerte priorite haute |

---

## 8. Exemple de scoring — Scenario SIM_SWAP_SIMPLE

```
Evenements recus :
  09:11  -> otp-events    : demande OTP depuis antenne MAT-ANT-002
  09:14  -> sim-events    : swap SIM, canal=agence, delai_otp=3.2 min
  09:22  -> transactions  : transfert 185 000 XOF vers BEN-inconnu

Features calculees au moment du scoring de la transaction :
  heures_depuis_swap       = 0.13h       (8 minutes apres le swap)
  z_score_montant          = 12.4        (tres anormal)
  ratio_montant            = 8.7x        (8.7x la moyenne habituelle)
  taux_vidage              = 0.73        (73% du solde emporte)
  est_nouveau_device       = True        (DEV inconnu du compte)
  est_beneficiaire_inconnu = True        (BEN-xxxx jamais vu)
  est_antenne_etrangere    = True        (MAT hors zone habituelle)
  otp_spike                = True        (pic OTP detecte)

Resultat :
  Couche 1 (regles)   -> score 95  [regle swap_recent + regle otp_spike]
  Couche 2 (anomalie) -> score 0.92 (Isolation Forest)
  Couche 3 (XGBoost)  -> score 0.97
  Score final         -> 97
  Decision            -> BLOCK (priorite haute)
  Temps de traitement -> 18ms
```

---

## 9. Resume — Features par niveau d'importance

| Priorite | Feature | Pourquoi |
|---|---|---|
| Critique | `heures_depuis_swap` | 90% des fraudes dans les 15 min suivant le swap |
| Critique | `otp_spike` | Signal precoce avant meme la transaction frauduleuse |
| Haute | `z_score_montant` | Detecte les montants anormaux par rapport aux habitudes de l'abonne |
| Haute | `ratio_montant` | Montant vs habitudes — discriminant a 2.2x en moyenne |
| Haute | `taux_vidage` | Vidage du compte — discriminant a 1.7x en moyenne |
| Haute | `est_nouveau_device` | Device inconnu = signal fort, souvent combine avec swap |
| Haute | `est_beneficiaire_inconnu` | Destinataire inconnu = signal fort dans 95% des fraudes |
| Haute | `velocite_1h_anormale` | Cascade de transferts rapides = vidage en plusieurs etapes |
| Moyenne | `est_antenne_etrangere` | Utile combine avec swap — l'attaquant opere depuis une autre zone |
| Moyenne | `heure_inhabituelle` | Signal faible seul, fort en combinaison avec d'autres features |
| Moyenne | `anciennete_compte_jours` | Contexte de confiance — un compte recent est plus vulnerable |

---

*CyberGuardian AI — Equipe IA — Juillet 2026*
*Ce dictionnaire correspond a la version 2 du simulateur (simulation 30 jours, modele Compte).*
*Il est mis a jour a chaque evolution du modele de donnees.*
