"""
engine/rules/engine.py
───────────────────────
Moteur d'évaluation des règles expertes — Couche 1 (IA-4).

Responsabilités :
  - Charger les règles depuis le fichier YAML (RULES_PATH) au démarrage
  - Évaluer chaque règle contre les features dérivées d'une transaction
  - Retourner le score le plus élevé des règles déclenchées + détail explicable
  - Supporter le rechargement à chaud sans redémarrage (reload_rules())
    → En local  : relit le fichier YAML depuis le disque
    → En AWS    : retélécharge depuis S3 via l'ObjectStore

Logiques supportées :
  AND          : toutes les conditions de `signals` doivent être vraies
  SINGLE       : un seul signal (équivalent à AND avec une condition)
  WEAK_SIGNALS : au moins `weak_signals_min` signaux faibles déclenchés

Le moteur retourne toujours un score (0 si aucune règle déclenchée)
et la liste des règles déclenchées pour l'explicabilité.
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

import yaml

from engine.rules.features import compute_features

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────
RULES_PATH   = os.getenv("RULES_PATH", "engine/rules/rules.yaml")
RULES_BUCKET = os.getenv("RULES_BUCKET", "cg-models")
RULES_S3_KEY = os.getenv("RULES_S3_KEY", "rules/rules.yaml")


# ════════════════════════════════════════════════════════════
#  Types de résultats
# ════════════════════════════════════════════════════════════

class RuleMatch:
    """Une règle déclenchée avec son score et les détails d'explicabilité."""

    def __init__(
        self,
        rule_id:     str,
        rule_name:   str,
        score:       int,
        severity:    str,
        signals_triggered: list[dict[str, Any]],
    ) -> None:
        self.rule_id           = rule_id
        self.rule_name         = rule_name
        self.score             = score
        self.severity          = severity
        self.signals_triggered = signals_triggered

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id":           self.rule_id,
            "rule_name":         self.rule_name,
            "score":             self.score,
            "severity":          self.severity,
            "signals_triggered": self.signals_triggered,
        }


class RuleEngineResult:
    """Résultat complet du moteur de règles pour une transaction."""

    def __init__(
        self,
        score:          int,
        matches:        list[RuleMatch],
        features:       dict[str, Any],
        rules_version:  str,
        evaluated_at:   str,
    ) -> None:
        self.score         = score          # score max parmi les règles déclenchées
        self.matches       = matches        # règles déclenchées (triées par score desc)
        self.features      = features       # features dérivées calculées
        self.rules_version = rules_version  # version du fichier rules.yaml
        self.evaluated_at  = evaluated_at

    @property
    def triggered(self) -> bool:
        return len(self.matches) > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "score":         self.score,
            "triggered":     self.triggered,
            "matches":       [m.to_dict() for m in self.matches],
            "rules_version": self.rules_version,
            "evaluated_at":  self.evaluated_at,
            # On n'expose pas toutes les features dans la réponse API
            # pour limiter la taille — seulement celles déclenchées.
            "features_snapshot": {
                k: self.features[k]
                for k in [
                    "amount_ratio", "hours_since_sim_swap", "new_device",
                    "new_beneficiary", "is_roaming", "otp_count_1h",
                    "nb_tx_1h", "zscore_montant",
                ]
                if k in self.features
            },
        }


# ════════════════════════════════════════════════════════════
#  Moteur principal
# ════════════════════════════════════════════════════════════

class RuleEngine:
    """
    Moteur d'évaluation des règles expertes (Couche 1).

    Thread-safe : le rechargement des règles se fait sous verrou RLock
    pour éviter les races conditions si reload_rules() est appelé depuis
    l'endpoint /reload-rules pendant que des requêtes de scoring tournent.

    Usage :
        engine = RuleEngine()
        result = engine.evaluate(event=tx_dict, profile=profile_dict)
        print(result.score, [m.rule_id for m in result.matches])
    """

    def __init__(self, store=None) -> None:
        """
        store : ObjectStore optionnel (injection pour les tests).
                Si None, utilisé uniquement en mode AWS pour charger depuis S3.
        """
        self._store       = store
        self._rules: list[dict] = []
        self._version     = "unknown"
        self._lock        = threading.RLock()
        self._loaded_from = "none"
        self.reload_rules()

    # ── Chargement / rechargement des règles ─────────────────

    def reload_rules(self) -> str:
        """
        Recharge les règles depuis le fichier YAML ou depuis S3/MinIO.

        Stratégie :
          1. Essayer le fichier local (RULES_PATH) — toujours prioritaire
             en local, et fallback fiable en AWS si S3 inaccessible.
          2. Si ENV=aws ET fichier local absent, charger depuis S3.

        Retourne un message décrivant la source utilisée.
        Thread-safe.
        """
        env = os.getenv("ENV", "local")

        # ── Tentative 1 : fichier local ───────────────────────

        if os.path.isfile(RULES_PATH):
            rules, version = self._load_from_file(RULES_PATH)
            with self._lock:
                self._rules       = rules
                self._version     = version
                self._loaded_from = f"file:{RULES_PATH}"
            msg = f"Règles chargées depuis {RULES_PATH} (v{version}, {len(rules)} règles)"
            logger.info(msg)
            return msg

        candidates = [
            RULES_PATH,
            os.path.join(os.path.dirname(__file__), "rules.yaml"),
            "engine/rules/rules.yaml",
            "cyberguardian/engine/rules/rules.yaml",
        ]
        for path in candidates:
            if path and os.path.isfile(path):
                rules, version = self._load_from_file(path)
                with self._lock:
                    self._rules       = rules
                    self._version     = version
                    self._loaded_from = f"file:{path}"
                msg = f"Règles chargées depuis {path} (v{version}, {len(rules)} règles)"
                logger.info(msg)
                return msg


        # ── Tentative 2 : S3 / MinIO ─────────────────────────
        if env == "aws" or self._store is not None:
            try:
                rules, version = self._load_from_object_store()
                with self._lock:
                    self._rules       = rules
                    self._version     = version
                    self._loaded_from = f"s3:{RULES_BUCKET}/{RULES_S3_KEY}"
                msg = (
                    f"Règles chargées depuis S3 {RULES_BUCKET}/{RULES_S3_KEY} "
                    f"(v{version}, {len(rules)} règles)"
                )
                logger.info(msg)
                return msg
            except Exception as exc:
                logger.error("Impossible de charger les règles depuis S3 : %s", exc)

        # ── Échec : règles vides (mode dégradé) ───────────────
        logger.warning(
            "Aucune source de règles disponible — moteur en mode dégradé (score=0)"
        )
        with self._lock:
            self._rules       = []
            self._version     = "degraded"
            self._loaded_from = "none"
        return "Mode dégradé — aucune règle chargée"

    def _load_from_file(self, path: str) -> tuple[list[dict], str]:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        rules   = data.get("rules", [])
        version = data.get("version", datetime.now(timezone.utc).strftime("%Y%m%d"))
        return rules, str(version)

    def _load_from_object_store(self) -> tuple[list[dict], str]:
        from interfaces.store import get_object_store
        store = self._store or get_object_store()
        raw   = store.download(RULES_BUCKET, RULES_S3_KEY)
        data  = yaml.safe_load(raw.decode("utf-8"))
        rules   = data.get("rules", [])
        version = data.get("version", "s3-loaded")
        return rules, str(version)

    # ── Évaluation ───────────────────────────────────────────

    def evaluate(
        self,
        event:   dict[str, Any],
        profile: dict[str, Any],
    ) -> RuleEngineResult:
        """
        Évalue toutes les règles actives contre l'événement courant.

        Paramètres
        ----------
        event   : dict — transaction à scorer (champs : horodatage, montant,
                  device_id, id_beneficiaire, antenne, id_compte…)
        profile : dict — profil abonné depuis Redis/DynamoDB (peut être vide)

        Retourne
        --------
        RuleEngineResult avec le score max et la liste des règles déclenchées.
        """
        # Calcul des features dérivées (module features.py)
        features = compute_features(event, profile)

        matches: list[RuleMatch] = []

        with self._lock:
            rules_snapshot = list(self._rules)
            version        = self._version

        for rule in rules_snapshot:
            match = self._evaluate_rule(rule, features)
            if match:
                matches.append(match)

        # Score = max des règles déclenchées (0 si aucune)
        matches.sort(key=lambda m: m.score, reverse=True)
        score = matches[0].score if matches else 0

        return RuleEngineResult(
            score         = score,
            matches       = matches,
            features      = features,
            rules_version = version,
            evaluated_at  = datetime.now(timezone.utc).isoformat(),
        )

    def _evaluate_rule(
        self,
        rule:     dict[str, Any],
        features: dict[str, Any],
    ) -> RuleMatch | None:
        """
        Évalue une règle individuelle.
        Retourne un RuleMatch si la règle est déclenchée, None sinon.
        """
        logic = rule.get("logic", "AND").upper()

        if logic in ("AND", "SINGLE"):
            return self._eval_and(rule, features)
        elif logic == "WEAK_SIGNALS":
            return self._eval_weak_signals(rule, features)
        else:
            logger.warning("Logic inconnue '%s' pour la règle %s", logic, rule.get("id"))
            return None

    def _eval_and(
        self,
        rule:     dict[str, Any],
        features: dict[str, Any],
    ) -> RuleMatch | None:
        """
        Logique AND : toutes les conditions de `signals` doivent être vraies.
        """
        signals  = rule.get("signals", [])
        triggered = []

        for sig in signals:
            feature_name = sig["feature"]
            val = features.get(feature_name)
            if val is None:
                return None  # Feature absente → règle non applicable
            if not _check_condition(val, sig["operator"], sig["threshold"]):
                return None  # Une condition échoue → règle non déclenchée
            triggered.append({
                "feature":   feature_name,
                "value":     val,
                "operator":  sig["operator"],
                "threshold": sig["threshold"],
            })

        return RuleMatch(
            rule_id           = rule["id"],
            rule_name         = rule["name"],
            score             = rule["score"],
            severity          = rule.get("severity", "Inconnue"),
            signals_triggered = triggered,
        )

    def _eval_weak_signals(
        self,
        rule:     dict[str, Any],
        features: dict[str, Any],
    ) -> RuleMatch | None:
        """
        Logique WEAK_SIGNALS : au moins `weak_signals_min` signaux faibles
        doivent être déclenchés simultanément.
        """
        weak_signals = rule.get("weak_signals", [])
        min_required = rule.get("weak_signals_min", 3)
        triggered    = []

        for sig in weak_signals:
            feature_name = sig["feature"]
            val = features.get(feature_name)
            if val is not None and _check_condition(val, sig["operator"], sig["threshold"]):
                triggered.append({
                    "feature":   feature_name,
                    "value":     val,
                    "operator":  sig["operator"],
                    "threshold": sig["threshold"],
                })

        if len(triggered) >= min_required:
            return RuleMatch(
                rule_id           = rule["id"],
                rule_name         = rule["name"],
                score             = rule["score"],
                severity          = rule.get("severity", "Inconnue"),
                signals_triggered = triggered,
            )
        return None

    # ── Introspection ─────────────────────────────────────────

    @property
    def rules_count(self) -> int:
        with self._lock:
            return len(self._rules)

    @property
    def version(self) -> str:
        with self._lock:
            return self._version

    @property
    def loaded_from(self) -> str:
        with self._lock:
            return self._loaded_from

    def status(self) -> dict[str, Any]:
        """Résumé de l'état du moteur — utile pour /health et /reload-rules."""
        with self._lock:
            return {
                "rules_count":  len(self._rules),
                "version":      self._version,
                "loaded_from":  self._loaded_from,
                "rules_ids":    [r.get("id") for r in self._rules],
            }


# ════════════════════════════════════════════════════════════
#  Évaluateur de conditions
# ════════════════════════════════════════════════════════════

_OPERATORS = {
    "<":  lambda v, t: v < t,
    "<=": lambda v, t: v <= t,
    ">":  lambda v, t: v > t,
    ">=": lambda v, t: v >= t,
    "==": lambda v, t: v == t,
    "!=": lambda v, t: v != t,
}


def _check_condition(value: Any, operator: str, threshold: Any) -> bool:
    """
    Évalue une condition atomique : value <op> threshold.

    Gestion de types :
      - bool vs bool : comparaison directe
      - bool vs str  : convertit "true"/"false" → bool
      - numérique    : comparaison numérique
    """
    # Normalisation du threshold
    if isinstance(threshold, str):
        lower = threshold.lower()
        if lower == "true":
            threshold = True
        elif lower == "false":
            threshold = False

    # Normalisation de la valeur si le threshold est bool
    if isinstance(threshold, bool) and not isinstance(value, bool):
        value = bool(value)

    op_fn = _OPERATORS.get(operator)
    if op_fn is None:
        logger.warning("Opérateur inconnu : '%s'", operator)
        return False

    try:
        return op_fn(value, threshold)
    except TypeError:
        logger.warning(
            "Erreur de comparaison : %r %s %r", value, operator, threshold
        )
        return False
