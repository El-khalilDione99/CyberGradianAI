import os

# Registre des modèles (table models) en mémoire pendant les tests :
# aucun fichier ni base PostgreSQL requis.
os.environ.setdefault("POSTGRES_DSN", "sqlite://")
