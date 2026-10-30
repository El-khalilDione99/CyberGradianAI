import os

# Registre des modèles (table models) en mémoire pendant les tests :
# aucun fichier ni base PostgreSQL requis.
os.environ.setdefault("POSTGRES_DSN", "sqlite://")

# engine/scoring_api/app.py charge .env.local à l'import, sans écraser les
# variables déjà définies : on fixe ici le volume attendu par test_simulator
# pour que la configuration locale (SIM_NB_ABONNES=1500) ne fuie pas dans les tests.
os.environ.setdefault("SIM_NB_ABONNES", "500")
