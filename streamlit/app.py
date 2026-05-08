

import streamlit as st

st.set_page_config(
    page_title="Credit Scoring — Wafasalaf",
    page_icon="🏦",
    layout="wide"
)

# ── Header ───────────────────────────────────────────────────────
st.title("🏦 Credit Scoring Pipeline")
st.subheader("Wafasalaf — PFE Data Science 2025-2026")
st.markdown("---")

# ── KPIs principaux ──────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

col1.metric(
    label="Clients scorés",
    value="688 443",
    delta="Janvier 2026"
)
col2.metric(
    label="Clients ciblés",
    value="139 425",
    delta="Seuil 0.5"
)
col3.metric(
    label="AUC-ROC",
    value="0.877",
    delta="+0.077 vs aléatoire"
)
col4.metric(
    label="Recall",
    value="82.7%",
    delta="Vrais souscripteurs détectés"
)

st.markdown("---")

# ── Description ──────────────────────────────────────────────────
st.markdown("""
### 📋 Description du projet

Pipeline complet de scoring crédit bancaire :

| Étape | Description | Statut |
|-------|-------------|--------|
| **Mois 1** | EDA + Tests statistiques | ✅ Terminé |
| **Mois 2** | dbt Bronze/Silver/Gold | ✅ Terminé |
| **Mois 3** | XGBoost + MLflow + Scoring | ✅ Terminé |
| **Mois 4** | Streamlit Dashboard | 🔄 En cours |
| **Mois 5** | Docker + CI/CD | ⏳ À venir |
| **Mois 6** | Airflow Pipeline | ⏳ À venir |

### 🔧 Stack technique
- **Data Engineering** : PostgreSQL, dbt, PySpark
- **Machine Learning** : XGBoost, LightGBM, SHAP, MLflow
- **Dashboard** : Streamlit, Plotly
- **MLOps** : Docker, Airflow (à venir)
""")

st.markdown("---")
st.caption("Saad Bouali — M2 Big Data & Cloud Computing — Wafasalaf 2026")