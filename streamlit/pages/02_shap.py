import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="SHAP", page_icon="🔍", layout="wide")
st.title("🔍 Explainability SHAP")
st.markdown("---")


@st.cache_data
def load_shap():
    return pd.DataFrame({
        'feature': [
            'nb_sav', 'aff_moy_delai_extraction', 'anciennete_emploi',
            'moy_mt_apport', 'flag_sav_cloture', 'sav_moy_delai_extraction',
            'flag_sav_actif', 'recence_sav', 'type_client_Professionel',
            'sav_nb_agences', 'charges', 'age', 'moy_mensualite_av_der',
            'nb_jours_dernier_evt', 'groupe_civilite_HOMME',
            'revenu', 'anciennete_annees', 'nb_sav_non_clotures',
            'sum_mt_equip', 'flag_credit_perso'
        ],
        'shap_importance': [
            0.3482, 0.3311, 0.1903, 0.1860, 0.1811,
            0.1555, 0.1533, 0.1472, 0.1169, 0.1106,
            0.1078, 0.1030, 0.0866, 0.0864, 0.0824,
            0.0822, 0.0806, 0.0770, 0.0757, 0.0691
        ],
        'source': [
            'SAV', 'Affaire', 'Signaletique',
            'Affaire', 'SAV', 'SAV',
            'SAV', 'SAV', 'Signaletique',
            'SAV', 'Signaletique', 'Signaletique', 'Affaire',
            'Signaletique', 'Signaletique',
            'Signaletique', 'Signaletique', 'SAV',
            'Affaire', 'Affaire'
        ]
    }).sort_values('shap_importance', ascending=True)

df_shap = load_shap()


col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Top 20 Features — Importance SHAP")
    fig = px.bar(
        df_shap,
        x='shap_importance', y='feature',
        color='source',
        orientation='h',
        color_discrete_map={
            'SAV'          : '#2196F3',
            'Affaire'      : '#4CAF50',
            'Signaletique' : '#FF9800',
            'Ciblage'      : '#9C27B0'
        },
        labels={
            'shap_importance': 'Importance SHAP',
            'feature'        : 'Feature',
            'source'         : 'Table source'
        }
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Par table source")
    source_imp = df_shap.groupby('source')['shap_importance'].sum().reset_index()
    fig2 = px.pie(
        source_imp,
        values='shap_importance',
        names='source',
        color_discrete_map={
            'SAV'          : '#2196F3',
            'Affaire'      : '#4CAF50',
            'Signaletique' : '#FF9800',
            'Ciblage'      : '#9C27B0'
        }
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")


st.subheader("💡 Insights clés")

col1, col2, col3 = st.columns(3)

col1.info("""
**🏦 Comportement SAV dominant**

Les variables SAV occupent 6 des 10 
premières positions → un client actif 
avec la banque est le meilleur signal 
de souscription
""")

col2.success("""
**💰 Profil financier important**

moy_mt_apport + sum_mt_equip + charges
→ la capacité financière du client
est un prédicteur fort
""")

col3.warning("""
**⏱️ Ancienneté = fidélité**

anciennete_emploi + anciennete_annees
→ un client stable professionnellement
et fidèle à la banque souscrit plus
""")


st.markdown("---")
st.subheader("📊 Graphiques SHAP originaux")

col1, col2 = st.columns(2)
with col1:
    st.image(
        "C:/Users/saadb/pfe-scoring-credit/sql/resultats/21_shap_importance.png",
        caption="SHAP Bar Plot", use_column_width=True
    )
with col2:
    st.image(
        "C:/Users/saadb/pfe-scoring-credit/sql/resultats/22_shap_beeswarm.png",
        caption="SHAP Beeswarm", use_column_width=True
    )