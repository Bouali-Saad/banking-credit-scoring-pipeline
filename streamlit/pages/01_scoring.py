import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Scoring", page_icon="🎯", layout="wide")
st.title("🎯 Scoring 012026")
st.markdown("---")

# ── Charger données ──────────────────────────────────────────────
@st.cache_data
def load_scoring():
    return pd.read_csv(
        "C:/Users/saadb/pfe-scoring-credit/data/scoring_012026_tous.csv"
    )

df = load_scoring()

# ── KPIs ─────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
col1.metric("Total clients",  f"{len(df):,}")
col2.metric("Clients ciblés", f"{df['flag_cible'].sum():,}")
col3.metric("Taux ciblage",   f"{df['flag_cible'].mean()*100:.1f}%")

st.markdown("---")

# ── Distribution probabilités ────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Distribution des probabilités")
    fig = px.histogram(
        df, x='proba_souscription',
        nbins=50,
        color_discrete_sequence=['steelblue'],
        labels={'proba_souscription': 'Probabilité'}
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Distribution par décile")
    decile_counts = df.groupby('score_decile').size().reset_index(name='nb_clients')
    decile_proba  = df.groupby('score_decile')['proba_souscription'].mean().reset_index()
    decile_counts = decile_counts.merge(decile_proba, on='score_decile')

    fig2 = px.bar(
        decile_counts,
        x='score_decile', y='nb_clients',
        color='proba_souscription',
        color_continuous_scale='RdYlGn',
        labels={'score_decile': 'Décile', 'nb_clients': 'Nb clients',
                'proba_souscription': 'Proba moy'},
        text='nb_clients'
    )
    fig2.update_traces(texttemplate='%{text:,}', textposition='outside')
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ── Tableau déciles ──────────────────────────────────────────────
st.subheader("Résumé par décile")
df_decile = df.groupby('score_decile').agg(
    nb_clients    =('tiers_client', 'count'),
    proba_moyenne =('proba_souscription', 'mean'),
    proba_max     =('proba_souscription', 'max'),
    proba_min     =('proba_souscription', 'min')
).reset_index()

df_decile['proba_moyenne'] = df_decile['proba_moyenne'].round(4)
df_decile['proba_max']     = df_decile['proba_max'].round(4)
df_decile['proba_min']     = df_decile['proba_min'].round(4)
df_decile['recommandation'] = df_decile['score_decile'].map({
    10: '✅✅✅ Priorité max',
    9 : '✅✅ Contacter',
    8 : '✅ Si budget',
    7 : '⚠️ Borderline',
    6 : '⚠️ Borderline',
    5 : '❌ Peu probable',
    4 : '❌ Peu probable',
    3 : '❌ Ne pas contacter',
    2 : '❌ Ne pas contacter',
    1 : '❌ Ne pas contacter'
})

st.dataframe(df_decile, use_container_width=True)