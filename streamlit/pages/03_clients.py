import streamlit as st
import pandas as pd

st.set_page_config(page_title="Clients", page_icon="👥", layout="wide")
st.title("👥 Clients Ciblés 012026")
st.markdown("---")

@st.cache_data
def load_scoring():
    return pd.read_csv(
        "C:/Users/saadb/pfe-scoring-credit/data/scoring_012026_tous.csv"
    )

df = load_scoring()


st.subheader("🔎 Filtres")
col1, col2 = st.columns(2)

with col1:
    seuil = st.slider(
        "Seuil probabilité",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.05
    )

with col2:
    deciles = st.multiselect(
        "Déciles",
        options=list(range(1, 11)),
        default=[9, 10]
    )


df_filtre = df[
    (df['proba_souscription'] >= seuil) &
    (df['score_decile'].isin(deciles))
].sort_values('proba_souscription', ascending=False)


col1, col2, col3 = st.columns(3)
col1.metric("Clients filtrés",  f"{len(df_filtre):,}")
col2.metric("Proba moyenne",    f"{df_filtre['proba_souscription'].mean():.4f}" if len(df_filtre) > 0 else "N/A")
col3.metric("% du total",       f"{len(df_filtre)/len(df)*100:.1f}%")

st.markdown("---")


st.subheader(f"Liste clients — {len(df_filtre):,} résultats")
st.dataframe(
    df_filtre.reset_index(drop=True),
    use_container_width=True
)


st.markdown("---")
csv = df_filtre.to_csv(index=False).encode('utf-8')
st.download_button(
    label="⬇️ Télécharger CSV",
    data=csv,
    file_name=f"clients_cibles_seuil{seuil}.csv",
    mime='text/csv'
)