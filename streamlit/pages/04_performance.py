import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os

st.set_page_config(page_title="Performance", page_icon="📈", layout="wide")
st.title("📈 Performance du Modèle")
st.markdown("---")

# ── KPIs modèle ──────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("AUC-ROC",     "0.8773", "+0.077 vs aléatoire")
col2.metric("Recall",      "82.7%",  "Vrais souscripteurs")
col3.metric("Overfitting", "0.0344", "Train-Test gap")
col4.metric("Arbres",      "480",    "early stopping")

st.markdown("---")

# ── Comparaison modèles ──────────────────────────────────────────
st.subheader("📊 Comparaison des 4 modèles")

df_models = pd.DataFrame({
    'Modèle' : ['Logistic Regression', 'Random Forest', 'XGBoost v3', 'LightGBM v3'],
    'AUC'    : [0.8603, 0.8480, 0.8773, 0.8768],
    'Recall' : [0.8577, 0.6577, 0.8269, 0.8231],
    'Overfit': [0.0171, 0.0500, 0.0344, 0.0338]
})

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        df_models, x='Modèle', y='AUC',
        color='AUC',
        color_continuous_scale='RdYlGn',
        text='AUC',
        title='AUC-ROC par modèle'
    )
    fig.update_traces(texttemplate='%{text:.4f}', textposition='outside')
    fig.update_layout(showlegend=False, yaxis_range=[0.5, 1.0])
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig2 = px.bar(
        df_models, x='Modèle', y='Recall',
        color='Recall',
        color_continuous_scale='RdYlGn',
        text='Recall',
        title='Recall par modèle'
    )
    fig2.update_traces(texttemplate='%{text:.4f}', textposition='outside')
    fig2.update_layout(showlegend=False, yaxis_range=[0, 1.1])
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ── Images SHAP ──────────────────────────────────────────────────
st.subheader("📉 Graphiques SHAP")

IMG_PATH = "C:/Users/saadb/pfe-scoring-credit/sql/resultats/"

col1, col2 = st.columns(2)
with col1:
    img1 = IMG_PATH + "21_shap_importance.png"
    if os.path.exists(img1):
        st.image(img1, caption="SHAP Feature Importance",
                 use_container_width=True)
    else:
        st.warning(f"Image non trouvée : {img1}")

with col2:
    img2 = IMG_PATH + "22_shap_beeswarm.png"
    if os.path.exists(img2):
        st.image(img2, caption="SHAP Beeswarm",
                 use_container_width=True)
    else:
        st.warning(f"Image non trouvée : {img2}")

st.markdown("---")

# ── Overfitting check ────────────────────────────────────────────
st.subheader("🔍 Overfitting Check — XGBoost v3")

df_overfit = pd.DataFrame({
    'Version' : ['v1', 'v2', 'v3', 'Optuna'],
    'AUC Train': [0.9636, 0.9229, 0.9111, 0.9185],
    'AUC Test' : [0.8505, 0.8762, 0.8773, 0.8758],
    'Overfit'  : [0.1131, 0.0468, 0.0344, 0.0412]
})

fig3 = go.Figure()
fig3.add_trace(go.Bar(
    name='AUC Train',
    x=df_overfit['Version'],
    y=df_overfit['AUC Train'],
    marker_color='steelblue'
))
fig3.add_trace(go.Bar(
    name='AUC Test',
    x=df_overfit['Version'],
    y=df_overfit['AUC Test'],
    marker_color='coral'
))
fig3.update_layout(
    barmode='group',
    yaxis_range=[0.5, 1.0],
    title='Evolution AUC Train vs Test — XGBoost'
)
st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")


st.subheader("📋 Tableau récapitulatif")
st.dataframe(df_models, use_container_width=True)

st.caption("Saad Bouali — M2 Big Data & Cloud Computing — Wafasalaf 2026")