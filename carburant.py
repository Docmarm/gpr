import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import io
import os
from typing import Dict, List, Tuple, Optional, Any
from dateutil.relativedelta import relativedelta # Pour calculer le nombre de mois

# ---------------------------------------------------------------------
# Constantes et Utilitaires
# ---------------------------------------------------------------------
EXCEL_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DATE_FORMAT = '%Y-%m-%d'

# --- Valeurs par défaut pour les paramètres (seront stockées dans session_state) ---
DEFAULT_SEUIL_HEURES_RAPPROCHEES = 2
DEFAULT_CONSO_SEUIL = 16.0
DEFAULT_HEURE_DEBUT_NON_OUVRE = 20
DEFAULT_HEURE_FIN_NON_OUVRE = 6
DEFAULT_DELTA_MINUTES_FACTURATION_DOUBLE = 60
DEFAULT_SEUIL_ANOMALIES_SUSPECTES_SCORE = 10 # Basé sur un score pondéré

# --- Nouveaux seuils pour le suivi des dotations ---
DEFAULT_SEUIL_SOUS_UTILISATION = 60  # Pourcentage en dessous duquel on considère une sous-utilisation
DEFAULT_SEUIL_SUR_UTILISATION = 110  # Pourcentage au-dessus duquel on considère une sur-utilisation
DEFAULT_MOIS_MIN_ANALYSE = 1  # Nombre minimum de mois pour l'analyse de tendance

# --- Poids par défaut pour le score de risque ---
DEFAULT_POIDS_CONSO_EXCESSIVE = 5
DEFAULT_POIDS_DEPASSEMENT_CAPACITE = 10
DEFAULT_POIDS_PRISES_RAPPROCHEES = 3
DEFAULT_POIDS_KM_DECROISSANT = 8
DEFAULT_POIDS_KM_INCHANGE = 2
DEFAULT_POIDS_KM_SAUT = 6
DEFAULT_POIDS_HORS_HORAIRE = 2
DEFAULT_POIDS_HORS_SERVICE = 9
DEFAULT_POIDS_FACT_DOUBLE = 7

# ---------------------------------------------------------------------
# Initialisation Session State pour les Paramètres
# ---------------------------------------------------------------------
def initialize_session_state(df_vehicules: Optional[pd.DataFrame] = None):
    """Initialise les paramètres dans st.session_state s'ils n'existent pas."""
    defaults = {
        'ss_seuil_heures_rapprochees': DEFAULT_SEUIL_HEURES_RAPPROCHEES,
        'ss_heure_debut_non_ouvre': DEFAULT_HEURE_DEBUT_NON_OUVRE,
        'ss_heure_fin_non_ouvre': DEFAULT_HEURE_FIN_NON_OUVRE,
        'ss_delta_minutes_facturation_double': DEFAULT_DELTA_MINUTES_FACTURATION_DOUBLE,
        'ss_seuil_anomalies_suspectes_score': DEFAULT_SEUIL_ANOMALIES_SUSPECTES_SCORE,
        'ss_poids_conso_excessive': DEFAULT_POIDS_CONSO_EXCESSIVE,
        'ss_poids_depassement_capacite': DEFAULT_POIDS_DEPASSEMENT_CAPACITE,
        'ss_poids_prises_rapprochees': DEFAULT_POIDS_PRISES_RAPPROCHEES,
        'ss_poids_km_decroissant': DEFAULT_POIDS_KM_DECROISSANT,
        'ss_poids_km_inchange': DEFAULT_POIDS_KM_INCHANGE,
        'ss_poids_km_saut': DEFAULT_POIDS_KM_SAUT,
        'ss_poids_hors_horaire': DEFAULT_POIDS_HORS_HORAIRE,
        'ss_poids_hors_service': DEFAULT_POIDS_HORS_SERVICE,
        'ss_poids_fact_double': DEFAULT_POIDS_FACT_DOUBLE,
        'ss_conso_seuils_par_categorie': {}, # Sera peuplé dynamiquement
        'ss_seuil_sous_utilisation': DEFAULT_SEUIL_SOUS_UTILISATION,  # Nouveau seuil 
        'ss_seuil_sur_utilisation': DEFAULT_SEUIL_SUR_UTILISATION,    # Nouveau seuil
        'ss_mois_min_analyse': DEFAULT_MOIS_MIN_ANALYSE,              # Nouveau seuil
        'data_loaded': False # Indicateur de chargement des données
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    # Initialisation dynamique des seuils de conso par catégorie
    if df_vehicules is not None and not st.session_state['ss_conso_seuils_par_categorie']:
        all_cats = sorted(df_vehicules['Catégorie'].dropna().astype(str).unique())
        st.session_state['ss_conso_seuils_par_categorie'] = {cat: DEFAULT_CONSO_SEUIL for cat in all_cats}
    elif df_vehicules is not None:
        # S'assurer que toutes les catégories actuelles ont un seuil
        all_cats = sorted(df_vehicules['Catégorie'].dropna().astype(str).unique())
        current_seuils = st.session_state['ss_conso_seuils_par_categorie']
        updated_seuils = {cat: current_seuils.get(cat, DEFAULT_CONSO_SEUIL) for cat in all_cats}
        st.session_state['ss_conso_seuils_par_categorie'] = updated_seuils


# ---------------------------------------------------------------------
# Fonctions : Nettoyage et chargement (inchangées sauf ajout Type Hints)
# ---------------------------------------------------------------------
def nettoyer_numero_carte(numero: Any) -> str:
    """Convertit un numéro de carte en entier si possible, puis string, retirant les espaces."""
    if pd.isna(numero):
        return ""
    try:
        # Tenter conversion en float puis int pour gérer les ".0" puis en str
        return str(int(float(str(numero)))).strip()
    except ValueError:
         # Si la conversion échoue, retourner le numéro comme string nettoyé
        return str(numero).strip()
    except Exception:
        # Fallback général
        return str(numero).strip()

@st.cache_data(show_spinner="Chargement et nettoyage des fichiers...")
def charger_donnees(fichier_transactions, fichier_cartes) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Charge et nettoie les données des fichiers CSV et Excel."""
    df_transactions, df_vehicules, df_ge, df_autres = None, None, None, None

    # --- Chargement Transactions ---
    try:
        df_transactions = pd.read_csv(fichier_transactions, sep=';', encoding='utf-8', low_memory=False)
        if 'Amount eur' in df_transactions.columns and 'Amount' not in df_transactions.columns:
             df_transactions.rename(columns={'Amount eur': 'Amount'}, inplace=True)
        if 'Place' not in df_transactions.columns and 'Place name' in df_transactions.columns:
             df_transactions.rename(columns={'Place name': 'Place'}, inplace=True)
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier de transactions : {e}")
        return None, None, None, None

    # --- Chargement Cartes ---
    try:
        xls = pd.ExcelFile(fichier_cartes)
        required_sheets = {'CARTES VEHICULE', 'CARTES GE', 'AUTRES CARTES'}
        available_sheets = set(xls.sheet_names)
        if not required_sheets.issubset(available_sheets):
             st.error(f"Feuilles manquantes dans le fichier Excel. Attendues: {required_sheets}. Trouvées: {available_sheets}")
             return None, None, None, None

        df_vehicules = pd.read_excel(xls, sheet_name='CARTES VEHICULE')
        df_ge = pd.read_excel(xls, sheet_name='CARTES GE')
        df_autres = pd.read_excel(xls, sheet_name='AUTRES CARTES')
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier des cartes : {e}")
        return None, None, None, None

    # --- Vérification Colonnes Transactions ---
    colonnes_attendues_transactions = ['Date', 'Hour', 'Card num.', 'Quantity', 'Past mileage', 'Current mileage', 'Amount', 'Place']
    missing_cols_trans = [col for col in colonnes_attendues_transactions if col not in df_transactions.columns]
    if missing_cols_trans:
        st.error(f"Colonnes manquantes dans le fichier de transactions: {', '.join(missing_cols_trans)}")
        return None, None, None, None

    # --- Vérification Colonnes Cartes ---
    colonnes_attendues_cartes = ['N° Carte']
    dfs_cartes = {'CARTES VEHICULE': df_vehicules, 'CARTES GE': df_ge, 'AUTRES CARTES': df_autres}
    for nom_feuille, df_sheet in dfs_cartes.items():
        missing_cols_carte = [col for col in colonnes_attendues_cartes if col not in df_sheet.columns]
        if missing_cols_carte:
            st.error(f"Colonne(s) manquante(s) dans la feuille '{nom_feuille}': {', '.join(missing_cols_carte)}")
            return None, None, None, None
        # Vérifier et convertir 'Cap-rèservoir' si existe
        if 'Cap-rèservoir' in df_sheet.columns:
            df_sheet['Cap-rèservoir'] = pd.to_numeric(df_sheet['Cap-rèservoir'], errors='coerce').fillna(0)
        # NOUVEAU: Vérifier et convertir 'Dotation' si existe
        if 'Dotation' in df_sheet.columns:
            df_sheet['Dotation'] = pd.to_numeric(df_sheet['Dotation'], errors='coerce').fillna(0)
        elif nom_feuille == 'CARTES VEHICULE': # Si absente dans CARTES VEHICULE, créer avec 0 et avertir
            st.warning("La colonne 'Dotation' est manquante dans la feuille 'CARTES VEHICULE'. Elle sera initialisée à 0. Le suivi des dotations ne sera pas significatif.")
            df_sheet['Dotation'] = 0

        # Assurer que Catégorie est string
        if 'Catégorie' in df_sheet.columns:
            df_sheet['Catégorie'] = df_sheet['Catégorie'].astype(str).fillna('Non défini')


    # --- Nettoyage Numéros de Carte ---
    df_transactions['Card num.'] = df_transactions['Card num.'].apply(nettoyer_numero_carte)
    for df_sheet in [df_vehicules, df_ge, df_autres]:
        df_sheet['N° Carte'] = df_sheet['N° Carte'].apply(nettoyer_numero_carte)
        df_sheet.dropna(subset=['N° Carte'], inplace=True) 
        df_sheet = df_sheet[df_sheet['N° Carte'] != ""]

    # --- Conversion Types Transactions ---
    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'], format='%d/%m/%Y', errors='coerce')
    try:
        df_transactions['Hour'] = pd.to_datetime(df_transactions['Hour'], format='%H:%M:%S', errors='coerce').dt.time
    except ValueError:
        try:
             df_transactions['Hour'] = pd.to_datetime(df_transactions['Hour'], format='%H:%M', errors='coerce').dt.time
        except Exception as e:
             st.warning(f"Impossible de parser la colonne 'Hour'. Vérifiez le format (HH:MM:SS ou HH:MM). Erreur: {e}")
             df_transactions['Hour'] = pd.NaT

    for col in ['Quantity', 'Past mileage', 'Current mileage', 'Amount']:
        df_transactions[col] = pd.to_numeric(df_transactions[col].astype(str).str.replace(',', '.'), errors='coerce') 

    # --- Suppression Lignes Invalides ---
    df_transactions.dropna(subset=['Date', 'Card num.'], inplace=True)
    df_transactions = df_transactions[df_transactions['Card num.'] != ""]
    df_vehicules = df_vehicules[df_vehicules['N° Carte'] != ""]
    df_ge = df_ge[df_ge['N° Carte'] != ""]
    df_autres = df_autres[df_autres['N° Carte'] != ""]

    df_transactions['DateTime'] = df_transactions.apply(
        lambda row: datetime.combine(row['Date'].date(), row['Hour']) if pd.notna(row['Date']) and pd.notna(row['Hour']) else pd.NaT,
        axis=1
    )
    df_transactions.dropna(subset=['DateTime'], inplace=True) 

    return df_transactions, df_vehicules, df_ge, df_autres

# ---------------------------------------------------------------------
# Fonctions : export Excel + affichage DataFrame (inchangées)
# ---------------------------------------------------------------------
def to_excel(df: pd.DataFrame) -> bytes:
    """Convertit un DataFrame en un fichier Excel (BytesIO)."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_copy = df.copy()
        for col in df_copy.select_dtypes(include=['datetime64[ns]']).columns:
             df_copy[col] = df_copy[col].dt.strftime(DATE_FORMAT)
        df_copy.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def afficher_dataframe_avec_export(df: pd.DataFrame, titre: str = "Tableau", key: str = "df"):
    """Affiche un DataFrame + un bouton d'export Excel."""
    if df is None or df.empty:
        st.info(f"{titre} : Aucune donnée à afficher.")
        return

    nb_lignes = len(df)
    st.markdown(f"### {titre} ({nb_lignes:,} lignes)")
    st.dataframe(df, use_container_width=True)
    try:
        excel_data = to_excel(df)
        nom_fic = f"{titre.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(':', '')[:50]}.xlsx" 
        st.download_button(
            label=f"Exporter '{titre}' en Excel",
            data=excel_data,
            file_name=nom_fic,
            mime=EXCEL_MIME_TYPE,
            key=f"export_{key}"
        )
    except Exception as e:
        st.error(f"Erreur lors de la génération de l'export Excel pour '{titre}': {e}")


# ---------------------------------------------------------------------
# Fonctions : Vérifications et Analyses (Mises à jour pour utiliser session_state)
# ---------------------------------------------------------------------

def verifier_cartes_inconnues(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, df_ge: pd.DataFrame, df_autres: pd.DataFrame) -> pd.DataFrame:
    """Identifie les transactions associées à des cartes non listées."""
    cartes_vehicules = set(df_vehicules['N° Carte'].unique()) if df_vehicules is not None else set()
    cartes_ge = set(df_ge['N° Carte'].unique()) if df_ge is not None else set()
    cartes_autres = set(df_autres['N° Carte'].unique()) if df_autres is not None else set()
    cartes_valides = cartes_vehicules.union(cartes_ge).union(cartes_autres)

    cartes_utilisees = set(df_transactions['Card num.'].unique())
    cartes_inconnues = cartes_utilisees - cartes_valides

    if not cartes_inconnues:
        return pd.DataFrame() 

    df_inc = df_transactions[df_transactions['Card num.'].isin(cartes_inconnues)].copy()

    if 'Card name' in df_inc.columns:
        stats = df_inc.groupby(['Card num.', 'Card name']).agg(
            Nombre_transactions=('Quantity', 'count'),
            Volume_total_L=('Quantity', 'sum'),
            Montant_total_CFA=('Amount', 'sum')
        ).round(2).reset_index()
    else:
         stats = df_inc.groupby('Card num.').agg(
            Nombre_transactions=('Quantity', 'count'),
            Volume_total_L=('Quantity', 'sum'),
            Montant_total_CFA=('Amount', 'sum')
        ).round(2).reset_index()
         stats['Card name'] = 'Nom non disponible' 

    stats = stats[['Card num.', 'Card name', 'Nombre_transactions', 'Volume_total_L', 'Montant_total_CFA']]
    return stats


def detecter_anomalies(
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame
) -> pd.DataFrame:
    """
    Fonction centrale (simplifiée) pour détecter toutes les anomalies sur les véhicules.
    Retourne un DataFrame unique avec toutes les anomalies détectées.
    """
    all_anomalies = []
    # S'assurer que la colonne 'Dotation' existe dans df_vehicules, sinon l'ajouter avec 0
    cols_vehicules_necessaires = ['N° Carte', 'Nouveau Immat', 'Catégorie', 'Type', 'Cap-rèservoir']
    if 'Dotation' in df_vehicules.columns:
        cols_vehicules_necessaires.append('Dotation')
    
    df_merged = df_transactions.merge(
        df_vehicules[cols_vehicules_necessaires], # Utiliser les colonnes nécessaires
        left_on='Card num.',
        right_on='N° Carte',
        how='inner' 
    )
    df_merged['distance_parcourue'] = df_merged['Current mileage'] - df_merged['Past mileage']
    df_merged['consommation_100km'] = np.where(
         (df_merged['distance_parcourue'] > 0) & df_merged['Quantity'].notna(),
         (df_merged['Quantity'] / df_merged['distance_parcourue']) * 100,
         np.nan
    )

    seuils_conso = st.session_state.get('ss_conso_seuils_par_categorie', {})
    seuil_heures_rapprochees = st.session_state.get('ss_seuil_heures_rapprochees', DEFAULT_SEUIL_HEURES_RAPPROCHEES)
    heure_debut_non_ouvre = st.session_state.get('ss_heure_debut_non_ouvre', DEFAULT_HEURE_DEBUT_NON_OUVRE)
    heure_fin_non_ouvre = st.session_state.get('ss_heure_fin_non_ouvre', DEFAULT_HEURE_FIN_NON_OUVRE)
    delta_minutes_double = st.session_state.get('ss_delta_minutes_facturation_double', DEFAULT_DELTA_MINUTES_FACTURATION_DOUBLE)

    for index, row in df_merged.iterrows():
        cat = row['Catégorie']
        seuil = seuils_conso.get(cat, DEFAULT_CONSO_SEUIL) 
        if pd.notna(row['consommation_100km']) and row['consommation_100km'] > seuil:
            anomalie = row.to_dict()
            anomalie['type_anomalie'] = 'Consommation excessive'
            anomalie['detail_anomalie'] = f"{row['consommation_100km']:.1f} L/100km > seuil {seuil} L/100km"
            anomalie['poids_anomalie'] = st.session_state.get('ss_poids_conso_excessive', DEFAULT_POIDS_CONSO_EXCESSIVE)
            all_anomalies.append(anomalie)

    depassement = df_merged[df_merged['Quantity'] > df_merged['Cap-rèservoir']].copy()
    if not depassement.empty:
         depassement['type_anomalie'] = 'Dépassement capacité'
         depassement['detail_anomalie'] = depassement.apply(lambda x: f"Volume: {x['Quantity']:.1f}L > Capacité: {x['Cap-rèservoir']:.1f}L", axis=1)
         depassement['poids_anomalie'] = st.session_state.get('ss_poids_depassement_capacite', DEFAULT_POIDS_DEPASSEMENT_CAPACITE)
         all_anomalies.extend(depassement.to_dict('records'))

    df_merged_sorted = df_merged.sort_values(['Card num.', 'DateTime'])
    rapprochees_indices = set()
    for carte in df_merged_sorted['Card num.'].unique():
        sub = df_merged_sorted[df_merged_sorted['Card num.'] == carte]
        if len(sub) > 1:
            time_diff = sub['DateTime'].diff().dt.total_seconds() / 3600 
            indices_anomalie = sub.index[time_diff < seuil_heures_rapprochees]
            indices_precedents = sub.index[time_diff.shift(-1) < seuil_heures_rapprochees]
            rapprochees_indices.update(indices_anomalie)
            rapprochees_indices.update(indices_precedents)

    if rapprochees_indices:
        rapprochees_df = df_merged_sorted.loc[list(rapprochees_indices)].copy()
        rapprochees_df['type_anomalie'] = 'Prises rapprochées'
        rapprochees_df['detail_anomalie'] = f'Moins de {seuil_heures_rapprochees}h entre prises'
        rapprochees_df['poids_anomalie'] = st.session_state.get('ss_poids_prises_rapprochees', DEFAULT_POIDS_PRISES_RAPPROCHEES)
        all_anomalies.extend(rapprochees_df.to_dict('records'))

    km_anomalies = []
    for carte in df_merged_sorted['Card num.'].unique():
        df_carte = df_merged_sorted[df_merged_sorted['Card num.'] == carte]
        prev_m = None
        prev_row = None
        for index, row in df_carte.iterrows():
            curr_m = row['Current mileage']
            past_m = row['Past mileage'] 
            user = row.get('Card name', 'N/A')

            if pd.isna(curr_m) or curr_m == 0 or pd.isna(past_m) :
                 prev_m = None 
                 prev_row = row
                 continue
            if past_m > curr_m:
                 anomalie = row.to_dict()
                 anomalie['type_anomalie'] = 'Kilométrage incohérent (transaction)'
                 anomalie['detail_anomalie'] = f"Km début ({past_m}) > Km fin ({curr_m})"
                 anomalie['poids_anomalie'] = st.session_state.get('ss_poids_km_decroissant', DEFAULT_POIDS_KM_DECROISSANT) 
                 km_anomalies.append(anomalie)
                 prev_m = None
                 prev_row = row
                 continue
            if prev_m is not None and prev_row is not None:
                 if curr_m < prev_m:
                     anomalie = row.to_dict()
                     anomalie['type_anomalie'] = 'Kilométrage décroissant (inter-transactions)'
                     anomalie['detail_anomalie'] = f"Km transaction N ({curr_m}) < Km transaction N-1 ({prev_m})"
                     anomalie['poids_anomalie'] = st.session_state.get('ss_poids_km_decroissant', DEFAULT_POIDS_KM_DECROISSANT)
                     km_anomalies.append(anomalie)
                 elif curr_m == prev_m:
                     anomalie = row.to_dict()
                     anomalie['type_anomalie'] = 'Kilométrage inchangé (inter-transactions)'
                     anomalie['detail_anomalie'] = f"Km identique à la transaction précédente: {curr_m} km"
                     anomalie['poids_anomalie'] = st.session_state.get('ss_poids_km_inchange', DEFAULT_POIDS_KM_INCHANGE)
                     km_anomalies.append(anomalie)
                 elif (curr_m - prev_m) > 1000: 
                     anomalie = row.to_dict()
                     anomalie['type_anomalie'] = 'Saut kilométrage important'
                     anomalie['detail_anomalie'] = f"Augmentation de +{curr_m - prev_m} km depuis transaction précédente"
                     anomalie['poids_anomalie'] = st.session_state.get('ss_poids_km_saut', DEFAULT_POIDS_KM_SAUT)
                     km_anomalies.append(anomalie)
            prev_m = curr_m
            prev_row = row
    all_anomalies.extend(km_anomalies)

    df_merged['heure'] = df_merged['DateTime'].dt.hour
    df_merged['jour_semaine'] = df_merged['DateTime'].dt.dayofweek 
    if heure_debut_non_ouvre < heure_fin_non_ouvre: 
        cond_heure = (df_merged['heure'] < heure_fin_non_ouvre) | (df_merged['heure'] >= heure_debut_non_ouvre)
    else: 
        cond_heure = (df_merged['heure'] >= heure_debut_non_ouvre) | (df_merged['heure'] < heure_fin_non_ouvre)
    cond_weekend = (df_merged['jour_semaine'] >= 5) 
    anomalies_hor = df_merged[cond_heure | cond_weekend].copy()
    if not anomalies_hor.empty:
        anomalies_hor['type_anomalie'] = 'Hors Horaires / Weekend'
        anomalies_hor['detail_anomalie'] = anomalies_hor.apply(
            lambda r: f"{r['DateTime'].strftime('%A %H:%M')} " + \
                      ("(Weekend)" if r['jour_semaine'] >= 5 else "") + \
                      ("(Heures non ouvrées)" if (cond_heure.loc[r.name]) else ""), 
            axis=1
        )
        anomalies_hor['poids_anomalie'] = st.session_state.get('ss_poids_hors_horaire', DEFAULT_POIDS_HORS_HORAIRE)
        all_anomalies.extend(anomalies_hor.to_dict('records'))

    hors_service = df_merged[df_merged['Type'].isin(['EN PANNE', 'IMMO'])].copy()
    if not hors_service.empty:
        hors_service['type_anomalie'] = 'Véhicule Hors Service'
        hors_service['detail_anomalie'] = 'Transaction alors que véhicule EN PANNE ou IMMO'
        hors_service['poids_anomalie'] = st.session_state.get('ss_poids_hors_service', DEFAULT_POIDS_HORS_SERVICE)
        all_anomalies.extend(hors_service.to_dict('records'))

    double_indices = set()
    for carte in df_merged_sorted['Card num.'].unique():
        sub = df_merged_sorted[df_merged_sorted['Card num.'] == carte]
        if len(sub) > 1:
            for i in range(len(sub) - 1):
                rowA = sub.iloc[i]
                rowB = sub.iloc[i+1]
                delta_sec = (rowB['DateTime'] - rowA['DateTime']).total_seconds()
                if delta_sec >= 0 and (delta_sec / 60.0) < delta_minutes_double and rowA['Amount'] == rowB['Amount'] and pd.notna(rowA['Amount']):
                    double_indices.add(sub.index[i])
                    double_indices.add(sub.index[i+1])

    if double_indices:
        doubles_df = df_merged_sorted.loc[list(double_indices)].copy()
        doubles_df['type_anomalie'] = 'Facturation double suspectée'
        doubles_df['detail_anomalie'] = f"Montant identique ({doubles_df['Amount']}) en < {delta_minutes_double} min"
        doubles_df['poids_anomalie'] = st.session_state.get('ss_poids_fact_double', DEFAULT_POIDS_FACT_DOUBLE)
        all_anomalies.extend(doubles_df.to_dict('records'))

    if not all_anomalies:
        return pd.DataFrame()

    df_final_anomalies = pd.DataFrame(all_anomalies)
    cols_to_keep = [
        'Date', 'Hour', 'DateTime', 'Card num.', 'Nouveau Immat', 'Catégorie', 'Type',
        'Quantity', 'Amount', 'Past mileage', 'Current mileage', 'distance_parcourue',
        'consommation_100km', 'Cap-rèservoir', 'Place', 'Card name',
        'type_anomalie', 'detail_anomalie', 'poids_anomalie'
    ]
    if 'Dotation' in df_final_anomalies.columns: # Ajouter Dotation si elle a été fusionnée
        cols_to_keep.append('Dotation')

    cols_final = [col for col in cols_to_keep if col in df_final_anomalies.columns]
    df_final_anomalies = df_final_anomalies[cols_final]
    return df_final_anomalies.sort_values(by=['Nouveau Immat', 'DateTime', 'type_anomalie'])


# --- Fonctions d'analyse spécifiques ---

def analyser_consommation_vehicule(vehicule_data: pd.DataFrame, info_vehicule: pd.Series) -> Dict[str, Any]:
    """Analyse la consommation d'un véhicule spécifique."""
    if vehicule_data.empty:
        return {
            'total_litres': 0, 'nb_prises': 0, 'moyenne_prise': 0,
            'distance_totale': 0, 'consommation_moyenne': 0,
            'cout_total': 0, 'cout_moyen_prise': 0, 'cout_par_km': 0,
            'conso_mensuelle': pd.DataFrame(), 'stations_frequentes': pd.Series(dtype='int64'),
            'prix_moyen_litre': 0
        }

    vehicule_data_sorted = vehicule_data.sort_values('DateTime')
    total_litres = vehicule_data_sorted['Quantity'].sum()
    cout_total = vehicule_data_sorted['Amount'].sum()
    nb_prises = len(vehicule_data_sorted)
    moyenne_prise = vehicule_data_sorted['Quantity'].mean() if nb_prises > 0 else 0
    cout_moyen_prise = vehicule_data_sorted['Amount'].mean() if nb_prises > 0 else 0
    prix_moyen_litre = (cout_total / total_litres) if total_litres > 0 else 0

    df_km = vehicule_data_sorted[['Past mileage', 'Current mileage']].dropna()
    distance_totale = 0
    consommation_moyenne = 0
    cout_par_km = 0

    if not df_km.empty and len(df_km) > 1:
        first_km = df_km['Past mileage'].iloc[0]
        last_km = df_km['Current mileage'].iloc[-1]
        if pd.notna(first_km) and pd.notna(last_km) and last_km > first_km:
            distance_totale = last_km - first_km

    vehicule_data_sorted['distance_transaction'] = vehicule_data_sorted['Current mileage'] - vehicule_data_sorted['Past mileage']
    distance_sommee_valide = vehicule_data_sorted.loc[vehicule_data_sorted['distance_transaction'] > 0, 'distance_transaction'].sum()
    if distance_sommee_valide > 0:
        distance_utilisee = distance_sommee_valide
        consommation_moyenne = (total_litres / distance_utilisee) * 100 if distance_utilisee > 0 else 0
        cout_par_km = (cout_total / distance_utilisee) if distance_utilisee > 0 else 0
    elif distance_totale > 0: 
        distance_utilisee = distance_totale
        consommation_moyenne = (total_litres / distance_utilisee) * 100 if distance_utilisee > 0 else 0
        cout_par_km = (cout_total / distance_utilisee) if distance_utilisee > 0 else 0
    else:
        distance_utilisee = 0

    vehicule_data_sorted['mois'] = vehicule_data_sorted['Date'].dt.strftime('%Y-%m')
    conso_mensuelle = vehicule_data_sorted.groupby('mois').agg(
        Volume_L=('Quantity', 'sum'),
        Montant_CFA=('Amount','sum'),
        Nb_prises=('Quantity', 'count'),
        Volume_moyen_L=('Quantity', 'mean')
    )
    stations_freq = vehicule_data_sorted['Place'].value_counts().head(5)

    return {
        'total_litres': total_litres,
        'nb_prises': nb_prises,
        'moyenne_prise': moyenne_prise,
        'distance_totale_estimee': distance_utilisee, 
        'consommation_moyenne': consommation_moyenne,
        'cout_total': cout_total,
        'cout_moyen_prise': cout_moyen_prise,
        'cout_par_km': cout_par_km,
        'conso_mensuelle': conso_mensuelle,
        'stations_frequentes': stations_freq,
        'prix_moyen_litre': prix_moyen_litre
    }

def generer_rapport_vehicule(donnees_vehicule: pd.DataFrame, info_vehicule: pd.Series, date_debut: datetime.date, date_fin: datetime.date, conso_moyenne_categorie: float) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series, Dict[str, Any]]:
    """Génère un rapport détaillé pour un véhicule, incluant le benchmarking."""
    dotation_vehicule = info_vehicule.get('Dotation', 0) # Récupérer la dotation
    
    infos_base_list = [
        ('Immatriculation', info_vehicule.get('Nouveau Immat', 'N/A')),
        ('Marque', info_vehicule.get('Marque', 'N/A')),
        ('Modèle', info_vehicule.get('Modèle', 'N/A')),
        ('Type', info_vehicule.get('Type', 'N/A')),
        ('Catégorie', info_vehicule.get('Catégorie', 'N/A')),
        ('Capacité réservoir', f"{info_vehicule.get('Cap-rèservoir', 0):.0f} L")
    ]
    if 'Dotation' in info_vehicule: # Ajouter la dotation si elle existe
         infos_base_list.append(('Dotation Mensuelle', f"{dotation_vehicule:.0f} L"))
    infos_base_list.extend([
        ('Période début', date_debut.strftime(DATE_FORMAT)),
        ('Période fin', date_fin.strftime(DATE_FORMAT))
    ])
    infos_base = pd.DataFrame(infos_base_list, columns=['Paramètre', 'Valeur'])


    analyse = analyser_consommation_vehicule(donnees_vehicule, info_vehicule)

    conso_veh = analyse['consommation_moyenne']
    ecart_conso = conso_veh - conso_moyenne_categorie if conso_moyenne_categorie > 0 and conso_veh > 0 else 0
    ecart_conso_pct = (ecart_conso / conso_moyenne_categorie) * 100 if conso_moyenne_categorie > 0 and conso_veh > 0 else 0

    stats_conso = pd.DataFrame({
        'Paramètre': [
            'Volume total', 'Coût total', 'Nombre de prises', 'Moyenne par prise (Volume)', 'Moyenne par prise (Coût)',
            'Prix moyen / Litre', 'Distance totale estimée', 'Consommation moyenne', 'Consommation moyenne (Catégorie)',
            'Écart vs Catégorie', 'Coût par Km'
        ],
        'Valeur': [
            f"{analyse['total_litres']:.1f} L", f"{analyse['cout_total']:,.0f} CFA", analyse['nb_prises'],
            f"{analyse['moyenne_prise']:.1f} L", f"{analyse['cout_moyen_prise']:,.0f} CFA", f"{analyse['prix_moyen_litre']:,.0f} CFA/L",
            f"{analyse.get('distance_totale_estimee', 0):,.0f} km",
            f"{conso_veh:.1f} L/100km" if conso_veh > 0 else "N/A",
            f"{conso_moyenne_categorie:.1f} L/100km" if conso_moyenne_categorie > 0 else "N/A",
            f"{ecart_conso:+.1f} L/100km ({ecart_conso_pct:+.1f}%)" if conso_veh > 0 and conso_moyenne_categorie > 0 else "N/A",
            f"{analyse['cout_par_km']:,.1f} CFA/km" if analyse['cout_par_km'] > 0 else "N/A"
        ]
    })
    return infos_base, stats_conso, analyse['conso_mensuelle'], analyse['stations_frequentes'], analyse 


def calculer_kpis_globaux(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date, selected_categories: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Calcule les KPIs de consommation et de coût par catégorie et véhicule."""
    # S'assurer que Dotation est présente dans df_vehicules pour les fusions futures si besoin
    cols_veh_kpi = ['N° Carte', 'Catégorie', 'Nouveau Immat']
    if 'Dotation' in df_vehicules.columns:
        cols_veh_kpi.append('Dotation')

    df = df_transactions.merge(
        df_vehicules[cols_veh_kpi],
        left_on='Card num.', right_on='N° Carte', how='left'
    )
    mask_date = (df['Date'].dt.date >= date_debut) & (df['Date'].dt.date <= date_fin)
    df = df[mask_date].copy()
    if selected_categories:
        df = df[df['Catégorie'].isin(selected_categories)]

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    vehicle_data = []
    df_sorted = df.sort_values(['Card num.', 'DateTime'])

    for card, group in df_sorted.groupby('Card num.'):
        if group.empty: continue
        total_lit = group['Quantity'].sum()
        total_amount = group['Amount'].sum()
        cat = group['Catégorie'].iloc[0]
        immat = group['Nouveau Immat'].iloc[0]
        nb_prises = len(group)
        dotation_mensuelle = group['Dotation'].iloc[0] if 'Dotation' in group.columns else 0


        group_km = group[['Past mileage', 'Current mileage']].dropna()
        dist = 0
        if not group_km.empty and len(group_km) > 1:
             first_km = group_km['Past mileage'].iloc[0]
             last_km = group_km['Current mileage'].iloc[-1]
             if pd.notna(first_km) and pd.notna(last_km) and last_km > first_km:
                 dist = last_km - first_km
        group['dist_transac'] = group['Current mileage'] - group['Past mileage']
        dist_sum_valid = group.loc[group['dist_transac'] > 0, 'dist_transac'].sum()
        distance_utilisee = max(dist, dist_sum_valid) 

        cons = (total_lit / distance_utilisee) * 100 if distance_utilisee > 0 else np.nan
        cpk = (total_amount / distance_utilisee) if distance_utilisee > 0 else np.nan
        avg_price_liter = (total_amount / total_lit) if total_lit > 0 else np.nan

        vehicle_data.append({
            'Card num.': card, 'Nouveau Immat': immat, 'Catégorie': cat,
            'total_litres': total_lit, 'total_cout': total_amount,
            'distance': distance_utilisee, 'consommation': cons, 'cout_par_km': cpk,
            'nb_prises': nb_prises, 'prix_moyen_litre': avg_price_liter,
            'Dotation': dotation_mensuelle # Ajout de la dotation ici
        })
    df_vehicle_kpi = pd.DataFrame(vehicle_data)
    if df_vehicle_kpi.empty:
        return pd.DataFrame(), pd.DataFrame()

    kpi_cat = df_vehicle_kpi.groupby('Catégorie').agg(
        consommation_moyenne=('consommation', 'mean'), 
        cout_par_km_moyen=('cout_par_km', 'mean'),
        total_litres=('total_litres', 'sum'),
        total_cout=('total_cout', 'sum'),
        distance_totale=('distance', 'sum'),
        nb_vehicules=('Card num.', 'nunique'),
        nb_transactions=('nb_prises', 'sum')
    ).reset_index()

    kpi_cat['consommation_globale'] = (kpi_cat['total_litres'] / kpi_cat['distance_totale']) * 100
    kpi_cat['cout_par_km_global'] = kpi_cat['total_cout'] / kpi_cat['distance_totale']
    kpi_cat['prix_moyen_litre_global'] = kpi_cat['total_cout'] / kpi_cat['total_litres']

    kpi_cat = kpi_cat.round({
        'consommation_moyenne': 1, 'cout_par_km_moyen': 1, 'total_litres': 0, 'total_cout': 0,
        'distance_totale': 0, 'consommation_globale': 1, 'cout_par_km_global': 1, 'prix_moyen_litre_global': 0
    })
    df_vehicle_kpi = df_vehicle_kpi.round({
         'total_litres': 1, 'total_cout': 0, 'distance': 0, 'consommation': 1, 'cout_par_km': 1,
         'prix_moyen_litre': 0, 'Dotation':0
    })
    return kpi_cat, df_vehicle_kpi


# ---------------------------------------------------------------------
# Fonctions d'agrégation des anomalies pour les résumés
# ---------------------------------------------------------------------

def calculer_score_risque(df_anomalies: pd.DataFrame) -> pd.DataFrame:
    """Calcule un score de risque pondéré par véhicule basé sur les anomalies."""
    if df_anomalies.empty or 'poids_anomalie' not in df_anomalies.columns:
        return pd.DataFrame(columns=['Nouveau Immat', 'Card num.', 'Catégorie', 'Nombre total anomalies', 'Score de risque'])

    pivot = df_anomalies.groupby(['Nouveau Immat', 'Card num.', 'Catégorie', 'type_anomalie']).agg(
        nombre=('type_anomalie', 'size'),
        score_partiel=('poids_anomalie', 'sum') 
    ).reset_index()

    summary = pivot.groupby(['Nouveau Immat', 'Card num.', 'Catégorie']).agg(
        nombre_total_anomalies=('nombre', 'sum'),
        score_risque=('score_partiel', 'sum')
    ).reset_index()
    return summary.sort_values('score_risque', ascending=False)

# ---------------------------------------------------------------------
# NOUVELLE FONCTION : Analyse consommation par période
# ---------------------------------------------------------------------
def analyser_consommation_par_periode(
    df_transactions: pd.DataFrame, 
    df_vehicules: pd.DataFrame, 
    date_debut: datetime.date, 
    date_fin: datetime.date, 
    periode: str = 'M', 
    selected_categories: List[str] = None,
    selected_vehicles: List[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analyse la consommation de carburant par période (jour, semaine, mois, trimestre, année)
    """
    cols_veh_periode = ['N° Carte', 'Catégorie', 'Nouveau Immat', 'Cap-rèservoir']
    if 'Dotation' in df_vehicules.columns:
        cols_veh_periode.append('Dotation')

    df = df_transactions.merge(
        df_vehicules[cols_veh_periode],
        left_on='Card num.', right_on='N° Carte', how='left'
    )
    
    mask_date = (df['Date'].dt.date >= date_debut) & (df['Date'].dt.date <= date_fin)
    df = df[mask_date].copy()
    
    if selected_categories:
        df = df[df['Catégorie'].isin(selected_categories)]
    if selected_vehicles:
        df = df[df['Nouveau Immat'].isin(selected_vehicles)]
    
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    df['distance_parcourue'] = df['Current mileage'] - df['Past mileage']
    df['consommation_100km'] = np.where(
        (df['distance_parcourue'] > 0) & df['Quantity'].notna(),
        (df['Quantity'] / df['distance_parcourue']) * 100,
        np.nan
    )
    
    seuils_conso = st.session_state.get('ss_conso_seuils_par_categorie', {})
    df['periode_datetime'] = df['Date'].dt.to_period(periode).dt.to_timestamp()
    
    if periode == 'D':
        df['periode_str'] = df['Date'].dt.strftime('%Y-%m-%d')
    elif periode == 'W':
        df['periode_str'] = df['Date'].dt.to_period('W').astype(str)
    elif periode == 'M':
        df['periode_str'] = df['Date'].dt.strftime('%Y-%m')
    elif periode == 'Q':
        df['periode_str'] = df['Date'].dt.to_period('Q').astype(str)
    else: 
        df['periode_str'] = df['Date'].dt.strftime('%Y')
    
    conso_veh_periode = df.groupby(['Nouveau Immat', 'Catégorie', 'periode_str']).agg(
        volume_total=('Quantity', 'sum'),
        cout_total=('Amount', 'sum'),
        distance_totale=('distance_parcourue', lambda x: x[x > 0].sum()),  
        nb_transactions=('Quantity', 'count'),
        date_debut_periode=('Date', 'min'),
        date_fin_periode=('Date', 'max')
    ).reset_index()
    
    conso_veh_periode['consommation_moyenne'] = np.where(
        conso_veh_periode['distance_totale'] > 0,
        (conso_veh_periode['volume_total'] / conso_veh_periode['distance_totale']) * 100,
        np.nan
    )
    conso_veh_periode['seuil_consommation'] = conso_veh_periode['Catégorie'].map(
        lambda x: seuils_conso.get(x, DEFAULT_CONSO_SEUIL)
    )
    conso_veh_periode['exces_consommation'] = np.where(
        conso_veh_periode['consommation_moyenne'] > conso_veh_periode['seuil_consommation'],
        conso_veh_periode['consommation_moyenne'] - conso_veh_periode['seuil_consommation'],
        0
    )
    conso_veh_periode['pourcentage_exces'] = np.where(
        conso_veh_periode['seuil_consommation'] > 0,
        (conso_veh_periode['exces_consommation'] / conso_veh_periode['seuil_consommation']) * 100,
        0
    )
    
    conso_periode = df.groupby(['periode_str']).agg(
        volume_total=('Quantity', 'sum'),
        cout_total=('Amount', 'sum'),
        distance_totale=('distance_parcourue', lambda x: x[x > 0].sum()),
        nb_transactions=('Quantity', 'count'),
        nb_vehicules=('Nouveau Immat', 'nunique'),
        date_debut_periode=('Date', 'min'),
        date_fin_periode=('Date', 'max')
    ).reset_index()
    
    conso_periode['consommation_moyenne'] = np.where(
        conso_periode['distance_totale'] > 0,
        (conso_periode['volume_total'] / conso_periode['distance_totale']) * 100,
        np.nan
    )
    conso_veh_periode = conso_veh_periode.round({
        'volume_total': 1,'cout_total': 0,'distance_totale': 0,
        'consommation_moyenne': 1,'exces_consommation': 1,'pourcentage_exces': 1
    })
    conso_periode = conso_periode.round({
        'volume_total': 1,'cout_total': 0,'distance_totale': 0,'consommation_moyenne': 1
    })
    
    conso_veh_periode = conso_veh_periode.sort_values(['periode_str', 'exces_consommation'], ascending=[True, False])
    conso_periode = conso_periode.sort_values('periode_str')
    return conso_periode, conso_veh_periode

# ---------------------------------------------------------------------
# NOUVELLE FONCTION : Amélioration du dashboard
# ---------------------------------------------------------------------
def ameliorer_dashboard(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, global_date_debut: datetime.date, global_date_fin: datetime.date, kpi_cat_dash: pd.DataFrame, df_vehicle_kpi_dash: pd.DataFrame):
    """Ajoute une section d'aperçu des excès de consommation au tableau de bord"""
    with st.expander("⚠️ Aperçu des Excès de Consommation (Mensuel)", expanded=True):
        _, conso_veh_mois = analyser_consommation_par_periode(
            df_transactions, df_vehicules, global_date_debut, global_date_fin, 
            periode='M', selected_categories=None, selected_vehicles=None
        )
        if not conso_veh_mois.empty:
            exces_mois = conso_veh_mois[conso_veh_mois['exces_consommation'] > 0]
            if not exces_mois.empty:
                nb_exces_mois = len(exces_mois)
                nb_vehicules_exces = exces_mois['Nouveau Immat'].nunique()
                
                col_e1, col_e2, col_e3 = st.columns(3)
                col_e1.metric("Nombre d'Excès Détectés", f"{nb_exces_mois}")
                col_e2.metric("Véhicules Concernés", f"{nb_vehicules_exces}")
                col_e3.metric("Excès Moyen", f"{exces_mois['pourcentage_exces'].mean():.1f}%")
                
                top_exces = exces_mois.nlargest(5, 'pourcentage_exces')
                top_exces_display = top_exces[[
                    'periode_str', 'Nouveau Immat', 'consommation_moyenne',
                    'seuil_consommation', 'pourcentage_exces'
                ]]
                top_exces_display.columns = [
                    'Période', 'Immatriculation', 'Consommation (L/100km)', 
                    'Seuil (L/100km)', 'Excès (%)'
                ]
                st.dataframe(top_exces_display, use_container_width=True)
                st.markdown("""
                👉 *Pour une analyse complète des excès de consommation, utilisez la page "Analyse par Période"*
                """)
            else:
                st.success("✅ Aucun excès de consommation détecté pour les périodes analysées.")
        else:
            st.info("Données insuffisantes pour l'analyse des excès de consommation.")

# ---------------------------------------------------------------------
# NOUVELLE FONCTION : Affichage de la page d'analyse par période
# ---------------------------------------------------------------------
def afficher_page_analyse_periodes(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page d'analyse de consommation par période."""
    st.header(f"📅 Analyse de Consommation par Période ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")
    
    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return
    
    st.subheader("Configuration de l'Analyse")
    col_config1, col_config2 = st.columns(2)
    with col_config1:
        periode_options = {'Jour': 'D','Semaine': 'W','Mois': 'M','Trimestre': 'Q','Année': 'Y'}
        periode_label = st.selectbox(
            "Sélectionner la période d'analyse :",
            options=list(periode_options.keys()),index=2
        )
        periode_code = periode_options[periode_label]
    with col_config2:
        all_cats = sorted(df_vehicules['Catégorie'].dropna().astype(str).unique())
        selected_cats = st.multiselect(
            "Filtrer par Catégories de véhicules",
            options=all_cats,default=all_cats,key="periode_cat_filter"
        )
    with st.expander("Filtrer par véhicules spécifiques (optionnel)"):
        if selected_cats:
            available_vehicles = sorted(df_vehicules[df_vehicules['Catégorie'].isin(selected_cats)]['Nouveau Immat'].dropna().unique())
        else:
            available_vehicles = sorted(df_vehicules['Nouveau Immat'].dropna().unique())
        selected_vehicles = st.multiselect(
            "Sélectionner des véhicules spécifiques",
            options=available_vehicles,default=None,key="periode_veh_filter"
        )
    
    with st.spinner(f"Analyse {periode_label.lower()} en cours..."):
        conso_periode, conso_veh_periode = analyser_consommation_par_periode(
            df_transactions, df_vehicules, date_debut, date_fin, 
            periode=periode_code, selected_categories=selected_cats, 
            selected_vehicles=selected_vehicles if selected_vehicles else None
        )
    
    if conso_periode.empty or conso_veh_periode.empty:
        st.warning(f"Données insuffisantes pour l'analyse {periode_label.lower()}.")
        return
    
    st.subheader(f"Consommation {periode_label} Globale")
    afficher_dataframe_avec_export(
        conso_periode[['periode_str', 'volume_total', 'cout_total', 'distance_totale', 
                      'consommation_moyenne', 'nb_transactions', 'nb_vehicules']],
        f"Récapitulatif {periode_label}",key=f"recap_periode_{periode_code}"
    )
    fig_conso = px.line(
        conso_periode, x='periode_str', y='consommation_moyenne',
        title=f"Évolution de la Consommation Moyenne ({periode_label})",
        labels={'periode_str': periode_label, 'consommation_moyenne': 'Conso. Moyenne (L/100km)'},
        markers=True
    )
    conso_moy_globale = conso_periode['consommation_moyenne'].mean()
    fig_conso.add_hline(
        y=conso_moy_globale,line_dash="dash", line_color="green",
        annotation_text=f"Moyenne: {conso_moy_globale:.1f} L/100km"
    )
    st.plotly_chart(fig_conso, use_container_width=True)
    
    fig_vol_cout = px.bar(
        conso_periode, x='periode_str', y=['volume_total', 'cout_total'],
        title=f"Volume et Coût par {periode_label}",
        labels={'periode_str': periode_label, 'value': 'Valeur', 'variable': 'Métrique'},
        barmode='group'
    )
    st.plotly_chart(fig_vol_cout, use_container_width=True)
    
    st.subheader(f"Détail par Véhicule et par {periode_label}")
    exces_veh = conso_veh_periode[conso_veh_periode['exces_consommation'] > 0]
    nb_exces = len(exces_veh)
    if nb_exces > 0:
        st.warning(f"⚠️ Détecté : {nb_exces} cas d'excès de consommation sur la période.")
        cols_display_exces = [
            'periode_str', 'Nouveau Immat', 'Catégorie', 'consommation_moyenne',
            'seuil_consommation', 'exces_consommation', 'pourcentage_exces',
            'volume_total', 'distance_totale', 'nb_transactions'
        ]
        afficher_dataframe_avec_export(
            exces_veh[cols_display_exces],"Excès de Consommation Détectés",key=f"exces_conso_{periode_code}"
        )
        top_exces = exces_veh.nlargest(10, 'pourcentage_exces')
        fig_top_exces = px.bar(
            top_exces,x='Nouveau Immat',y='pourcentage_exces',color='Catégorie',
            title="Top 10 des Excès de Consommation (%)",
            labels={'pourcentage_exces': "Excès (%)", 'Nouveau Immat': 'Véhicule'},
            hover_data=['periode_str', 'consommation_moyenne', 'seuil_consommation']
        )
        st.plotly_chart(fig_top_exces, use_container_width=True)
    else:
        st.success("✅ Aucun excès de consommation détecté sur la période analysée.")
    
    with st.expander("Voir toutes les données détaillées par véhicule et période"):
        cols_display_detail = [
            'periode_str', 'Nouveau Immat', 'Catégorie', 'volume_total',
            'distance_totale', 'consommation_moyenne', 'seuil_consommation',
            'exces_consommation', 'pourcentage_exces', 'cout_total', 'nb_transactions'
        ]
        afficher_dataframe_avec_export(
            conso_veh_periode[cols_display_detail],
            f"Toutes les données par Véhicule et {periode_label}",key=f"all_data_periode_{periode_code}"
        )
    with st.expander("Analyse comparative entre périodes", expanded=False):
        st.info("Cette section permet de visualiser l'évolution de la consommation par véhicule à travers les périodes.")
        vehicules_list = sorted(conso_veh_periode['Nouveau Immat'].unique())
        if vehicules_list:
            vehicule_selected = st.selectbox(
                "Sélectionner un véhicule pour l'analyse détaillée :",
                options=vehicules_list,key="compare_vehicule_select"
            )
            veh_data = conso_veh_periode[conso_veh_periode['Nouveau Immat'] == vehicule_selected]
            if not veh_data.empty:
                fig_veh_evo = px.line(
                    veh_data, x='periode_str', y=['consommation_moyenne', 'seuil_consommation'],
                    title=f"Évolution de la Consommation - {vehicule_selected}",
                    labels={'periode_str': periode_label, 'value': 'Consommation (L/100km)', 'variable': 'Métrique'},
                    markers=True
                )
                st.plotly_chart(fig_veh_evo, use_container_width=True)
                st.dataframe(veh_data[[
                    'periode_str', 'consommation_moyenne', 'seuil_consommation',
                    'exces_consommation', 'volume_total', 'distance_totale'
                ]], use_container_width=True)
            else:
                st.info(f"Pas de données disponibles pour {vehicule_selected} sur les périodes sélectionnées.")
        else:
            st.info("Aucun véhicule avec données suffisantes pour l'analyse comparative.")

# ---------------------------------------------------------------------
# FONCTIONS AMÉLIORÉES POUR LE SUIVI DES DOTATIONS
# ---------------------------------------------------------------------
def analyser_suivi_dotations(
    df_transactions_filtrees: pd.DataFrame,
    df_vehicules: pd.DataFrame, # Doit contenir 'N° Carte', 'Nouveau Immat', 'Catégorie', 'Dotation'
    date_debut_periode: datetime.date,
    date_fin_periode: datetime.date
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analyse l'utilisation des dotations de carburant.

    Args:
        df_transactions_filtrees: Transactions déjà filtrées pour la période globale.
        df_vehicules: DataFrame des véhicules avec leurs informations, incluant 'Dotation'.
        date_debut_periode: Date de début de la période d'analyse.
        date_fin_periode: Date de fin de la période d'analyse.

    Returns:
        Un tuple de DataFrames:
        - df_recap_dotation_periode: Récapitulatif par véhicule sur toute la période.
        - df_detail_dotation_mensuel: Détail mensuel par véhicule.
    """
    if 'Dotation' not in df_vehicules.columns:
        st.error("La colonne 'Dotation' est indispensable pour cette analyse et n'a pas été trouvée dans les données véhicules.")
        return pd.DataFrame(), pd.DataFrame()

    # Merge transactions avec infos véhicules (y compris Dotation)
    df_merged = df_transactions_filtrees.merge(
        df_vehicules[['N° Carte', 'Nouveau Immat', 'Catégorie', 'Dotation']],
        left_on='Card num.',
        right_on='N° Carte',
        how='inner' # Garder seulement les transactions des cartes véhicules connues avec dotation
    )

    if df_merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    # --- Calcul du nombre de mois dans la période ---
    # Utilisation de dateutil.relativedelta pour plus de précision
    delta = relativedelta(date_fin_periode, date_debut_periode)
    nombre_mois_periode = delta.years * 12 + delta.months + 1

    # --- Analyse mensuelle détaillée ---
    df_merged['AnneeMois'] = df_merged['Date'].dt.strftime('%Y-%m')
    
    # Consommation réelle par mois et par véhicule
    conso_mensuelle_veh = df_merged.groupby(['Nouveau Immat', 'Catégorie', 'Dotation', 'AnneeMois']).agg(
        Consommation_Mois_L=('Quantity', 'sum')
    ).reset_index()
    
    # La dotation est déjà mensuelle, donc 'Dotation' est la dotation pour ce mois.
    conso_mensuelle_veh.rename(columns={'Dotation': 'Dotation_Mensuelle_L'}, inplace=True)
    
    conso_mensuelle_veh['Difference_Mois_L'] = conso_mensuelle_veh['Dotation_Mensuelle_L'] - conso_mensuelle_veh['Consommation_Mois_L']
    conso_mensuelle_veh['Taux_Utilisation_Mois_%'] = np.where(
        conso_mensuelle_veh['Dotation_Mensuelle_L'] > 0,
        (conso_mensuelle_veh['Consommation_Mois_L'] / conso_mensuelle_veh['Dotation_Mensuelle_L']) * 100,
        np.nan # ou 0 si consommation = 0, ou un grand nombre si consommation > 0 et dotation = 0
    )
    
    # Ajout de flags pour sous-utilisation et sur-utilisation
    seuil_sous_utilisation = st.session_state.get('ss_seuil_sous_utilisation', DEFAULT_SEUIL_SOUS_UTILISATION)
    seuil_sur_utilisation = st.session_state.get('ss_seuil_sur_utilisation', DEFAULT_SEUIL_SUR_UTILISATION)
    
    conso_mensuelle_veh['Statut_Utilisation'] = np.select(
        [
            conso_mensuelle_veh['Taux_Utilisation_Mois_%'] < seuil_sous_utilisation,
            conso_mensuelle_veh['Taux_Utilisation_Mois_%'] > seuil_sur_utilisation,
        ],
        [
            'Sous-utilisation',
            'Sur-utilisation',
        ],
        default='Normal'
    )
    
    conso_mensuelle_veh['Potentiel_Economie_L'] = np.where(
        conso_mensuelle_veh['Statut_Utilisation'] == 'Sous-utilisation',
        (conso_mensuelle_veh['Dotation_Mensuelle_L'] * (1 - (conso_mensuelle_veh['Taux_Utilisation_Mois_%'] / 100))).round(1),
        0
    )
    
    conso_mensuelle_veh['Taux_Utilisation_Mois_%'] = conso_mensuelle_veh['Taux_Utilisation_Mois_%'].round(1)
    
    # --- Récapitulatif par véhicule sur toute la période ---
    # Consommation totale sur la période par véhicule
    conso_totale_periode_veh = df_merged.groupby(['Nouveau Immat', 'Catégorie', 'Dotation']).agg(
        Consommation_Reelle_Periode_L=('Quantity', 'sum')
    ).reset_index()
    conso_totale_periode_veh.rename(columns={'Dotation': 'Dotation_Mensuelle_L'}, inplace=True)

    # Dotation totale allouée pour la période
    conso_totale_periode_veh['Nb_Mois_Periode'] = nombre_mois_periode
    conso_totale_periode_veh['Dotation_Allouee_Periode_L'] = conso_totale_periode_veh['Dotation_Mensuelle_L'] * nombre_mois_periode
    
    conso_totale_periode_veh['Difference_Periode_L'] = conso_totale_periode_veh['Dotation_Allouee_Periode_L'] - conso_totale_periode_veh['Consommation_Reelle_Periode_L']
    conso_totale_periode_veh['Taux_Utilisation_Periode_%'] = np.where(
        conso_totale_periode_veh['Dotation_Allouee_Periode_L'] > 0,
        (conso_totale_periode_veh['Consommation_Reelle_Periode_L'] / conso_totale_periode_veh['Dotation_Allouee_Periode_L']) * 100,
        np.nan
    )
    
    # Ajout d'analyses supplémentaires pour l'optimisation des dotations
    conso_totale_periode_veh['Statut_Utilisation'] = np.select(
        [
            conso_totale_periode_veh['Taux_Utilisation_Periode_%'] < seuil_sous_utilisation,
            conso_totale_periode_veh['Taux_Utilisation_Periode_%'] > seuil_sur_utilisation,
        ],
        [
            'Sous-utilisation',
            'Sur-utilisation',
        ],
        default='Normal'
    )
    
    # Calcul des économies potentielles en litres et pourcentage de réduction
    conso_totale_periode_veh['Pourcentage_Inutilise'] = np.where(
        conso_totale_periode_veh['Taux_Utilisation_Periode_%'] < 100,
        100 - conso_totale_periode_veh['Taux_Utilisation_Periode_%'],
        0
    )
    
    # Nouvelles colonnes pour l'optimisation
    conso_totale_periode_veh['Economie_Potentielle_L'] = np.where(
        conso_totale_periode_veh['Statut_Utilisation'] == 'Sous-utilisation',
        (conso_totale_periode_veh['Dotation_Mensuelle_L'] * 
         (1 - (conso_totale_periode_veh['Taux_Utilisation_Periode_%'] / 100))).round(1),
        0
    )
    
    conso_totale_periode_veh['Dotation_Optimisee_L'] = np.where(
        conso_totale_periode_veh['Statut_Utilisation'] == 'Sous-utilisation',
        (conso_totale_periode_veh['Consommation_Reelle_Periode_L'] / nombre_mois_periode * 1.1).round(0),  # 10% de marge
        conso_totale_periode_veh['Dotation_Mensuelle_L']
    )
    
    conso_totale_periode_veh['Variation_Dotation_%'] = np.where(
        conso_totale_periode_veh['Dotation_Mensuelle_L'] > 0,
        ((conso_totale_periode_veh['Dotation_Optimisee_L'] / conso_totale_periode_veh['Dotation_Mensuelle_L']) * 100 - 100).round(1),
        0
    )
    
    # Arrondi des valeurs finales
    conso_totale_periode_veh['Taux_Utilisation_Periode_%'] = conso_totale_periode_veh['Taux_Utilisation_Periode_%'].round(1)
    conso_totale_periode_veh['Pourcentage_Inutilise'] = conso_totale_periode_veh['Pourcentage_Inutilise'].round(1)
    
    # Joindre le nombre de mois où il y a sous-utilisation pour chaque véhicule
    mois_sous_util = conso_mensuelle_veh[conso_mensuelle_veh['Statut_Utilisation'] == 'Sous-utilisation'].groupby('Nouveau Immat').size().reset_index(name='Nb_Mois_Sous_Utilisation')
    conso_totale_periode_veh = conso_totale_periode_veh.merge(mois_sous_util, on='Nouveau Immat', how='left')
    conso_totale_periode_veh['Nb_Mois_Sous_Utilisation'] = conso_totale_periode_veh['Nb_Mois_Sous_Utilisation'].fillna(0).astype(int)
    
    # Calcul du ratio de mois en sous-utilisation
    conso_totale_periode_veh['Ratio_Mois_Sous_Util_%'] = (conso_totale_periode_veh['Nb_Mois_Sous_Utilisation'] / nombre_mois_periode * 100).round(1)
    
    # Recommandation basée sur l'analyse
    conditions = [
        (conso_totale_periode_veh['Statut_Utilisation'] == 'Sous-utilisation') & 
        (conso_totale_periode_veh['Ratio_Mois_Sous_Util_%'] >= 70) &
        (conso_totale_periode_veh['Pourcentage_Inutilise'] >= 30),
        
        (conso_totale_periode_veh['Statut_Utilisation'] == 'Sous-utilisation') & 
        (conso_totale_periode_veh['Ratio_Mois_Sous_Util_%'] >= 50),
        
        (conso_totale_periode_veh['Statut_Utilisation'] == 'Sur-utilisation') &
        (conso_totale_periode_veh['Taux_Utilisation_Periode_%'] > 120)
    ]
    
    choix = [
        'Forte réduction recommandée',
        'Réduction possible',
        'Augmentation recommandée'
    ]
    
    conso_totale_periode_veh['Recommandation'] = np.select(conditions, choix, default='Maintenir')

    # Sélection et ordre des colonnes
    cols_recap = ['Nouveau Immat', 'Catégorie', 'Dotation_Mensuelle_L', 'Nb_Mois_Periode', 
                  'Dotation_Allouee_Periode_L', 'Consommation_Reelle_Periode_L', 
                  'Difference_Periode_L', 'Taux_Utilisation_Periode_%', 'Statut_Utilisation',
                  'Nb_Mois_Sous_Utilisation', 'Ratio_Mois_Sous_Util_%',
                  'Economie_Potentielle_L', 'Dotation_Optimisee_L', 
                  'Variation_Dotation_%', 'Recommandation']
    
    df_recap_dotation_periode = conso_totale_periode_veh[cols_recap]

    cols_detail = ['Nouveau Immat', 'Catégorie', 'AnneeMois', 'Dotation_Mensuelle_L', 
                   'Consommation_Mois_L', 'Difference_Mois_L', 'Taux_Utilisation_Mois_%',
                   'Statut_Utilisation', 'Potentiel_Economie_L']
    
    df_detail_dotation_mensuel = conso_mensuelle_veh[cols_detail].sort_values(['Nouveau Immat', 'AnneeMois'])
    
    return df_recap_dotation_periode, df_detail_dotation_mensuel


def afficher_page_suivi_dotations(
    df_transactions: pd.DataFrame, 
    df_vehicules: pd.DataFrame, 
    date_debut: datetime.date, 
    date_fin: datetime.date
):
    """Affiche la page de suivi des dotations améliorée avec focus sur l'optimisation."""
    st.header(f"⛽ Suivi et Optimisation des Dotations ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if 'Dotation' not in df_vehicules.columns or df_vehicules['Dotation'].sum() == 0:
        st.warning("Aucune donnée de dotation n'est disponible ou les dotations sont toutes à zéro. Le suivi des dotations ne peut pas être effectué.")
        st.info("Veuillez vérifier que la colonne 'Dotation' (représentant la dotation mensuelle en litres) est présente et correctement renseignée dans votre fichier 'CARTES VEHICULE'.")
        return

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    # --- Configuration des paramètres de l'analyse ---
    with st.sidebar.expander("⚙️ Paramètres d'analyse des dotations", expanded=False):
        st.session_state.ss_seuil_sous_utilisation = st.slider(
            "Seuil de sous-utilisation (%)", 
            min_value=30, max_value=90, 
            value=st.session_state.get('ss_seuil_sous_utilisation', DEFAULT_SEUIL_SOUS_UTILISATION),
            step=5, help="Pourcentage en dessous duquel on considère qu'un véhicule sous-utilise sa dotation."
        )
        
        st.session_state.ss_seuil_sur_utilisation = st.slider(
            "Seuil de sur-utilisation (%)", 
            min_value=100, max_value=150, 
            value=st.session_state.get('ss_seuil_sur_utilisation', DEFAULT_SEUIL_SUR_UTILISATION),
            step=5, help="Pourcentage au-dessus duquel on considère qu'un véhicule sur-utilise sa dotation."
        )
        
        st.session_state.ss_mois_min_analyse = st.slider(
            "Nombre minimum de mois pour l'analyse", 
            min_value=1, max_value=6, 
            value=st.session_state.get('ss_mois_min_analyse', DEFAULT_MOIS_MIN_ANALYSE),
            step=1, help="Nombre minimum de mois requis pour une analyse fiable."
        )

    # --- Filtres ---
    st.sidebar.subheader("Filtres pour Suivi Dotations")
    all_cats_dot = sorted(df_vehicules['Catégorie'].dropna().astype(str).unique())
    selected_cats_dot = st.sidebar.multiselect(
        "Filtrer par Catégories", options=all_cats_dot, default=all_cats_dot, key="dot_cat_filter"
    )

    vehicules_filtrables = df_vehicules[df_vehicules['Catégorie'].isin(selected_cats_dot)]['Nouveau Immat'].dropna().unique()
    
    filtre_statut_options = ["Tous", "Sous-utilisation", "Normal", "Sur-utilisation"]
    filtre_statut = st.sidebar.selectbox(
        "Filtrer par statut d'utilisation", 
        options=filtre_statut_options, 
        index=0,
        key="dot_statut_filter"
    )
    
    tri_options = {
        "Taux d'utilisation (croissant)": ("Taux_Utilisation_Periode_%", True),
        "Taux d'utilisation (décroissant)": ("Taux_Utilisation_Periode_%", False),
        "Économie potentielle (décroissant)": ("Economie_Potentielle_L", False),
        "Variation dotation (croissant)": ("Variation_Dotation_%", True),
        "Alphabétique (immatriculation)": ("Nouveau Immat", True)
    }
    
    option_tri = st.sidebar.selectbox(
        "Trier les véhicules par", 
        options=list(tri_options.keys()),
        index=0,
        key="dot_sort_option"
    )
    
    selected_vehicle_dot = st.sidebar.selectbox(
        "Choisir un véhicule spécifique (Optionnel)", 
        options=["Tous les véhicules"] + sorted(list(vehicules_filtrables)),
        key="dot_veh_filter"
    )

    # --- Analyse ---
    with st.spinner("Analyse du suivi des dotations en cours..."):
        df_recap, df_detail_mensuel = analyser_suivi_dotations(
            df_transactions, df_vehicules, date_debut, date_fin
        )

    if df_recap.empty:
        st.info("Aucune donnée à afficher pour le suivi des dotations avec les filtres actuels.")
        return
        
    # Appliquer les filtres post-analyse
    df_recap_filtered = df_recap[df_recap['Catégorie'].isin(selected_cats_dot)]
    
    if filtre_statut != "Tous":
        df_recap_filtered = df_recap_filtered[df_recap_filtered['Statut_Utilisation'] == filtre_statut]
    
    # Appliquer le tri sélectionné
    tri_col, tri_asc = tri_options[option_tri]
    df_recap_filtered = df_recap_filtered.sort_values(by=tri_col, ascending=tri_asc)
    
    if selected_vehicle_dot != "Tous les véhicules":
        df_recap_filtered = df_recap_filtered[df_recap_filtered['Nouveau Immat'] == selected_vehicle_dot]
        df_detail_mensuel_filtered = df_detail_mensuel[
            (df_detail_mensuel['Nouveau Immat'] == selected_vehicle_dot) &
            (df_detail_mensuel['Catégorie'].isin(selected_cats_dot))
        ]
    else:
        df_detail_mensuel_filtered = df_detail_mensuel[df_detail_mensuel['Catégorie'].isin(selected_cats_dot)]
        if filtre_statut != "Tous":
            vehicules_statut = df_recap_filtered['Nouveau Immat'].unique()
            df_detail_mensuel_filtered = df_detail_mensuel_filtered[
                df_detail_mensuel_filtered['Nouveau Immat'].isin(vehicules_statut)
            ]

    # --- Indicateurs Clés ---
    st.subheader("🔍 Aperçu Général des Dotations")
    
    # Calcul des principaux indicateurs
    total_vehicules = len(df_recap_filtered)
    total_dotation_mensuelle = df_recap_filtered['Dotation_Mensuelle_L'].sum()
    total_consommation = df_recap_filtered['Consommation_Reelle_Periode_L'].sum()
    total_dotation_periode = df_recap_filtered['Dotation_Allouee_Periode_L'].sum()
    taux_utilisation_global = (total_consommation / total_dotation_periode * 100) if total_dotation_periode > 0 else 0
    
    # Nombre de véhicules par statut
    nb_sous_util = len(df_recap_filtered[df_recap_filtered['Statut_Utilisation'] == 'Sous-utilisation'])
    nb_normal = len(df_recap_filtered[df_recap_filtered['Statut_Utilisation'] == 'Normal'])
    nb_sur_util = len(df_recap_filtered[df_recap_filtered['Statut_Utilisation'] == 'Sur-utilisation'])
    
    # Calcul des économies potentielles
    economie_potentielle_totale = df_recap_filtered['Economie_Potentielle_L'].sum()
    nb_vehicules_a_optimiser = len(df_recap_filtered[df_recap_filtered['Recommandation'].isin(['Forte réduction recommandée', 'Réduction possible'])])
    
    # Affichage des métriques en colonnes
    col1, col2, col3 = st.columns(3)
    col1.metric("Nombre de Véhicules", f"{total_vehicules}")
    col2.metric("Dotation Mensuelle Totale", f"{total_dotation_mensuelle:.0f} L")
    col3.metric("Taux d'Utilisation Global", f"{taux_utilisation_global:.1f}%")
    
    col4, col5, col6 = st.columns(3)
    col4.metric("Véhicules en Sous-utilisation", f"{nb_sous_util}", delta=f"{nb_sous_util/total_vehicules*100:.1f}%" if total_vehicules > 0 else "0%")
    col5.metric("Véhicules avec Utilisation Normale", f"{nb_normal}", delta=f"{nb_normal/total_vehicules*100:.1f}%" if total_vehicules > 0 else "0%")
    col6.metric("Véhicules en Sur-utilisation", f"{nb_sur_util}", delta=f"{nb_sur_util/total_vehicules*100:.1f}%" if total_vehicules > 0 else "0%")
    
    col7, col8 = st.columns(2)
    col7.metric("Économie Mensuelle Potentielle", f"{economie_potentielle_totale:.0f} L", 
               delta=f"{economie_potentielle_totale/total_dotation_mensuelle*100:.1f}%" if total_dotation_mensuelle > 0 else "0%")
    col8.metric("Véhicules à Optimiser", f"{nb_vehicules_a_optimiser}", 
               delta=f"{nb_vehicules_a_optimiser/total_vehicules*100:.1f}%" if total_vehicules > 0 else "0%")
    
    # --- Graphiques d'analyse des dotations ---
    st.subheader("📊 Analyse des Dotations")
    
    col_graph1, col_graph2 = st.columns(2)
    with col_graph1:
        # Distribution du taux d'utilisation
        fig_distribution = px.histogram(
            df_recap_filtered, x='Taux_Utilisation_Periode_%',
            title="Distribution du Taux d'Utilisation des Dotations",
            labels={'Taux_Utilisation_Periode_%': "Taux d'Utilisation (%)"},
            histnorm='percent',
            nbins=20,
        )
        fig_distribution.add_vline(x=st.session_state.ss_seuil_sous_utilisation, line_dash="dash", line_color="red", 
                                  annotation_text=f"Seuil sous-utilisation ({st.session_state.ss_seuil_sous_utilisation}%)")
        fig_distribution.add_vline(x=100, line_dash="solid", line_color="green", 
                                  annotation_text="Idéal (100%)")
        fig_distribution.add_vline(x=st.session_state.ss_seuil_sur_utilisation, line_dash="dash", line_color="orange", 
                                  annotation_text=f"Seuil sur-utilisation ({st.session_state.ss_seuil_sur_utilisation}%)")
        st.plotly_chart(fig_distribution, use_container_width=True)
    
    with col_graph2:
        # Répartition des statuts d'utilisation
        statut_counts = df_recap_filtered['Statut_Utilisation'].value_counts().reset_index()
        statut_counts.columns = ['Statut', 'Nombre']
        
        fig_statut = px.pie(
            statut_counts, values='Nombre', names='Statut',
            title="Répartition des Statuts d'Utilisation",
            color='Statut',
            color_discrete_map={
                'Sous-utilisation': 'lightblue',
                'Normal': 'green',
                'Sur-utilisation': 'coral'
            }
        )
        st.plotly_chart(fig_statut, use_container_width=True)
    
    # Top véhicules sous-utilisés
    top_sous_util = df_recap_filtered[df_recap_filtered['Statut_Utilisation'] == 'Sous-utilisation'].nlargest(10, 'Economie_Potentielle_L')
    if not top_sous_util.empty:
        fig_top_sous = px.bar(
            top_sous_util,
            x='Nouveau Immat',
            y='Economie_Potentielle_L',
            title="Top 10 Véhicules - Économie Potentielle Mensuelle (L)",
            color='Taux_Utilisation_Periode_%',
            color_continuous_scale=px.colors.sequential.Blues_r,  # Échelle inversée pour que le bleu foncé = faible utilisation
            labels={'Economie_Potentielle_L': 'Économie Mensuelle (L)', 'Nouveau Immat': 'Immatriculation'},
            hover_data=['Catégorie', 'Dotation_Mensuelle_L', 'Taux_Utilisation_Periode_%', 'Recommandation']
        )
        st.plotly_chart(fig_top_sous, use_container_width=True)

    # --- Affichage Récapitulatif par Véhicule sur la Période ---
    st.subheader("📋 Récapitulatif de l'Utilisation des Dotations")
    
    # Ajouter des filtres visuels directement dans le tableau
    df_styled = df_recap_filtered.copy()
    
    # Renommer les colonnes pour l'affichage
    columns_display = {
        'Nouveau Immat': 'Immatriculation', 
        'Catégorie': 'Catégorie',
        'Dotation_Mensuelle_L': 'Dotation Mensuelle (L)', 
        'Nb_Mois_Periode': 'Nb Mois',
        'Consommation_Reelle_Periode_L': 'Conso. Totale (L)',
        'Taux_Utilisation_Periode_%': "Taux d'Utilisation (%)", 
        'Statut_Utilisation': 'Statut',
        'Economie_Potentielle_L': 'Économie Possible (L/mois)',
        'Dotation_Optimisee_L': 'Dotation Optimisée (L)',
        'Recommandation': 'Recommandation',
        'Variation_Dotation_%': 'Variation Dotation (%)'
    }
    
    cols_to_display = list(columns_display.keys())
    df_display = df_styled[cols_to_display].rename(columns=columns_display)
    
    afficher_dataframe_avec_export(df_display, "Récapitulatif Dotations par Véhicule", key="recap_dot_veh")
    
    # --- Affichage d'un Plan d'Optimisation ---
    vehicules_a_optimiser = df_recap_filtered[df_recap_filtered['Recommandation'].isin(['Forte réduction recommandée', 'Réduction possible'])]
    
    if not vehicules_a_optimiser.empty:
        st.subheader("💡 Plan d'Optimisation des Dotations")
        
        optimisation_text = f"""
        ### Potentiel d'économie identifié
        
        D'après l'analyse des données de consommation sur la période sélectionnée:
        - **{len(vehicules_a_optimiser)}** véhicules pourraient avoir leur dotation optimisée
        - Économie mensuelle potentielle: **{vehicules_a_optimiser['Economie_Potentielle_L'].sum():.0f} litres**
        - Économie annuelle projetée: **{vehicules_a_optimiser['Economie_Potentielle_L'].sum() * 12:.0f} litres**
        
        Le tableau ci-dessous présente un plan d'optimisation concret avec les nouvelles dotations suggérées.
        """
        st.markdown(optimisation_text)
        
        # Création d'un DataFrame de plan d'optimisation
        plan_optimisation = vehicules_a_optimiser[[
            'Nouveau Immat', 'Catégorie', 'Dotation_Mensuelle_L', 
            'Dotation_Optimisee_L', 'Variation_Dotation_%', 
            'Taux_Utilisation_Periode_%', 'Economie_Potentielle_L', 'Recommandation'
        ]].copy()
        
        plan_optimisation.columns = [
            'Immatriculation', 'Catégorie', 'Dotation Actuelle (L)',
            'Dotation Recommandée (L)', 'Variation (%)', 
            "Taux d'Utilisation Actuel (%)", 'Économie Mensuelle (L)', 'Recommandation'
        ]
        
        plan_optimisation = plan_optimisation.sort_values('Économie Mensuelle (L)', ascending=False)
        
        afficher_dataframe_avec_export(plan_optimisation, "Plan d'Optimisation des Dotations", key="plan_optimisation")
        
        # Visualisation du potentiel d'économie
        fig_eco = px.bar(
            plan_optimisation.head(15),  # Limiter aux 15 premiers pour lisibilité
            x='Immatriculation',
            y=['Dotation Actuelle (L)', 'Dotation Recommandée (L)'],
            barmode='group',
            title="Comparaison des Dotations Actuelles vs Recommandées (Top 15)",
            labels={'value': 'Dotation (L)', 'variable': ''},
            color_discrete_map={
                'Dotation Actuelle (L)': 'lightblue',
                'Dotation Recommandée (L)': 'darkblue'
            }
        )
        st.plotly_chart(fig_eco, use_container_width=True)
        
        # Ajouter un bouton d'export pour le plan complet
        total_eco_mensuelle = plan_optimisation['Économie Mensuelle (L)'].sum()
        total_eco_annuelle = total_eco_mensuelle * 12
        
        st.info(f"💰 Économie totale estimée: {total_eco_mensuelle:.0f} L/mois, soit {total_eco_annuelle:.0f} L/an")
    else:
        st.info("Aucun véhicule n'a été identifié comme nécessitant une optimisation de sa dotation selon les critères actuels.")

    # --- Affichage Détail Mensuel (si un véhicule est sélectionné) ---
    if selected_vehicle_dot != "Tous les véhicules":
        st.subheader(f"📅 Détail Mensuel pour le Véhicule : {selected_vehicle_dot}")
        if not df_detail_mensuel_filtered.empty:
            # Renommer les colonnes pour l'affichage
            columns_detail_display = {
                'Nouveau Immat': 'Immatriculation',
                'AnneeMois': 'Mois',
                'Dotation_Mensuelle_L': 'Dotation (L)',
                'Consommation_Mois_L': 'Consommation (L)',
                'Difference_Mois_L': 'Écart (L)',
                'Taux_Utilisation_Mois_%': "Taux d'Utilisation (%)",
                'Statut_Utilisation': 'Statut',
                'Potentiel_Economie_L': 'Économie Possible (L)'
            }
            
            df_detail_display = df_detail_mensuel_filtered.rename(columns=columns_detail_display)
            
            afficher_dataframe_avec_export(df_detail_display, 
                                          f"Détail Mensuel Dotations - {selected_vehicle_dot}", 
                                          key="detail_dot_mensuel_veh")

            # Visualisation de l'évolution mensuelle
            fig_detail_veh = px.line(
                df_detail_mensuel_filtered,
                x='AnneeMois',
                y=['Dotation_Mensuelle_L', 'Consommation_Mois_L'],
                title=f"Évolution Mensuelle: Dotation vs Consommation - {selected_vehicle_dot}",
                labels={'value': 'Volume (L)', 'variable': 'Type', 'AnneeMois': 'Mois'},
                markers=True,
                color_discrete_map={
                    'Dotation_Mensuelle_L': 'blue',
                    'Consommation_Mois_L': 'green'
                }
            )
            
            # Ajouter une ligne pour le taux d'utilisation (axe secondaire)
            fig_taux = px.line(
                df_detail_mensuel_filtered,
                x='AnneeMois',
                y=['Taux_Utilisation_Mois_%'],
                labels={'Taux_Utilisation_Mois_%': "Taux d'Utilisation (%)"},
                markers=True
            )
            
            # Ajouter second axe Y
            fig_combined = px.line()
            fig_combined.add_trace(fig_detail_veh.data[0])  # Dotation
            fig_combined.add_trace(fig_detail_veh.data[1])  # Consommation
            
            # Ajouter le taux d'utilisation avec axe secondaire
            taux_trace = fig_taux.data[0]
            taux_trace.yaxis = "y2"
            taux_trace.name = "Taux d'Utilisation (%)"
            taux_trace.line.color = "red"
            fig_combined.add_trace(taux_trace)
            
            # Configurer les axes
            fig_combined.update_layout(
                title=f"Évolution Mensuelle: Dotation, Consommation et Taux d'Utilisation - {selected_vehicle_dot}",
                yaxis=dict(title="Volume (L)"),
                yaxis2=dict(title="Taux d'Utilisation (%)", overlaying="y", side="right"),
                legend=dict(orientation="h", y=1.1)
            )
            
            st.plotly_chart(fig_combined, use_container_width=True)
            
            # Analyse des variations saisonnières si données suffisantes
            if len(df_detail_mensuel_filtered) >= 3:
                st.subheader("🔄 Analyse des Variations Saisonnières")
                
                # Extraire le mois pour analyse saisonnière
                df_detail_mensuel_filtered['Mois'] = pd.to_datetime(df_detail_mensuel_filtered['AnneeMois'] + '-01').dt.month_name()
                
                saisonnalite = df_detail_mensuel_filtered.groupby('Mois').agg(
                    Taux_Utilisation_Moyen=('Taux_Utilisation_Mois_%', 'mean'),
                    Consommation_Moyenne=('Consommation_Mois_L', 'mean'),
                    Nb_Observations=('AnneeMois', 'count')
                ).reset_index()
                
                # Ordonner les mois chronologiquement
                mois_ordre = ['January', 'February', 'March', 'April', 'May', 'June', 
                              'July', 'August', 'September', 'October', 'November', 'December']
                saisonnalite['Mois'] = pd.Categorical(saisonnalite['Mois'], categories=mois_ordre, ordered=True)
                saisonnalite = saisonnalite.sort_values('Mois')
                
                fig_saison = px.line(
                    saisonnalite,
                    x='Mois',
                    y='Taux_Utilisation_Moyen',
                    title=f"Variations Saisonnières du Taux d'Utilisation - {selected_vehicle_dot}",
                    labels={'Taux_Utilisation_Moyen': "Taux d'Utilisation Moyen (%)", 'Mois': 'Mois'},
                    markers=True
                )
                
                # Ajouter ligne horizontale pour 100%
                fig_saison.add_hline(y=100, line_dash="dash", line_color="green", 
                                    annotation_text="Utilisation idéale (100%)")
                
                st.plotly_chart(fig_saison, use_container_width=True)
                
                # Afficher des recommandations saisonnières
                if len(saisonnalite) >= 3:  # Au moins 3 mois pour faire des recommandations
                    mois_faible_util = saisonnalite[saisonnalite['Taux_Utilisation_Moyen'] < 70]
                    mois_forte_util = saisonnalite[saisonnalite['Taux_Utilisation_Moyen'] > 110]
                    
                    if not mois_faible_util.empty or not mois_forte_util.empty:
                        st.info("💡 **Recommandation saisonnière**: Envisagez d'ajuster la dotation en fonction des mois:")
                        
                        if not mois_faible_util.empty:
                            msg_faible = ", ".join(mois_faible_util['Mois'])
                            st.markdown(f"- **Réduction possible** pour les mois de: {msg_faible}")
                        
                        if not mois_forte_util.empty:
                            msg_forte = ", ".join(mois_forte_util['Mois'])
                            st.markdown(f"- **Augmentation recommandée** pour les mois de: {msg_forte}")
            
            # Obtenir les infos vehicle depuis df_vehicules
            info_vehicle = df_vehicules[df_vehicules['Nouveau Immat'] == selected_vehicle_dot].iloc[0] if len(df_vehicules[df_vehicules['Nouveau Immat'] == selected_vehicle_dot]) > 0 else None
            
            if info_vehicle is not None:
                st.subheader("📝 Résumé et Recommandations")
                
                # Trouver les données récapitulatives pour ce véhicule
                vehicle_recap = df_recap_filtered[df_recap_filtered['Nouveau Immat'] == selected_vehicle_dot].iloc[0] if len(df_recap_filtered[df_recap_filtered['Nouveau Immat'] == selected_vehicle_dot]) > 0 else None
                
                if vehicle_recap is not None:
                    # Créer un résumé personnalisé
                    statut = vehicle_recap['Statut_Utilisation']
                    taux_util = vehicle_recap['Taux_Utilisation_Periode_%']
                    dotation_actuelle = vehicle_recap['Dotation_Mensuelle_L']
                    dotation_optimisee = vehicle_recap['Dotation_Optimisee_L']
                    
                    st.write(f"**Marque/Modèle**: {info_vehicle.get('Marque', 'N/A')} {info_vehicle.get('Modèle', 'N/A')}")
                    st.write(f"**Catégorie**: {info_vehicle.get('Catégorie', 'N/A')}")
                    st.write(f"**Statut d'utilisation**: {statut} ({taux_util:.1f}%)")
                    
                    if statut == 'Sous-utilisation':
                        eco_potentielle = vehicle_recap['Economie_Potentielle_L']
                        variation = vehicle_recap['Variation_Dotation_%']
                        
                        st.warning(f"""
                        📉 **Ce véhicule sous-utilise sa dotation actuelle.**
                        
                        - Dotation actuelle: {dotation_actuelle:.0f} L/mois
                        - Dotation optimisée recommandée: {dotation_optimisee:.0f} L/mois ({variation:.1f}%)
                        - Économie mensuelle potentielle: {eco_potentielle:.1f} L/mois
                        - Économie annuelle projetée: {eco_potentielle * 12:.0f} L/an
                        
                        Recommandation: {vehicle_recap['Recommandation']}
                        """)
                    elif statut == 'Sur-utilisation':
                        st.error(f"""
                        📈 **Ce véhicule dépasse régulièrement sa dotation actuelle.**
                        
                        - Dotation actuelle: {dotation_actuelle:.0f} L/mois
                        - Dotation suggérée: {dotation_optimisee:.0f} L/mois
                        
                        Recommandation: Envisagez d'augmenter la dotation ou d'analyser les raisons de cette sur-utilisation.
                        """)
                    else:  # Normal
                        st.success(f"""
                        ✅ **Ce véhicule utilise sa dotation de manière optimale.**
                        
                        - Dotation actuelle: {dotation_actuelle:.0f} L/mois
                        - Taux d'utilisation: {taux_util:.1f}%
                        
                        Recommandation: Maintenir la dotation actuelle.
                        """)
        else:
            st.info(f"Aucun détail mensuel à afficher pour {selected_vehicle_dot} avec les filtres actuels.")
    else:
        with st.expander("Voir le détail mensuel pour tous les véhicules"):
            afficher_dataframe_avec_export(df_detail_mensuel_filtered, 
                                          "Détail Mensuel Dotations - Tous Véhicules Filtrés", 
                                          key="detail_dot_mensuel_all_veh")
            
            # Évolution du taux d'utilisation global par mois
            agg_mois = df_detail_mensuel_filtered.groupby('AnneeMois').agg(
                Dotation_Totale=('Dotation_Mensuelle_L', 'sum'),
                Consommation_Totale=('Consommation_Mois_L', 'sum')
            ).reset_index()
            
            agg_mois['Taux_Utilisation_%'] = (agg_mois['Consommation_Totale'] / agg_mois['Dotation_Totale'] * 100).round(1)
            
            fig_global_mois = px.line(
                agg_mois,
                x='AnneeMois',
                y='Taux_Utilisation_%',
                title="Évolution Mensuelle du Taux d'Utilisation Global",
                labels={'AnneeMois': 'Mois', 'Taux_Utilisation_%': "Taux d'Utilisation (%)"},
                markers=True
            )
            
            fig_global_mois.add_hline(y=100, line_dash="dash", line_color="green", 
                                     annotation_text="Utilisation idéale (100%)")
            
            st.plotly_chart(fig_global_mois, use_container_width=True)

# ---------------------------------------------------------------------
# Fonctions d'Affichage des Pages
# ---------------------------------------------------------------------

def afficher_page_dashboard(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, df_ge: pd.DataFrame, df_autres: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche le tableau de bord principal."""
    st.header(f"📊 Tableau de Bord Principal ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    total_volume = df_transactions['Quantity'].sum()
    total_cout = df_transactions['Amount'].sum()
    nb_transactions = len(df_transactions)
    cartes_veh_actives = df_transactions[df_transactions['Card num.'].isin(df_vehicules['N° Carte'])]['Card num.'].nunique()
    prix_moyen_litre_global = (total_cout / total_volume) if total_volume > 0 else 0

    kpi_cat_dash, df_vehicle_kpi_dash = calculer_kpis_globaux(df_transactions, df_vehicules, date_debut, date_fin, list(st.session_state.ss_conso_seuils_par_categorie.keys()))
    conso_moyenne_globale = (kpi_cat_dash['total_litres'].sum() / kpi_cat_dash['distance_totale'].sum()) * 100 if not kpi_cat_dash.empty and kpi_cat_dash['distance_totale'].sum() > 0 else 0
    cout_km_global = (kpi_cat_dash['total_cout'].sum() / kpi_cat_dash['distance_totale'].sum()) if not kpi_cat_dash.empty and kpi_cat_dash['distance_totale'].sum() > 0 else 0

    df_anomalies_dash = detecter_anomalies(df_transactions, df_vehicules)
    cartes_inconnues_dash = verifier_cartes_inconnues(df_transactions, df_vehicules, df_ge, df_autres)
    vehicules_risques_dash = calculer_score_risque(df_anomalies_dash)
    nb_vehicules_suspects = len(vehicules_risques_dash[vehicules_risques_dash['score_risque'] >= st.session_state.ss_seuil_anomalies_suspectes_score]) if not vehicules_risques_dash.empty else 0
    nb_anomalies_critiques = len(df_anomalies_dash[df_anomalies_dash['poids_anomalie'] >= 8]) if not df_anomalies_dash.empty else 0 

    st.subheader("🚀 Indicateurs Clés")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Volume Total", f"{total_volume:,.0f} L")
    col2.metric("Coût Total", f"{total_cout:,.0f} CFA")
    col3.metric("Transactions", f"{nb_transactions:,}")
    col4.metric("Véhicules Actifs", f"{cartes_veh_actives:,}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Conso. Moyenne Globale", f"{conso_moyenne_globale:.1f} L/100km" if conso_moyenne_globale else "N/A")
    col6.metric("Coût Moyen / Km Global", f"{cout_km_global:.1f} CFA/km" if cout_km_global else "N/A")
    col7.metric("Prix Moyen / Litre", f"{prix_moyen_litre_global:,.0f} CFA/L" if prix_moyen_litre_global else "N/A")
    # col8.metric("placeholder", "...") 

    st.subheader("⚠️ Alertes Rapides")
    col_a1, col_a2, col_a3 = st.columns(3)
    col_a1.metric("Cartes Inconnues", len(cartes_inconnues_dash), delta_color="inverse")
    col_a2.metric(f"Véhicules Suspects (Score > {st.session_state.ss_seuil_anomalies_suspectes_score})", nb_vehicules_suspects, delta_color="inverse")
    col_a3.metric("Anomalies Critiques (Poids >= 8)", nb_anomalies_critiques, delta_color="inverse")

    st.subheader("📈 Graphiques Clés")
    with st.expander("Évolution Mensuelle Volume & Coût", expanded=True):
        evo_mensuelle = df_transactions.groupby(pd.Grouper(key='Date', freq='M')).agg(
            Volume_L=('Quantity', 'sum'),
            Cout_CFA=('Amount', 'sum')
        ).reset_index()
        evo_mensuelle['Mois'] = evo_mensuelle['Date'].dt.strftime('%Y-%m')
        fig_evo = px.bar(evo_mensuelle, x='Mois', y=['Volume_L', 'Cout_CFA'],
                         title="Évolution Mensuelle du Volume et du Coût",
                         labels={'value': 'Valeur', 'variable': 'Indicateur'}, barmode='group')
        fig_evo.update_layout(yaxis_title="Volume (L) / Coût (CFA)")
        st.plotly_chart(fig_evo, use_container_width=True)

    with st.expander("Répartition par Catégorie de Véhicule", expanded=False):
        if not kpi_cat_dash.empty:
             col_g1, col_g2 = st.columns(2)
             fig_pie_vol = px.pie(kpi_cat_dash, values='total_litres', names='Catégorie', title='Répartition Volume par Catégorie')
             col_g1.plotly_chart(fig_pie_vol, use_container_width=True)
             fig_pie_cout = px.pie(kpi_cat_dash, values='total_cout', names='Catégorie', title='Répartition Coût par Catégorie')
             col_g2.plotly_chart(fig_pie_cout, use_container_width=True)
        else:
             st.info("Données insuffisantes pour la répartition par catégorie.")

    with st.expander("Top 5 Véhicules (Coût / Volume / Anomalies)", expanded=False):
        if not df_vehicle_kpi_dash.empty:
             col_t1, col_t2 = st.columns(2)
             top_cout = df_vehicle_kpi_dash.nlargest(5, 'total_cout')[['Nouveau Immat', 'total_cout']]
             top_volume = df_vehicle_kpi_dash.nlargest(5, 'total_litres')[['Nouveau Immat', 'total_litres']]
             with col_t1: # Afficher dans les colonnes pour un meilleur layout
                afficher_dataframe_avec_export(top_cout, "Top 5 Coût Total", key="dash_top_cout")
             with col_t2:
                afficher_dataframe_avec_export(top_volume, "Top 5 Volume Total", key="dash_top_vol")
        else:
            st.info("Données insuffisantes pour le classement des véhicules.")

        if not vehicules_risques_dash.empty:
             top_risque = vehicules_risques_dash.nlargest(5, 'score_risque')[['Nouveau Immat', 'score_risque', 'nombre_total_anomalies']]
             afficher_dataframe_avec_export(top_risque, "Top 5 Score Risque", key="dash_top_risque")
        else:
             st.info("Aucune anomalie détectée pour le classement par risque.")

    if not cartes_inconnues_dash.empty:
        with st.expander("🚨 Cartes Inconnues Détectées", expanded=False):
            afficher_dataframe_avec_export(cartes_inconnues_dash, "Détail des Cartes Inconnues", key="dash_cartes_inconnues")


def afficher_page_analyse_vehicules(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut_globale: datetime.date, date_fin_globale: datetime.date, kpi_categories: pd.DataFrame):
    """Affiche la page d'analyse détaillée par véhicule."""
    st.header("🚗 Analyse Détaillée par Véhicule")

    veh_list = ["Sélectionner un véhicule..."] + sorted(df_vehicules['Nouveau Immat'].dropna().unique())
    vehicule_immat = st.selectbox("Choisir un véhicule par immatriculation", veh_list, index=0)

    if vehicule_immat == "Sélectionner un véhicule...":
        st.info("Veuillez sélectionner un véhicule dans la liste déroulante.")
        st.subheader("Statistiques Globales (tous véhicules sur période)")
        if not kpi_categories.empty:
            kpi_cat_sum = kpi_categories[[
                 'Catégorie', 'nb_vehicules', 'nb_transactions', 'total_litres', 'total_cout',
                 'distance_totale', 'consommation_globale', 'cout_par_km_global', 'prix_moyen_litre_global'
            ]]
            afficher_dataframe_avec_export(kpi_cat_sum, "Résumé par Catégorie", key="veh_global_cat_summary")
        else:
            st.warning("Aucune donnée KPI à afficher.")
        return

    try:
        info_vehicule = df_vehicules[df_vehicules['Nouveau Immat'] == vehicule_immat].iloc[0]
        carte_veh = info_vehicule['N° Carte']
    except IndexError:
        st.error(f"Impossible de trouver les informations pour le véhicule {vehicule_immat}.")
        return

    data_veh = df_transactions[df_transactions['Card num.'] == carte_veh].copy()

    if data_veh.empty:
        st.warning(f"Aucune transaction trouvée pour le véhicule {vehicule_immat} sur la période sélectionnée ({date_debut_globale.strftime('%d/%m/%Y')} - {date_fin_globale.strftime('%d/%m/%Y')}).")
        # Afficher quand même les infos de base, y compris la dotation
        dotation_veh_vide = info_vehicule.get('Dotation', 0)
        infos_base_vide_list = [
            ('Immatriculation', info_vehicule.get('Nouveau Immat', 'N/A')),
            ('Marque', info_vehicule.get('Marque', 'N/A')),
            ('Modèle', info_vehicule.get('Modèle', 'N/A')),
            ('Type', info_vehicule.get('Type', 'N/A')),
            ('Catégorie', info_vehicule.get('Catégorie', 'N/A')),
            ('Capacité réservoir', f"{info_vehicule.get('Cap-rèservoir', 0):.0f} L")
        ]
        if 'Dotation' in info_vehicule:
             infos_base_vide_list.append(('Dotation Mensuelle', f"{dotation_veh_vide:.0f} L"))
        infos_base_vide_list.extend([
            ('Période début', date_debut_globale.strftime(DATE_FORMAT)),
            ('Période fin', date_fin_globale.strftime(DATE_FORMAT))
        ])
        infos_base_vide = pd.DataFrame(infos_base_vide_list, columns=['Paramètre', 'Valeur'])
        afficher_dataframe_avec_export(infos_base_vide, "Informations du véhicule", key="df_infos_veh_vide")
        return

    st.subheader(f"Analyse du véhicule : {vehicule_immat} ({info_vehicule.get('Marque','')} {info_vehicule.get('Modèle','')})")

    categorie_veh = info_vehicule.get('Catégorie', 'N/A')
    conso_moyenne_cat = 0.0
    if not kpi_categories.empty and categorie_veh != 'N/A':
        ligne_cat = kpi_categories[kpi_categories['Catégorie'] == categorie_veh]
        if not ligne_cat.empty:
            conso_moyenne_cat = ligne_cat['consommation_globale'].iloc[0]

    infos_base, stats_conso, conso_mensuelle, stations_freq, analyse_detail = generer_rapport_vehicule(
        data_veh, info_vehicule, date_debut_globale, date_fin_globale, conso_moyenne_cat
    )

    col_info1, col_info2 = st.columns(2)
    with col_info1:
        afficher_dataframe_avec_export(infos_base, "Informations Véhicule", key=f"infos_{vehicule_immat}")
    with col_info2:
        afficher_dataframe_avec_export(stats_conso, "Statistiques Consommation & Coût", key=f"stats_{vehicule_immat}")

    st.markdown("### Graphiques")
    with st.expander("Graphiques détaillés du véhicule", expanded=False):
        col_g1, col_g2 = st.columns(2)
        fig_line = px.line(data_veh.sort_values('Date'), x='Date', y='Quantity', title="Consommation Journalière (Volume)", markers=True)
        col_g1.plotly_chart(fig_line, use_container_width=True)
        fig_hist = px.histogram(data_veh, x='Quantity', title="Distribution des Volumes Pris", nbins=20)
        col_g2.plotly_chart(fig_hist, use_container_width=True)
        if not conso_mensuelle.empty:
             fig_mens = px.bar(conso_mensuelle.reset_index(), x='mois', y=['Volume_L', 'Montant_CFA'], title="Évolution Mensuelle (Volume & Coût)", barmode='group')
             st.plotly_chart(fig_mens, use_container_width=True)

    st.markdown("### Stations")
    with st.expander("Stations les plus fréquentées", expanded=False):
        if not stations_freq.empty:
            station_df = stations_freq.reset_index()
            station_df.columns = ['Place', 'Nombre de visites']
            afficher_dataframe_avec_export(station_df, "Top 5 Stations", key=f"stations_{vehicule_immat}")
        else:
            st.info("Aucune donnée de station disponible.")

    st.markdown("### Anomalies Détectées")
    with st.expander("Détail des anomalies pour ce véhicule", expanded=True):
        anomalies_all = detecter_anomalies(df_transactions, df_vehicules) 
        anomalies_veh = anomalies_all[anomalies_all['Card num.'] == carte_veh].copy()

        if not anomalies_veh.empty:
            score_veh = anomalies_veh['poids_anomalie'].sum()
            nb_anom_veh = len(anomalies_veh)
            st.warning(f"🚨 {nb_anom_veh} anomalie(s) détectée(s) pour ce véhicule (Score de risque total: {score_veh}).")

            cols_display_anom = ['Date', 'Hour', 'type_anomalie', 'detail_anomalie', 'Quantity', 'Amount', 'distance_parcourue', 'consommation_100km', 'Place', 'poids_anomalie']
            cols_final_anom = [col for col in cols_display_anom if col in anomalies_veh.columns]
            afficher_dataframe_avec_export(anomalies_veh[cols_final_anom], "Liste des Anomalies", key=f"anom_detail_{vehicule_immat}")

            summary_anom_veh = anomalies_veh.groupby('type_anomalie').agg(
                 Nombre=('type_anomalie','size'),
                 Score_Partiel=('poids_anomalie','sum')
            ).reset_index().sort_values('Score_Partiel', ascending=False)
            afficher_dataframe_avec_export(summary_anom_veh, "Résumé des Anomalies par Type", key=f"anom_summary_{vehicule_immat}")
        else:
            st.success("✅ Aucune anomalie détectée pour ce véhicule sur la période sélectionnée.")


def afficher_page_analyse_couts(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page d'analyse des coûts."""
    st.header(f"💰 Analyse des Coûts ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    kpi_cat, df_vehicle_kpi = calculer_kpis_globaux(
        df_transactions, df_vehicules, date_debut, date_fin,
        list(st.session_state.ss_conso_seuils_par_categorie.keys()) 
    )
    if df_vehicle_kpi.empty:
         st.warning("Impossible de calculer les indicateurs de coûts (données de kilométrage ou transactions insuffisantes).")
         return

    tab1, tab2, tab3 = st.tabs(["📊 Coût par Km (CpK)", "📈 Tendances des Coûts", "⛽ Analyse par Station"])
    with tab1:
        st.subheader("Coût par Kilomètre (CpK) par Véhicule")
        cpk_veh = df_vehicle_kpi[['Nouveau Immat', 'Catégorie', 'cout_par_km', 'distance', 'total_cout']].dropna(subset=['cout_par_km']).sort_values('cout_par_km', ascending=False)
        afficher_dataframe_avec_export(cpk_veh, "Classement CpK par Véhicule", key="cpk_veh_table")

        st.subheader("Coût par Kilomètre (CpK) Moyen par Catégorie")
        if not kpi_cat.empty:
            cpk_cat = kpi_cat[['Catégorie', 'cout_par_km_global', 'distance_totale', 'total_cout']].dropna(subset=['cout_par_km_global']).sort_values('cout_par_km_global', ascending=False)
            afficher_dataframe_avec_export(cpk_cat, "CpK Moyen par Catégorie", key="cpk_cat_table")
            fig_cpk_cat = px.bar(cpk_cat, x='Catégorie', y='cout_par_km_global', title="Coût Moyen par Km Global par Catégorie", labels={'cout_par_km_global': 'CpK Global (CFA/km)'})
            st.plotly_chart(fig_cpk_cat, use_container_width=True)
        else:
            st.info("Données insuffisantes pour l'analyse CpK par catégorie.")
    with tab2:
        st.subheader("Tendances Mensuelles des Coûts")
        evo_mensuelle_cout = df_transactions.groupby(pd.Grouper(key='Date', freq='M')).agg(
            Cout_Total_CFA=('Amount', 'sum'),
            Volume_Total_L=('Quantity', 'sum')
        ).reset_index()
        evo_mensuelle_cout['Mois'] = evo_mensuelle_cout['Date'].dt.strftime('%Y-%m')
        evo_mensuelle_cout['Prix_Moyen_L'] = evo_mensuelle_cout['Cout_Total_CFA'] / evo_mensuelle_cout['Volume_Total_L']

        fig_trend_cout = px.line(evo_mensuelle_cout, x='Mois', y='Cout_Total_CFA', title="Évolution Mensuelle du Coût Total", markers=True, labels={'Cout_Total_CFA': 'Coût Total (CFA)'})
        st.plotly_chart(fig_trend_cout, use_container_width=True)
        fig_trend_prix_l = px.line(evo_mensuelle_cout, x='Mois', y='Prix_Moyen_L', title="Évolution Mensuelle du Prix Moyen au Litre", markers=True, labels={'Prix_Moyen_L': 'Prix Moyen (CFA/L)'})
        st.plotly_chart(fig_trend_prix_l, use_container_width=True)

        st.subheader("Transactions les Plus Coûteuses")
        top_trans_base = df_transactions.nlargest(10, 'Amount')
        top_transactions_merged = top_trans_base.merge(
            df_vehicules[['N° Carte', 'Nouveau Immat', 'Catégorie']],
            left_on='Card num.',right_on='N° Carte',how='left'
        )
        cols_to_display_top = ['Date', 'Hour', 'Nouveau Immat', 'Catégorie', 'Quantity', 'Amount', 'Place', 'Card num.']
        cols_final_top = [col for col in cols_to_display_top if col in top_transactions_merged.columns]
        afficher_dataframe_avec_export(top_transactions_merged[cols_final_top], "Top 10 Transactions par Montant", key="top_transac_amount")

    with tab3:
         st.subheader("Analyse des Coûts par Station")
         if 'Place' in df_transactions.columns:
             analyse_station = df_transactions.groupby('Place').agg(
                 Volume_Total_L=('Quantity', 'sum'),
                 Cout_Total_CFA=('Amount', 'sum'),
                 Nb_Transactions=('Quantity', 'count')
             ).reset_index()
             analyse_station['Prix_Moyen_L'] = analyse_station['Cout_Total_CFA'] / analyse_station['Volume_Total_L']
             analyse_station = analyse_station[analyse_station['Volume_Total_L'] > 0].sort_values('Cout_Total_CFA', ascending=False)

             if not analyse_station.empty:
                 afficher_dataframe_avec_export(analyse_station, "Résumé par Station", key="station_summary")
                 col_s1, col_s2 = st.columns(2)
                 top_n_stations = 15 
                 fig_station_cout = px.bar(analyse_station.head(top_n_stations), x='Place', y='Cout_Total_CFA', title=f"Top {top_n_stations} Stations par Coût Total", labels={'Cout_Total_CFA': 'Coût Total (CFA)'})
                 col_s1.plotly_chart(fig_station_cout, use_container_width=True)
                 fig_station_prix = px.bar(analyse_station.head(top_n_stations).sort_values('Prix_Moyen_L', ascending=False), x='Place', y='Prix_Moyen_L', title=f"Top {top_n_stations} Stations par Prix Moyen / Litre", labels={'Prix_Moyen_L': 'Prix Moyen (CFA/L)'})
                 col_s2.plotly_chart(fig_station_prix, use_container_width=True)
             else:
                 st.info("Aucune donnée de transaction avec information de station valide trouvée.")
         else:
             st.warning("La colonne 'Place' (nom de la station) est manquante dans le fichier de transactions pour effectuer cette analyse.")


def afficher_page_anomalies(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page de synthèse des anomalies."""
    st.header(f"🚨 Détection des Anomalies ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    with st.spinner("Détection des anomalies en cours..."):
         df_anomalies = detecter_anomalies(df_transactions, df_vehicules)
         df_scores = calculer_score_risque(df_anomalies)

    if df_anomalies.empty:
        st.success("✅ Aucune anomalie détectée sur la période sélectionnée !")
        return

    nb_total_anomalies = len(df_anomalies)
    nb_vehicules_avec_anomalies = df_anomalies['Card num.'].nunique()
    st.warning(f"Détecté : **{nb_total_anomalies:,}** anomalies concernant **{nb_vehicules_avec_anomalies:,}** véhicules.")

    st.subheader(f"🎯 Véhicules Suspects (Score de Risque ≥ {st.session_state.ss_seuil_anomalies_suspectes_score})")
    vehicules_suspects = df_scores[df_scores['score_risque'] >= st.session_state.ss_seuil_anomalies_suspectes_score]

    if not vehicules_suspects.empty:
        pivot_details = df_anomalies.groupby(['Nouveau Immat', 'Card num.', 'Catégorie', 'type_anomalie']).size().unstack(fill_value=0)
        vehicules_suspects_details = vehicules_suspects.merge(pivot_details, on=['Nouveau Immat', 'Card num.', 'Catégorie'], how='left')
        afficher_dataframe_avec_export(vehicules_suspects_details, f"Liste des {len(vehicules_suspects)} Véhicules Suspects", key="anom_suspects_score")

        with st.expander("Voir les transactions détaillées des véhicules suspects"):
            details_suspects = df_anomalies[df_anomalies['Card num.'].isin(vehicules_suspects['Card num.'])]
            cols_display_detail = ['Date', 'Hour', 'Nouveau Immat', 'Catégorie', 'type_anomalie', 'detail_anomalie', 'Quantity', 'Amount', 'Place', 'poids_anomalie']
            cols_final_detail = [col for col in cols_display_detail if col in details_suspects.columns]
            afficher_dataframe_avec_export(details_suspects[cols_final_detail], "Détail Transactions des Suspects", key="anom_suspects_details_transac")
    else:
        st.info("Aucun véhicule n'atteint le seuil de score de risque suspect.")

    st.subheader("📊 Synthèse par Type d'Anomalie")
    summary_type = df_anomalies.groupby('type_anomalie').agg(
        Nombre=('type_anomalie', 'size'),
        Score_Total=('poids_anomalie', 'sum'),
        Nb_Vehicules_Touches=('Card num.', 'nunique')
    ).reset_index().sort_values('Score_Total', ascending=False)
    afficher_dataframe_avec_export(summary_type, "Nombre et Score par Type d'Anomalie", key="anom_summary_type")

    fig_summary_type = px.bar(summary_type, x='type_anomalie', y='Nombre', title="Nombre d'Anomalies par Type", color='Score_Total', labels={'Nombre':"Nombre d'occurrences", 'type_anomalie':'Type d\'Anomalie'})
    st.plotly_chart(fig_summary_type, use_container_width=True)

    with st.expander("Voir toutes les anomalies détectées (tableau complet)"):
         cols_display_all = ['Date', 'Hour', 'Nouveau Immat', 'Catégorie', 'type_anomalie', 'detail_anomalie', 'Quantity', 'Amount', 'Place', 'poids_anomalie']
         cols_final_all = [col for col in cols_display_all if col in df_anomalies.columns]
         afficher_dataframe_avec_export(df_anomalies[cols_final_all], "Tableau Complet des Anomalies", key="anom_all_details")


def afficher_page_kpi(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page des Indicateurs Clés de Performance."""
    st.header(f"📈 Indicateurs Clés de Performance (KPIs) ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    all_cats = sorted(df_vehicules['Catégorie'].dropna().astype(str).unique())
    selected_cats_kpi = st.multiselect(
        "Filtrer par Catégories de véhicules",
        options=all_cats,default=all_cats,key="kpi_cat_filter"
    )
    with st.spinner("Calcul des KPIs..."):
        kpi_categories, df_vehicle_kpi = calculer_kpis_globaux(
            df_transactions, df_vehicules, date_debut, date_fin, selected_cats_kpi
        )

    if kpi_categories.empty or df_vehicle_kpi.empty:
        st.warning("Données insuffisantes pour calculer les KPIs pour les catégories sélectionnées.")
        return

    st.subheader("KPIs Agrégés par Catégorie")
    cols_kpi_cat_display = [
        'Catégorie', 'nb_vehicules', 'nb_transactions', 'total_litres', 'total_cout',
        'distance_totale', 'consommation_globale', 'cout_par_km_global', 'prix_moyen_litre_global'
    ]
    afficher_dataframe_avec_export(kpi_categories[cols_kpi_cat_display], f"KPIs pour {len(kpi_categories)} catégorie(s)", key="kpi_cat_table")

    col_gkpi1, col_gkpi2 = st.columns(2)
    fig_kpi_conso = px.bar(kpi_categories, x='Catégorie', y='consommation_globale', title="Consommation Globale par Catégorie", labels={'consommation_globale': 'Consommation (L/100km)'})
    col_gkpi1.plotly_chart(fig_kpi_conso, use_container_width=True)
    fig_kpi_cpk = px.bar(kpi_categories, x='Catégorie', y='cout_par_km_global', title="Coût par Km Global par Catégorie", labels={'cout_par_km_global': 'Coût par Km (CFA/km)'})
    col_gkpi2.plotly_chart(fig_kpi_cpk, use_container_width=True)

    with st.expander("Voir les KPIs détaillés par véhicule"):
        cols_kpi_veh_display = [
            'Nouveau Immat', 'Catégorie', 'nb_prises', 'total_litres', 'total_cout',
            'distance', 'consommation', 'cout_par_km', 'prix_moyen_litre', 'Dotation' # Ajout Dotation
        ]
        afficher_dataframe_avec_export(df_vehicle_kpi[cols_kpi_veh_display], f"KPIs pour {len(df_vehicle_kpi)} véhicule(s)", key="kpi_veh_table")

    with st.expander("📈 Analyse des Tendances d'Anomalies", expanded=False):
        st.info("L'analyse des tendances d'anomalies peut prendre du temps.")
        if st.button("Lancer l'analyse des tendances", key="btn_trend_anom"):
             with st.spinner("Calcul des tendances d'anomalies..."):
                 df_anomalies_kpi = detecter_anomalies(df_transactions, df_vehicules) 
                 if not df_anomalies_kpi.empty:
                     df_anomalies_kpi = df_anomalies_kpi[df_anomalies_kpi['Catégorie'].isin(selected_cats_kpi)]
                     if not df_anomalies_kpi.empty:
                         df_anomalies_kpi['Mois'] = df_anomalies_kpi['Date'].dt.to_period('M').astype(str)
                         trend_anom = df_anomalies_kpi.groupby(['Mois', 'type_anomalie']).size().reset_index(name='Nombre')
                         fig_trend = px.line(trend_anom, x='Mois', y='Nombre', color='type_anomalie',
                                              title="Évolution Mensuelle du Nombre d'Anomalies par Type",
                                              markers=True, labels={'type_anomalie': 'Type d\'Anomalie'})
                         st.plotly_chart(fig_trend, use_container_width=True)
                         afficher_dataframe_avec_export(trend_anom, "Données Tendances Anomalies", key="kpi_trend_anom_data")
                     else:
                         st.info("Aucune anomalie trouvée pour les catégories sélectionnées dans la période.")
                 else:
                     st.info("Aucune anomalie détectée globalement dans la période.")


def afficher_page_autres_cartes(df_transactions: pd.DataFrame, df_autres: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page d'analyse des 'Autres Cartes'."""
    st.header(f"💳 Analyse Autres Cartes ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_autres is None or df_autres.empty:
        st.info("Aucune 'Autre Carte' n'est définie dans le fichier des cartes.")
        return

    with st.expander("Liste des Autres Cartes Définies"):
        afficher_dataframe_avec_export(df_autres, "Liste des Autres Cartes", key="autres_cartes_liste")

    cartes_autres_list = df_autres['N° Carte'].unique()
    data_autres = df_transactions[df_transactions['Card num.'].isin(cartes_autres_list)].copy()

    if data_autres.empty:
        st.warning("Aucune transaction trouvée pour les 'Autres Cartes' sur la période sélectionnée.")
        return

    st.subheader("Consommation et Coût des Autres Cartes")
    group_cols = ['Card num.']
    if 'Card name' in data_autres.columns:
        group_cols.append('Card name')

    conso_autres = data_autres.groupby(group_cols).agg(
        Volume_Total_L=('Quantity', 'sum'),
        Cout_Total_CFA=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count'),
        Volume_Moyen_L=('Quantity', 'mean'),
        Cout_Moyen_CFA=('Amount', 'mean')
    ).reset_index().round(2)

    if 'Card name' not in conso_autres.columns:
         card_names = data_autres[['Card num.', 'Card name']].drop_duplicates()
         conso_autres = conso_autres.merge(card_names, on='Card num.', how='left')
         conso_autres['Card name'] = conso_autres['Card name'].fillna('N/A')
    afficher_dataframe_avec_export(conso_autres, "Résumé par Autre Carte", key="autres_cartes_summary")

    st.subheader("Évolution de la Consommation (Autres Cartes)")
    conso_temp_autres = data_autres.groupby(pd.Grouper(key='Date', freq='D'))['Quantity'].sum().reset_index()
    if not conso_temp_autres.empty:
        fig_autres_line = px.line(conso_temp_autres, x='Date', y='Quantity', title="Consommation Quotidienne (Volume) - Autres Cartes")
        st.plotly_chart(fig_autres_line, use_container_width=True)
    else:
        st.info("Pas assez de données pour afficher l'évolution quotidienne.")


def afficher_page_parametres(df_vehicules: Optional[pd.DataFrame] = None):
    """Affiche la page des paramètres modifiables."""
    st.header("⚙️ Paramètres de l'Application")
    st.warning("Modifier ces paramètres affectera les analyses et la détection d'anomalies.")

    with st.expander("Seuils Généraux d'Anomalies", expanded=True):
        st.session_state.ss_seuil_heures_rapprochees = st.number_input(
            "Seuil Prises Rapprochées (heures)",min_value=0.5, max_value=24.0,
            value=float(st.session_state.get('ss_seuil_heures_rapprochees', DEFAULT_SEUIL_HEURES_RAPPROCHEES)),
            step=0.5, format="%.1f", key='param_seuil_rappr'
        )
        st.session_state.ss_delta_minutes_facturation_double = st.number_input(
            "Delta Max Facturation Double (minutes)",min_value=1, max_value=180,
            value=st.session_state.get('ss_delta_minutes_facturation_double', DEFAULT_DELTA_MINUTES_FACTURATION_DOUBLE),
            step=1, key='param_delta_double'
        )
        st.session_state.ss_seuil_anomalies_suspectes_score = st.number_input(
            "Seuil Score de Risque Suspect",min_value=1, max_value=1000,
            value=st.session_state.get('ss_seuil_anomalies_suspectes_score', DEFAULT_SEUIL_ANOMALIES_SUSPECTES_SCORE),
            step=1, key='param_seuil_score'
        )
    with st.expander("Heures Non Ouvrées"):
        st.session_state.ss_heure_debut_non_ouvre = st.slider(
            "Heure Début Période Non Ouvrée",min_value=0, max_value=23,
            value=st.session_state.get('ss_heure_debut_non_ouvre', DEFAULT_HEURE_DEBUT_NON_OUVRE),
            step=1, key='param_heure_debut_no'
        )
        st.session_state.ss_heure_fin_non_ouvre = st.slider(
            "Heure Fin Période Non Ouvrée (exclusive)",min_value=0, max_value=23,
            value=st.session_state.get('ss_heure_fin_non_ouvre', DEFAULT_HEURE_FIN_NON_OUVRE),
            step=1, key='param_heure_fin_no'
        )
        st.caption(f"Plage non ouvrée actuelle (approximative): de {st.session_state.ss_heure_debut_non_ouvre}h à {st.session_state.ss_heure_fin_non_ouvre}h (hors weekend).")

    with st.expander("Seuils de Consommation par Catégorie (L/100km)"):
        if df_vehicules is not None and st.session_state.get('data_loaded', False):
            current_seuils = st.session_state.get('ss_conso_seuils_par_categorie', {})
            all_cats = sorted(current_seuils.keys()) 
            new_seuils = {}
            cols = st.columns(3) 
            col_idx = 0
            for cat in all_cats:
                with cols[col_idx % 3]:
                     new_seuils[cat] = st.number_input(
                         f"Seuil {cat}",min_value=5.0, max_value=100.0,
                         value=float(current_seuils.get(cat, DEFAULT_CONSO_SEUIL)), 
                         step=0.5, format="%.1f",key=f"param_seuil_conso_{cat}"
                     )
                col_idx += 1
            st.session_state.ss_conso_seuils_par_categorie = new_seuils
        else:
            st.info("Chargez les données pour définir les seuils par catégorie.")
            st.number_input("Seuil Consommation par Défaut (utilisé si catégorie non définie)", value=DEFAULT_CONSO_SEUIL, disabled=True)
    
    with st.expander("Paramètres d'Analyse des Dotations"):
        st.session_state.ss_seuil_sous_utilisation = st.slider(
            "Seuil de sous-utilisation des dotations (%)", 
            min_value=30, max_value=90, 
            value=st.session_state.get('ss_seuil_sous_utilisation', DEFAULT_SEUIL_SOUS_UTILISATION),
            step=5, key='param_seuil_sous_util',
            help="Un véhicule est considéré en sous-utilisation quand son taux d'utilisation est inférieur à ce seuil."
        )
        
        st.session_state.ss_seuil_sur_utilisation = st.slider(
            "Seuil de sur-utilisation des dotations (%)", 
            min_value=100, max_value=150, 
            value=st.session_state.get('ss_seuil_sur_utilisation', DEFAULT_SEUIL_SUR_UTILISATION),
            step=5, key='param_seuil_sur_util',
            help="Un véhicule est considéré en sur-utilisation quand son taux d'utilisation dépasse ce seuil."
        )
        
        st.session_state.ss_mois_min_analyse = st.slider(
            "Nombre minimum de mois pour l'analyse fiable", 
            min_value=1, max_value=6, 
            value=st.session_state.get('ss_mois_min_analyse', DEFAULT_MOIS_MIN_ANALYSE),
            step=1, key='param_mois_min',
            help="Nombre minimum de mois de données requis pour considérer l'analyse des tendances comme fiable."
        )

    with st.expander("Poids des Anomalies pour Score de Risque"):
        st.caption("Ajustez l'importance de chaque type d'anomalie dans le calcul du score de risque.")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.session_state.ss_poids_conso_excessive = st.slider("Poids: Conso. Excessive", 1, 15, st.session_state.get('ss_poids_conso_excessive', DEFAULT_POIDS_CONSO_EXCESSIVE), key='poids_cex')
            st.session_state.ss_poids_depassement_capacite = st.slider("Poids: Dépassement Capacité", 1, 15, st.session_state.get('ss_poids_depassement_capacite', DEFAULT_POIDS_DEPASSEMENT_CAPACITE), key='poids_dep')
            st.session_state.ss_poids_prises_rapprochees = st.slider("Poids: Prises Rapprochées", 1, 15, st.session_state.get('ss_poids_prises_rapprochees', DEFAULT_POIDS_PRISES_RAPPROCHEES), key='poids_rap')
        with c2:
            st.session_state.ss_poids_km_decroissant = st.slider("Poids: Km Décroissant", 1, 15, st.session_state.get('ss_poids_km_decroissant', DEFAULT_POIDS_KM_DECROISSANT), key='poids_kmd')
            st.session_state.ss_poids_km_inchange = st.slider("Poids: Km Inchangé", 1, 15, st.session_state.get('ss_poids_km_inchange', DEFAULT_POIDS_KM_INCHANGE), key='poids_kmi')
            st.session_state.ss_poids_km_saut = st.slider("Poids: Saut Km Important", 1, 15, st.session_state.get('ss_poids_km_saut', DEFAULT_POIDS_KM_SAUT), key='poids_kms')
        with c3:
            st.session_state.ss_poids_hors_horaire = st.slider("Poids: Hors Horaires/WE", 1, 15, st.session_state.get('ss_poids_hors_horaire', DEFAULT_POIDS_HORS_HORAIRE), key='poids_hor')
            st.session_state.ss_poids_hors_service = st.slider("Poids: Véhicule Hors Service", 1, 15, st.session_state.get('ss_poids_hors_service', DEFAULT_POIDS_HORS_SERVICE), key='poids_hsv')
            st.session_state.ss_poids_fact_double = st.slider("Poids: Facturation Double", 1, 15, st.session_state.get('ss_poids_fact_double', DEFAULT_POIDS_FACT_DOUBLE), key='poids_dbl')

    st.markdown("---")
    st.info("Les paramètres sont sauvegardés automatiquement pendant la session.")


# ---------------------------------------------------------------------
# Point d'entrée avec navigation mise à jour
# ---------------------------------------------------------------------
def main():
    st.title("📊 Gestion & Analyse Cartes Carburant")

    st.sidebar.header("1. Chargement des Données")
    fichier_transactions = st.sidebar.file_uploader("Fichier Transactions (CSV)", type=['csv'])
    fichier_cartes = st.sidebar.file_uploader("Fichier Cartes (Excel)", type=['xlsx', 'xls'])

    if not fichier_transactions or not fichier_cartes:
        st.info("👋 Bienvenue ! Veuillez charger le fichier des transactions (CSV) et le fichier des cartes (Excel) via la barre latérale pour commencer.")
        initialize_session_state() 
        if st.sidebar.radio("Navigation", ["Paramètres"], index=0, key="nav_no_data") == "Paramètres": # Clé unique
            afficher_page_parametres()
        return

    df_transactions, df_vehicules, df_ge, df_autres = charger_donnees(fichier_transactions, fichier_cartes)

    if df_transactions is None or df_vehicules is None or df_ge is None or df_autres is None:
        st.error("❌ Erreur lors du chargement ou de la validation des fichiers. Veuillez vérifier les fichiers et les colonnes requises.")
        st.session_state['data_loaded'] = False
        return 

    st.session_state['data_loaded'] = True
    st.sidebar.success("✅ Données chargées avec succès !")
    min_date, max_date = df_transactions['Date'].min(), df_transactions['Date'].max()
    st.sidebar.markdown(f"**Transactions :** {len(df_transactions):,}")
    st.sidebar.markdown(f"**Période :** {min_date.strftime('%d/%m/%Y')} - {max_date.strftime('%d/%m/%Y')}")

    initialize_session_state(df_vehicules)

    st.sidebar.header("2. Période d'Analyse Globale")
    col_date1, col_date2 = st.sidebar.columns(2)
    global_date_debut = col_date1.date_input("Date Début", min_date.date(), min_value=min_date.date(), max_value=max_date.date(), key="global_date_debut")
    global_date_fin = col_date2.date_input("Date Fin", max_date.date(), min_value=min_date.date(), max_value=max_date.date(), key="global_date_fin")

    if global_date_debut > global_date_fin:
        st.sidebar.error("La date de début ne peut pas être postérieure à la date de fin.")
        return

    mask_global_date = (df_transactions['Date'].dt.date >= global_date_debut) & (df_transactions['Date'].dt.date <= global_date_fin)
    df_transac_filtered = df_transactions[mask_global_date].copy()

    if df_transac_filtered.empty:
         st.warning("Aucune transaction trouvée pour la période sélectionnée.")
    else:
         st.sidebar.info(f"{len(df_transac_filtered):,} transactions dans la période sélectionnée.")

    st.sidebar.header("3. Navigation")
    pages = [
        "Tableau de Bord", "Analyse Véhicules", "Analyse des Coûts", 
        "Analyse par Période", "Suivi des Dotations", "Anomalies", "KPIs", "Autres Cartes", "Paramètres" # Ajout Suivi Dotations
    ]
    # Laisser toutes les pages accessibles même si df_transac_filtered est vide, les pages géreront l'affichage.
    page = st.sidebar.radio("Choisir une page :", pages, key="navigation_main")


    if page == "Tableau de Bord":
        kpi_cat_dashboard, df_vehicle_kpi_dashboard = calculer_kpis_globaux(
            df_transac_filtered, df_vehicules, global_date_debut, global_date_fin,
            list(st.session_state.ss_conso_seuils_par_categorie.keys()) 
        )
        afficher_page_dashboard(df_transac_filtered, df_vehicules, df_ge, df_autres, global_date_debut, global_date_fin)
        ameliorer_dashboard(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin, 
                        kpi_cat_dashboard, df_vehicle_kpi_dashboard)
    elif page == "Analyse Véhicules":
         kpi_cat_veh_page, _ = calculer_kpis_globaux( # Renommer pour éviter conflit
             df_transac_filtered, df_vehicules, global_date_debut, global_date_fin,
             list(st.session_state.ss_conso_seuils_par_categorie.keys()) 
         )
         afficher_page_analyse_vehicules(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin, kpi_cat_veh_page)
    elif page == "Analyse des Coûts":
         afficher_page_analyse_couts(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Analyse par Période":
         afficher_page_analyse_periodes(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Suivi des Dotations": # NOUVELLE PAGE
         afficher_page_suivi_dotations(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Anomalies":
        afficher_page_anomalies(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "KPIs":
        afficher_page_kpi(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Autres Cartes":
        afficher_page_autres_cartes(df_transac_filtered, df_autres, global_date_debut, global_date_fin)
    elif page == "Paramètres":
        afficher_page_parametres(df_vehicules) 


if __name__ == "__main__":
    main()
