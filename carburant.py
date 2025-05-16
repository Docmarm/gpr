import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta # timedelta est utilisé dans la nouvelle fonction
import io
import os
from typing import Dict, List, Tuple, Optional, Any
from dateutil.relativedelta import relativedelta # Pour calculer le nombre de mois
from haversine import haversine, Unit # Pour calculer la distance entre coordonnées GPS
import folium
from streamlit_folium import folium_static
from PIL import Image
import base64
from io import BytesIO

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

# --- Nouveaux poids pour anomalies de géolocalisation ---
DEFAULT_POIDS_TRAJET_HORS_HEURES = 6
DEFAULT_POIDS_TRAJET_WEEKEND = 5
DEFAULT_POIDS_ARRETS_FREQUENTS = 4
DEFAULT_POIDS_DETOUR_SUSPECT = 7
DEFAULT_POIDS_TRANSACTION_SANS_PRESENCE = 9
DEFAULT_POIDS_VITESSE_EXCESSIVE = 8

# --- Valeurs par défaut pour les paramètres de géolocalisation ---
DEFAULT_RAYON_STATION_KM = 0.3  # Rayon autour d'une station pour considérer le véhicule présent
DEFAULT_SEUIL_ARRET_MINUTES = 5  # Durée minimum pour considérer un arrêt
DEFAULT_SEUIL_DETOUR_PCT = 20    # Pourcentage au-delà duquel un trajet est considéré comme détour
DEFAULT_HEURE_DEBUT_SERVICE = 7  # Heure normale de début de service
DEFAULT_HEURE_FIN_SERVICE = 19   # Heure normale de fin de service
DEFAULT_NB_ARRETS_SUSPECT = 4    # Nombre d'arrêts au-delà duquel c'est suspect pour un trajet court

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
        'data_loaded': False, # Indicateur de chargement des données
        # Nouveaux paramètres pour géolocalisation
        'ss_rayon_station_km': DEFAULT_RAYON_STATION_KM,
        'ss_seuil_arret_minutes': DEFAULT_SEUIL_ARRET_MINUTES,
        'ss_seuil_detour_pct': DEFAULT_SEUIL_DETOUR_PCT,
        'ss_heure_debut_service': DEFAULT_HEURE_DEBUT_SERVICE,
        'ss_heure_fin_service': DEFAULT_HEURE_FIN_SERVICE,
        'ss_nb_arrets_suspect': DEFAULT_NB_ARRETS_SUSPECT,
        # Nouveaux poids pour anomalies de géolocalisation
        'ss_poids_trajet_hors_heures': DEFAULT_POIDS_TRAJET_HORS_HEURES,
        'ss_poids_trajet_weekend': DEFAULT_POIDS_TRAJET_WEEKEND,
        'ss_poids_arrets_frequents': DEFAULT_POIDS_ARRETS_FREQUENTS,
        'ss_poids_detour_suspect': DEFAULT_POIDS_DETOUR_SUSPECT,
        'ss_poids_transaction_sans_presence': DEFAULT_POIDS_TRANSACTION_SANS_PRESENCE,
        'ss_poids_vitesse_excessive': DEFAULT_POIDS_VITESSE_EXCESSIVE
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

# MODIFIED FUNCTION
@st.cache_data(show_spinner="Chargement et nettoyage du fichier de géolocalisation...")
def charger_donnees_geolocalisation(fichier_geoloc) -> Optional[pd.DataFrame]:
    """Charge et nettoie les données du fichier de géolocalisation."""
    if fichier_geoloc is None:
        return None

    try:
        # Chargement en spécifiant les types pour éviter les problèmes de conversion
        df_geoloc = pd.read_excel(fichier_geoloc, engine='openpyxl', dtype={
            'Début': str, 'Fin': str, 'Durée': str, 'Distance': str
        })

        # Vérification des colonnes essentielles
        colonnes_attendues = ['Véhicule', 'Date', 'Début', 'Fin', 'Durée', 'Distance', 'Type']
        # Ajout des coordonnées si elles existent (pour la carte)
        optional_coords = ['Latitude_depart', 'Longitude_depart', 'Latitude_arrivee', 'Longitude_arrivee']
        for oc in optional_coords:
            if oc in df_geoloc.columns:
                colonnes_attendues.append(oc)

        missing_cols = [col for col in colonnes_attendues if col not in df_geoloc.columns and col not in optional_coords]
        # Re-check missing_cols only for mandatory ones if some optional are not found
        mandatory_cols = ['Véhicule', 'Date', 'Début', 'Fin', 'Durée', 'Distance', 'Type']
        missing_mandatory_cols = [col for col in mandatory_cols if col not in df_geoloc.columns]


        if missing_mandatory_cols:
            st.error(f"Colonnes manquantes dans le fichier de géolocalisation: {', '.join(missing_mandatory_cols)}")
            return None

        # Normalisation du type (Trajet/Arrêt)
        if 'Type' in df_geoloc.columns:
            df_geoloc['Type'] = df_geoloc['Type'].astype(str).str.strip()
            type_corrections = {
                'trajet': 'Trajet', 'TRAJET': 'Trajet', 'Tragets': 'Trajet',
                'arret': 'Arrêt', 'ARRÊT': 'Arrêt', 'Arrèt': 'Arrêt', 'Arret': 'Arrêt', 'ArrËt': 'Arrêt'
            }
            df_geoloc['Type'] = df_geoloc['Type'].replace(type_corrections)

        # Conversion des dates
        try:
            df_geoloc['Date'] = pd.to_datetime(df_geoloc['Date'], format='%d/%m/%Y', errors='coerce')
        except:
            # Essayer sans format spécifique si le format standard échoue
            df_geoloc['Date'] = pd.to_datetime(df_geoloc['Date'], errors='coerce')

        if df_geoloc['Date'].isna().all():
            st.error("Impossible de convertir la colonne 'Date' en format date")
            return None

        # Conversion des heures de début et fin
        for col in ['Début', 'Fin']:
            df_geoloc[f'{col}_str'] = df_geoloc[col].astype(str).replace(['NaT', 'nan', '<NA>', 'None', 'nat', 'NAT', 'none'], '', regex=False).str.split('.').str[0]

        date_str_series = df_geoloc['Date'].dt.strftime('%Y-%m-%d')

        # Conversion des heures de début
        df_geoloc['DateTime_Debut'] = pd.NaT
        valid_debut_mask = df_geoloc['Date'].notna() & (df_geoloc['Début_str'] != '')
        if valid_debut_mask.any():
            combined_debut_str = date_str_series[valid_debut_mask] + ' ' + df_geoloc.loc[valid_debut_mask, 'Début_str']
            df_geoloc.loc[valid_debut_mask, 'DateTime_Debut'] = pd.to_datetime(combined_debut_str, errors='coerce')

        # Conversion des heures de fin
        df_geoloc['DateTime_Fin'] = pd.NaT
        valid_fin_mask = df_geoloc['Date'].notna() & (df_geoloc['Fin_str'] != '')
        if valid_fin_mask.any():
            combined_fin_str = date_str_series[valid_fin_mask] + ' ' + df_geoloc.loc[valid_fin_mask, 'Fin_str']
            df_geoloc.loc[valid_fin_mask, 'DateTime_Fin'] = pd.to_datetime(combined_fin_str, errors='coerce')

        # Conversion des durées en timedelta
        df_geoloc['Durée_str'] = df_geoloc['Durée'].astype(str).replace(['NaT', 'nan', '<NA>', 'None', 'nat', 'NAT', 'none'], '', regex=False).str.split('.').str[0]
        df_geoloc['Durée_minutes'] = pd.to_timedelta(df_geoloc['Durée_str'], errors='coerce').dt.total_seconds() / 60

        # Recalculer les durées manquantes à partir de début et fin
        mask_recalc_duree = (df_geoloc['Type'] == 'Trajet') & df_geoloc['Durée_minutes'].isna() & df_geoloc['DateTime_Debut'].notna() & df_geoloc['DateTime_Fin'].notna() & (df_geoloc['DateTime_Fin'] > df_geoloc['DateTime_Debut'])
        if mask_recalc_duree.any():
            df_geoloc.loc[mask_recalc_duree, 'Durée_minutes'] = (df_geoloc.loc[mask_recalc_duree, 'DateTime_Fin'] - df_geoloc.loc[mask_recalc_duree, 'DateTime_Debut']).dt.total_seconds() / 60

        # Si la fin est avant le début, on ajoute un jour à la fin (trajet à cheval sur minuit)
        mask_nuit = df_geoloc['DateTime_Fin'] < df_geoloc['DateTime_Debut']
        if mask_nuit.any(): # Check if any True values before attempting .loc
            df_geoloc.loc[mask_nuit, 'DateTime_Fin'] = df_geoloc.loc[mask_nuit, 'DateTime_Fin'] + timedelta(days=1)


        # *** NOUVEAU TRAITEMENT DES DISTANCES ***
        # Nettoyer et convertir la colonne Distance
        distance_str = df_geoloc['Distance'].astype(str)
        # Enlever "km" et tout espace en fin de chaîne
        distance_str = distance_str.str.strip().str.replace(r'\s*km$', '', regex=True, case=False)
        # Enlever les points avant les séparateurs de milliers (1.234,56 -> 1234,56)
        distance_str = distance_str.str.replace(r'\.(?=\d{3},)', '', regex=True)
        # Remplacer la virgule par un point décimal
        distance_str = distance_str.str.replace(',', '.', regex=False)
        # Enlever tous les espaces
        distance_str = distance_str.str.replace(r'\s+', '', regex=True)
        # Convertir en numérique
        df_geoloc['Distance'] = pd.to_numeric(distance_str, errors='coerce')

        # Calcul de la vitesse moyenne pour les trajets
        df_geoloc['Vitesse moyenne'] = np.nan
        trajets_mask = df_geoloc['Type'] == 'Trajet'
        if trajets_mask.any():
            trajets_df = df_geoloc[trajets_mask]
            durees_minutes = trajets_df['Durée_minutes']
            distances = trajets_df['Distance']
            calcul_valide_mask = durees_minutes.notna() & (durees_minutes > 0) & distances.notna() & (distances > 0)
            if calcul_valide_mask.any():
                duree_heures_valides = durees_minutes[calcul_valide_mask] / 60  # minutes -> heures
                distances_valides = distances[calcul_valide_mask]
                vitesses = (distances_valides / duree_heures_valides).round(2)
                df_geoloc.loc[trajets_df[calcul_valide_mask].index, 'Vitesse moyenne'] = vitesses

        # Ajout des jours de la semaine et indication weekend
        df_geoloc['Jour_semaine'] = df_geoloc['Date'].dt.dayofweek
        df_geoloc['Est_weekend'] = df_geoloc['Jour_semaine'] >= 5

        # Ajout de l'heure de début en format numérique
        df_geoloc['Heure_debut'] = df_geoloc['DateTime_Debut'].dt.hour

        # Création d'une colonne AnnéeMois pour le regroupement
        df_geoloc['AnnéeMois'] = df_geoloc['Date'].dt.to_period('M').astype(str)

        # Suppression des lignes avec des données cruciales manquantes
        df_geoloc.dropna(subset=['Véhicule', 'Date', 'Type'], inplace=True)

        # Nettoyage des colonnes temporaires
        columns_to_drop = ['Début_str', 'Fin_str', 'Durée_str']
        df_geoloc.drop(columns=columns_to_drop, errors='ignore', inplace=True)

        # Coordonnées GPS (vérifier si elles existent avant de les traiter)
        if 'Latitude_depart' in df_geoloc.columns and 'Longitude_depart' in df_geoloc.columns:
            df_geoloc['Coordonnees_depart'] = df_geoloc.apply(
                lambda row: (row['Latitude_depart'], row['Longitude_depart'])
                if pd.notna(row['Latitude_depart']) and pd.notna(row['Longitude_depart'])
                else None,
                axis=1
            )
        else:
            df_geoloc['Latitude_depart'] = np.nan
            df_geoloc['Longitude_depart'] = np.nan
            df_geoloc['Coordonnees_depart'] = None

        if 'Latitude_arrivee' in df_geoloc.columns and 'Longitude_arrivee' in df_geoloc.columns:
            df_geoloc['Coordonnees_arrivee'] = df_geoloc.apply(
                lambda row: (row['Latitude_arrivee'], row['Longitude_arrivee'])
                if pd.notna(row['Latitude_arrivee']) and pd.notna(row['Longitude_arrivee'])
                else None,
                axis=1
            )
        else:
            df_geoloc['Latitude_arrivee'] = np.nan
            df_geoloc['Longitude_arrivee'] = np.nan
            df_geoloc['Coordonnees_arrivee'] = None


        return df_geoloc

    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier de géolocalisation : {e}")
        return None


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
# NOUVELLES FONCTIONS POUR LE SUIVI DES DOTATIONS
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
    conso_totale_periode_veh['Taux_Utilisation_Periode_%'] = conso_totale_periode_veh['Taux_Utilisation_Periode_%'].round(1)

    # Sélection et ordre des colonnes
    cols_recap = ['Nouveau Immat', 'Catégorie', 'Dotation_Mensuelle_L', 'Nb_Mois_Periode',
                  'Dotation_Allouee_Periode_L', 'Consommation_Reelle_Periode_L',
                  'Difference_Periode_L', 'Taux_Utilisation_Periode_%']
    df_recap_dotation_periode = conso_totale_periode_veh[cols_recap]

    cols_detail = ['Nouveau Immat', 'Catégorie', 'AnneeMois', 'Dotation_Mensuelle_L',
                   'Consommation_Mois_L', 'Difference_Mois_L', 'Taux_Utilisation_Mois_%']
    df_detail_dotation_mensuel = conso_mensuelle_veh[cols_detail].sort_values(['Nouveau Immat', 'AnneeMois'])

    return df_recap_dotation_periode, df_detail_dotation_mensuel


def afficher_page_suivi_dotations(
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
):
    """Affiche la page de suivi des dotations."""
    st.header(f"⛽ Suivi des Dotations Carburant ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if 'Dotation' not in df_vehicules.columns or df_vehicules['Dotation'].sum() == 0:
        st.warning("Aucune donnée de dotation n'est disponible ou les dotations sont toutes à zéro. Le suivi des dotations ne peut pas être effectué.")
        st.info("Veuillez vérifier que la colonne 'Dotation' (représentant la dotation mensuelle en litres) est présente et correctement renseignée dans votre fichier 'CARTES VEHICULE'.")
        return

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    # --- Filtres ---
    st.sidebar.subheader("Filtres pour Suivi Dotations")
    all_cats_dot = sorted(df_vehicules['Catégorie'].dropna().astype(str).unique())
    selected_cats_dot = st.sidebar.multiselect(
        "Filtrer par Catégories", options=all_cats_dot, default=all_cats_dot, key="dot_cat_filter"
    )

    vehicules_filtrables = df_vehicules[df_vehicules['Catégorie'].isin(selected_cats_dot)]['Nouveau Immat'].dropna().unique()
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
    if selected_vehicle_dot != "Tous les véhicules":
        df_recap_filtered = df_recap_filtered[df_recap_filtered['Nouveau Immat'] == selected_vehicle_dot]
        df_detail_mensuel_filtered = df_detail_mensuel[
            (df_detail_mensuel['Nouveau Immat'] == selected_vehicle_dot) &
            (df_detail_mensuel['Catégorie'].isin(selected_cats_dot)) # Garder filtre catégorie aussi
        ]
    else:
        df_detail_mensuel_filtered = df_detail_mensuel[df_detail_mensuel['Catégorie'].isin(selected_cats_dot)]


    # --- Affichage Récapitulatif par Véhicule sur la Période ---
    st.subheader("Récapitulatif de l'Utilisation des Dotations sur la Période")
    afficher_dataframe_avec_export(df_recap_filtered, "Récapitulatif Dotations par Véhicule", key="recap_dot_veh")

    if not df_recap_filtered.empty:
        fig_taux_utilisation = px.bar(
            df_recap_filtered.sort_values('Taux_Utilisation_Periode_%', ascending=False).head(20), # Top 20
            x='Nouveau Immat',
            y='Taux_Utilisation_Periode_%',
            color='Catégorie',
            title="Taux d'Utilisation des Dotations par Véhicule (%) - Top 20",
            labels={'Taux_Utilisation_Periode_%': "Taux d'Utilisation (%)"},
            hover_data=['Dotation_Allouee_Periode_L', 'Consommation_Reelle_Periode_L']
        )
        fig_taux_utilisation.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="Objectif 100%")
        st.plotly_chart(fig_taux_utilisation, use_container_width=True)

    # --- Affichage Détail Mensuel (si un véhicule est sélectionné) ---
    if selected_vehicle_dot != "Tous les véhicules":
        st.subheader(f"Détail Mensuel pour le Véhicule : {selected_vehicle_dot}")
        if not df_detail_mensuel_filtered.empty:
            afficher_dataframe_avec_export(df_detail_mensuel_filtered, f"Détail Mensuel Dotations - {selected_vehicle_dot}", key="detail_dot_mensuel_veh")

            fig_detail_veh = px.line(
                df_detail_mensuel_filtered,
                x='AnneeMois',
                y=['Dotation_Mensuelle_L', 'Consommation_Mois_L'],
                title=f"Dotation vs Consommation Mensuelle - {selected_vehicle_dot}",
                labels={'value': 'Volume (L)', 'variable': 'Type'},
                markers=True
            )
            st.plotly_chart(fig_detail_veh, use_container_width=True)
        else:
            st.info(f"Aucun détail mensuel à afficher pour {selected_vehicle_dot} avec les filtres actuels.")
    else:
        with st.expander("Voir le détail mensuel pour tous les véhicules (peut être long)"):
             afficher_dataframe_avec_export(df_detail_mensuel_filtered, "Détail Mensuel Dotations - Tous Véhicules Filtrés", key="detail_dot_mensuel_all_veh")

# ---------------------------------------------------------------------
# Fonctions pour l'analyse de géolocalisation
# ---------------------------------------------------------------------

def analyser_geolocalisation_vs_transactions(
    df_geoloc: pd.DataFrame,
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compare les données de géolocalisation avec les transactions carburant.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        df_transactions: DataFrame des transactions.
        df_vehicules: DataFrame des véhicules.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un tuple de DataFrames:
        - comparaison_par_vehicule: Comparaison des distances pour chaque véhicule.
        - anomalies_distance: Anomalies détectées dans les déclarations de distance.
    """
    # Filtrer les données pour la période spécifiée
    mask_date_geoloc = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_geoloc_filtre = df_geoloc[mask_date_geoloc].copy()

    mask_date_trans = (df_transactions['Date'].dt.date >= date_debut) & (df_transactions['Date'].dt.date <= date_fin)
    df_trans_filtre = df_transactions[mask_date_trans].copy()

    # Fusionner transactions avec infos véhicules pour avoir l'immatriculation
    df_trans_avec_immat = df_trans_filtre.merge(
        df_vehicules[['N° Carte', 'Nouveau Immat']],
        left_on='Card num.',
        right_on='N° Carte',
        how='inner'
    )

    # Agréger les distances par immatriculation
    # Filtrer sur Type == 'Trajet' pour ne sommer que les distances des trajets
    trajets_geoloc = df_geoloc_filtre[df_geoloc_filtre['Type'] == 'Trajet']
    distance_geoloc = trajets_geoloc.groupby('Véhicule').agg(
        Distance_Geoloc_Totale=('Distance', 'sum'),
        Nb_Trajets=('Distance', 'count'), # Compte le nombre de segments de trajet
        Distance_Moyenne_Trajet=('Distance', 'mean'),
        Vitesse_Moyenne_Globale=('Vitesse moyenne', 'mean'), # Mean of average speeds of segments
        Vitesse_Max_Observee=('Vitesse moyenne', 'max'), # Max of average speeds of segments
        Duree_Totale_Minutes=('Durée_minutes', 'sum')
    ).reset_index()

    # Calculer les écarts de kilométrage dans les transactions
    df_trans_avec_immat['distance_declaree'] = df_trans_avec_immat['Current mileage'] - df_trans_avec_immat['Past mileage']

    # Agréger par immatriculation
    distance_trans = df_trans_avec_immat.groupby('Nouveau Immat').agg(
        Distance_Declaree_Totale=('distance_declaree', lambda x: x[x > 0].sum()),  # Somme des distances positives
        Nb_Transactions=('distance_declaree', 'count'),
        Volume_Carburant_Total=('Quantity', 'sum')
    ).reset_index()

    # Fusionner les deux ensembles de données
    # Normaliser les noms de colonnes pour la fusion
    distance_geoloc.rename(columns={'Véhicule': 'Immatriculation'}, inplace=True)
    distance_trans.rename(columns={'Nouveau Immat': 'Immatriculation'}, inplace=True)

    comparaison = distance_geoloc.merge(
        distance_trans,
        on='Immatriculation',
        how='outer'
    )

    # Calculer les écarts et pourcentages
    comparaison['Ecart_Distance'] = comparaison['Distance_Declaree_Totale'] - comparaison['Distance_Geoloc_Totale']
    comparaison['Pourcentage_Ecart'] = np.where(
        comparaison['Distance_Geoloc_Totale'] > 0,
        (comparaison['Ecart_Distance'] / comparaison['Distance_Geoloc_Totale']) * 100,
        np.nan
    )

    # Calculer la consommation aux 100km basée sur la distance géolocalisée (plus fiable)
    comparaison['Consommation_100km_Reelle'] = np.where(
        comparaison['Distance_Geoloc_Totale'] > 0,
        (comparaison['Volume_Carburant_Total'] / comparaison['Distance_Geoloc_Totale']) * 100,
        np.nan
    )

    # Identifier les anomalies significatives (écart > 10% et au moins 10km)
    seuil_ecart_pct = 10  # Pourcentage
    seuil_ecart_km = 10   # Kilomètres

    anomalies = comparaison[
        (abs(comparaison['Pourcentage_Ecart']) > seuil_ecart_pct) &
        (abs(comparaison['Ecart_Distance']) > seuil_ecart_km)
    ].copy()

    anomalies['Type_Anomalie'] = np.where(
        anomalies['Ecart_Distance'] > 0,
        'Sur-déclaration kilométrique',
        'Sous-déclaration kilométrique'
    )

    anomalies['Gravite'] = np.where(
        abs(anomalies['Pourcentage_Ecart']) > 25,
        'Élevée',
        'Moyenne'
    )

    # Arrondir les valeurs numériques
    cols_arrondi = ['Distance_Geoloc_Totale', 'Distance_Declaree_Totale', 'Ecart_Distance',
                    'Pourcentage_Ecart', 'Consommation_100km_Reelle', 'Distance_Moyenne_Trajet',
                    'Vitesse_Moyenne_Globale', 'Vitesse_Max_Observee']

    for col in cols_arrondi:
        if col in comparaison.columns:
            comparaison[col] = comparaison[col].round(1)
        if col in anomalies.columns:
            anomalies[col] = anomalies[col].round(1)

    # Trier les résultats
    comparaison_triee = comparaison.sort_values('Ecart_Distance', ascending=False)
    anomalies_triees = anomalies.sort_values('Pourcentage_Ecart', ascending=False)

    return comparaison_triee, anomalies_triees


def analyser_exces_vitesse(
    df_geoloc: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date,
    seuil_vitesse: int = 90  # Vitesse limite par défaut
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analyse les excès de vitesse à partir des données de géolocalisation.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.
        seuil_vitesse: Seuil de vitesse considéré comme un excès (km/h).

    Returns:
        Un tuple de DataFrames:
        - resume_exces: Résumé des excès de vitesse par véhicule.
        - detail_exces: Détail de tous les trajets avec excès de vitesse.
    """
    # Filtrer les données pour la période spécifiée
    mask_date = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_filtre = df_geoloc[mask_date].copy()
    
    # Garder uniquement les trajets pour l'analyse de vitesse
    df_filtre = df_filtre[df_filtre['Type'] == 'Trajet'].copy()


    if 'Vitesse moyenne' not in df_filtre.columns:
        st.warning("La colonne 'Vitesse moyenne' est manquante dans les données de géolocalisation pour l'analyse des excès.")
        return pd.DataFrame(), pd.DataFrame()

    # Identifier les excès de vitesse
    df_filtre['Exces_Vitesse'] = df_filtre['Vitesse moyenne'] > seuil_vitesse
    df_filtre['Depassement_km/h'] = np.where(
        df_filtre['Exces_Vitesse'],
        df_filtre['Vitesse moyenne'] - seuil_vitesse,
        0
    )

    # Détail de tous les trajets avec excès de vitesse
    detail_exces = df_filtre[df_filtre['Exces_Vitesse']].copy()
    if not detail_exces.empty:
        detail_exces['Niveau_Exces'] = pd.cut(
            detail_exces['Depassement_km/h'],
            bins=[0, 10, 20, 30, float('inf')],
            labels=['Léger (< 10 km/h)', 'Modéré (10-20 km/h)', 'Important (20-30 km/h)', 'Grave (> 30 km/h)'],
            right=False # Important pour que 0 ne soit pas inclus dans la première catégorie si un excès est juste au seuil
        )
    else:
        detail_exces['Niveau_Exces'] = pd.NA


    # Résumé par véhicule
    resume_exces = df_filtre.groupby('Véhicule').agg(
        Nb_Total_Trajets=('Exces_Vitesse', 'count'),
        Nb_Trajets_Exces=('Exces_Vitesse', 'sum'),
        Vitesse_Max_Observee=('Vitesse moyenne', 'max'), # Max des vitesses moyennes des segments
        Vitesse_Moyenne_Trajets=('Vitesse moyenne', 'mean'), # Moyenne des vitesses moyennes des segments
        Depassement_Moyen=('Depassement_km/h', lambda x: x[x > 0].mean() if any(x > 0) else 0)
    ).reset_index()

    # Calculer le pourcentage de trajets en excès
    resume_exces['Pourcentage_Trajets_Exces'] = (resume_exces['Nb_Trajets_Exces'] / resume_exces['Nb_Total_Trajets'] * 100).round(1)

    # Identifier le niveau de risque
    resume_exces['Niveau_Risque'] = pd.cut(
        resume_exces['Pourcentage_Trajets_Exces'],
        bins=[-1, 10, 25, 50, float('inf')], # -1 pour inclure 0%
        labels=['Faible', 'Modéré', 'Élevé', 'Très élevé']
    )

    # Trier par nombre d'excès décroissant
    resume_exces = resume_exces.sort_values('Nb_Trajets_Exces', ascending=False)
    detail_exces = detail_exces.sort_values(['Véhicule', 'Vitesse moyenne'], ascending=[True, False])

    return resume_exces, detail_exces


def analyser_utilisation_vehicules(
    df_geoloc: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analyse l'utilisation des véhicules (temps, distance, périodes d'activité).

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un tuple de DataFrames:
        - utilisation_par_vehicule: Résumé d'utilisation par véhicule.
        - utilisation_quotidienne: Utilisation quotidienne des véhicules.
    """
    # Filtrer les données pour la période spécifiée
    mask_date = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_filtre = df_geoloc[mask_date].copy()

    if df_filtre.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Garder uniquement les trajets pour cette analyse spécifique
    df_filtre_trajets = df_filtre[df_filtre['Type'] == 'Trajet'].copy()
    if df_filtre_trajets.empty:
         return pd.DataFrame(), pd.DataFrame()


    # Extraire l'heure des déplacements pour analyser les périodes d'activité
    if 'Heure_debut' in df_filtre_trajets.columns: # Heure_debut est déjà numérique
        # Classifier les périodes de la journée
        conditions = [
            (df_filtre_trajets['Heure_debut'] >= 6) & (df_filtre_trajets['Heure_debut'] < 9),
            (df_filtre_trajets['Heure_debut'] >= 9) & (df_filtre_trajets['Heure_debut'] < 12),
            (df_filtre_trajets['Heure_debut'] >= 12) & (df_filtre_trajets['Heure_debut'] < 14),
            (df_filtre_trajets['Heure_debut'] >= 14) & (df_filtre_trajets['Heure_debut'] < 17),
            (df_filtre_trajets['Heure_debut'] >= 17) & (df_filtre_trajets['Heure_debut'] < 20),
            (df_filtre_trajets['Heure_debut'] >= 20) | (df_filtre_trajets['Heure_debut'] < 6)
        ]

        periodes = [
            'Matin (6h-9h)', 'Matinée (9h-12h)', 'Midi (12h-14h)',
            'Après-midi (14h-17h)', 'Soir (17h-20h)', 'Nuit (20h-6h)'
        ]
        df_filtre_trajets['Periode_Jour'] = np.select(conditions, periodes, default='Non défini')
    else:
        df_filtre_trajets['Periode_Jour'] = 'Non défini'


    # Ajouter le jour de la semaine
    df_filtre_trajets['Jour_Semaine'] = df_filtre_trajets['Date'].dt.day_name()
    # Est_weekend est déjà calculé par charger_donnees_geolocalisation

    # Résumé d'utilisation par véhicule
    utilisation_par_vehicule = df_filtre_trajets.groupby('Véhicule').agg(
        Distance_Totale=('Distance', 'sum'),
        Nb_Trajets=('Distance', 'count'),
        Duree_Totale_Minutes=('Durée_minutes', 'sum'),
        Distance_Moyenne_Trajet=('Distance', 'mean'),
        Duree_Moyenne_Trajet=('Durée_minutes', 'mean'),
        Nb_Trajets_Weekend=('Est_weekend', lambda x: x.sum()),
        Vitesse_Moyenne=('Vitesse moyenne', 'mean')
    ).reset_index()

    # Calculer la durée totale en heures
    utilisation_par_vehicule['Duree_Totale_Heures'] = (utilisation_par_vehicule['Duree_Totale_Minutes'] / 60).round(1)

    # Calculer le pourcentage de trajets le weekend
    utilisation_par_vehicule['Pourcentage_Trajets_Weekend'] = (
        utilisation_par_vehicule['Nb_Trajets_Weekend'] / utilisation_par_vehicule['Nb_Trajets'] * 100
    ).round(1)

    # Utilisation quotidienne (pour graphiques d'évolution)
    utilisation_quotidienne = df_filtre_trajets.groupby(['Date', 'Véhicule']).agg(
        Distance_Jour=('Distance', 'sum'),
        Nb_Trajets_Jour=('Distance', 'count'),
        Duree_Jour_Minutes=('Durée_minutes', 'sum')
    ).reset_index()

    utilisation_quotidienne['Duree_Jour_Heures'] = (utilisation_quotidienne['Duree_Jour_Minutes'] / 60).round(1)

    # Analyser la répartition des trajets par période
    if 'Periode_Jour' in df_filtre_trajets.columns:
        repartition_periodes = df_filtre_trajets.groupby(['Véhicule', 'Periode_Jour']).size().unstack(fill_value=0)
        # Fusionner avec le résumé d'utilisation
        if not repartition_periodes.empty:
            utilisation_par_vehicule = utilisation_par_vehicule.merge(
                repartition_periodes,
                left_on='Véhicule',
                right_index=True,
                how='left'
            )

    # Arrondir les valeurs numériques
    cols_arrondi = ['Distance_Totale', 'Distance_Moyenne_Trajet', 'Duree_Moyenne_Trajet', 'Vitesse_Moyenne']
    for col in cols_arrondi:
        if col in utilisation_par_vehicule.columns:
            utilisation_par_vehicule[col] = utilisation_par_vehicule[col].round(1)

    return utilisation_par_vehicule.sort_values('Distance_Totale', ascending=False), utilisation_quotidienne


# ---------------------------------------------------------------------
# NOUVELLES FONCTIONS POUR L'ANALYSE AVANCÉE DE GÉOLOCALISATION
# ---------------------------------------------------------------------

def detecter_trajets_suspects(
    df_geoloc: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> pd.DataFrame:
    """
    Détecte les trajets suspects basés sur plusieurs critères:
    - Trajets hors heures de service
    - Trajets le weekend
    - Trajets avec vitesse anormalement basse (arrêts fréquents non enregistrés)

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un DataFrame des trajets suspects avec les détails et le score de risque.
    """
    # Paramètres (récupérés de session_state)
    heure_debut_service = st.session_state.get('ss_heure_debut_service', DEFAULT_HEURE_DEBUT_SERVICE)
    heure_fin_service = st.session_state.get('ss_heure_fin_service', DEFAULT_HEURE_FIN_SERVICE)
    # nb_arrets_suspect n'est pas utilisé directement ici mais dans la logique d'agrégation globale

    # Poids pour le score de risque
    poids_trajet_hors_heures = st.session_state.get('ss_poids_trajet_hors_heures', DEFAULT_POIDS_TRAJET_HORS_HEURES)
    poids_trajet_weekend = st.session_state.get('ss_poids_trajet_weekend', DEFAULT_POIDS_TRAJET_WEEKEND)
    poids_arrets_frequents = st.session_state.get('ss_poids_arrets_frequents', DEFAULT_POIDS_ARRETS_FREQUENTS) # pour vitesse lente

    # Filtrer les données pour la période spécifiée
    mask_date = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_filtre = df_geoloc[mask_date].copy()

    # S'assurer de ne traiter que les trajets
    df_filtre = df_filtre[df_filtre['Type'] == 'Trajet'].copy()


    if df_filtre.empty:
        return pd.DataFrame()

    # Identifier les types d'anomalies et calculer les scores
    # Heure_debut et Est_weekend sont déjà dans df_filtre grâce à charger_donnees_geolocalisation
    df_filtre['Est_Hors_Heures'] = (df_filtre['Heure_debut'] < heure_debut_service) | (df_filtre['Heure_debut'] >= heure_fin_service)
    df_filtre['Score_Hors_Heures'] = np.where(df_filtre['Est_Hors_Heures'], poids_trajet_hors_heures, 0)

    df_filtre['Score_Weekend'] = np.where(df_filtre['Est_weekend'], poids_trajet_weekend, 0)

    # Vitesse moyenne trop basse pour la distance peut indiquer des arrêts fréquents non documentés
    df_filtre['Est_Vitesse_Lente'] = (df_filtre['Vitesse moyenne'] < 25) & (df_filtre['Distance'] > 5) # Exemple de seuils
    df_filtre['Score_Vitesse_Lente'] = np.where(df_filtre['Est_Vitesse_Lente'], poids_arrets_frequents, 0)

    # Calculer le score total de suspicion
    df_filtre['Score_Suspicion_Total'] = df_filtre['Score_Hors_Heures'] + df_filtre['Score_Weekend'] + df_filtre['Score_Vitesse_Lente']

    # Filtrer les trajets avec un score de suspicion > 0
    trajets_suspects = df_filtre[df_filtre['Score_Suspicion_Total'] > 0].copy()

    if trajets_suspects.empty:
        return pd.DataFrame()

    # Créer un résumé des motifs de suspicion
    def creer_description_suspicion(row):
        motifs = []
        if row['Est_Hors_Heures']:
            motifs.append(f"Hors heures de service ({row['Heure_debut']}h)")
        if row['Est_weekend']:
            motifs.append("Weekend")
        if row['Est_Vitesse_Lente']:
            motifs.append(f"Vitesse anormalement basse ({row['Vitesse moyenne']:.1f} km/h)")
        return " + ".join(motifs)

    trajets_suspects['Description_Suspicion'] = trajets_suspects.apply(creer_description_suspicion, axis=1)

    # Déterminer le niveau de gravité
    trajets_suspects['Niveau_Suspicion'] = pd.cut(
        trajets_suspects['Score_Suspicion_Total'],
        bins=[0, 5, 10, float('inf')],
        labels=['Faible', 'Modéré', 'Élevé'],
        right=False
    )

    # Ajouter des informations utiles (Début est la colonne str originale, DateTime_Debut est le datetime)
    trajets_suspects['Date_Heure_Debut'] = trajets_suspects.apply(
        lambda row: f"{row['Date'].strftime('%d/%m/%Y')} {row['Début']}" if pd.notna(row['Début']) and isinstance(row['Début'], str) else row['DateTime_Debut'].strftime('%d/%m/%Y %H:%M:%S') if pd.notna(row['DateTime_Debut']) else "N/A",
        axis=1
    )


    # Sélectionner et ordonner les colonnes pertinentes
    cols_suspects = [
        'Véhicule', 'Date_Heure_Debut', 'Distance', 'Durée_minutes', 'Vitesse moyenne',
        'Description_Suspicion', 'Score_Suspicion_Total', 'Niveau_Suspicion'
    ]

    return trajets_suspects[cols_suspects].sort_values('Score_Suspicion_Total', ascending=False)


def analyser_correspondance_transactions_geoloc(
    df_geoloc: pd.DataFrame,
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analyse la correspondance entre les transactions de carburant et la géolocalisation.
    Vérifie notamment si le véhicule était présent à la station lors des transactions.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        df_transactions: DataFrame des transactions.
        df_vehicules: DataFrame des véhicules.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un tuple de DataFrames:
        - transactions_avec_presence: Transactions avec indication de présence du véhicule.
        - transactions_suspectes: Transactions pour lesquelles aucune présence du véhicule n'est détectée.
    """
    # Paramètres
    rayon_station_km = st.session_state.get('ss_rayon_station_km', DEFAULT_RAYON_STATION_KM)
    poids_transaction_sans_presence = st.session_state.get('ss_poids_transaction_sans_presence', DEFAULT_POIDS_TRANSACTION_SANS_PRESENCE)

    # Filtrer les données pour la période spécifiée
    mask_date_geoloc = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_geoloc_filtre = df_geoloc[mask_date_geoloc].copy()

    mask_date_trans = (df_transactions['Date'].dt.date >= date_debut) & (df_transactions['Date'].dt.date <= date_fin)
    df_trans_filtre = df_transactions[mask_date_trans].copy()

    # Fusionner transactions avec infos véhicules pour avoir l'immatriculation
    df_trans_avec_immat = df_trans_filtre.merge(
        df_vehicules[['N° Carte', 'Nouveau Immat']],
        left_on='Card num.',
        right_on='N° Carte',
        how='inner'
    )

    # Vérifier si des coordonnées GPS sont disponibles dans les données
    coordonnees_disponibles_geoloc = (
        'Latitude_depart' in df_geoloc_filtre.columns and
        'Longitude_depart' in df_geoloc_filtre.columns and
        not df_geoloc_filtre['Latitude_depart'].isna().all()
    )

    df_trans_avec_immat['Presence_Vehicule'] = False
    df_trans_avec_immat['Methode_Verification'] = "Non vérifié"
    df_trans_avec_immat['Score_Suspicion'] = 0
    df_trans_avec_immat['Distance_Station_Km'] = np.nan


    for idx, trans_row in df_trans_avec_immat.iterrows():
        immat = trans_row['Nouveau Immat']
        datetime_trans = trans_row['DateTime']

        if pd.isna(datetime_trans):
            df_trans_avec_immat.at[idx, 'Methode_Verification'] = "Date/heure transaction manquante"
            continue

        # Filtrer les trajets/arrêts du même véhicule le jour de la transaction
        date_trans = datetime_trans.date()
        geoloc_veh_jour = df_geoloc_filtre[
            (df_geoloc_filtre['Véhicule'] == immat) &
            (df_geoloc_filtre['Date'].dt.date == date_trans)
        ]

        presence_detectee_temporelle = False
        for _, geoloc_entry in geoloc_veh_jour.iterrows():
            if pd.notna(geoloc_entry['DateTime_Debut']) and pd.notna(geoloc_entry['DateTime_Fin']):
                # Fenêtre de tolérance pour la présence (ex: 30 min avant début à 30 min après fin)
                periode_debut = geoloc_entry['DateTime_Debut'] - timedelta(minutes=30)
                periode_fin = geoloc_entry['DateTime_Fin'] + timedelta(minutes=30)

                if periode_debut <= datetime_trans <= periode_fin:
                    presence_detectee_temporelle = True
                    df_trans_avec_immat.at[idx, 'Presence_Vehicule'] = True
                    df_trans_avec_immat.at[idx, 'Methode_Verification'] = f"Temporelle ({geoloc_entry['Type']})"

                    # Si les coordonnées sont disponibles et on a une localisation de station (hypothétique)
                    # Cette partie nécessiterait un fichier/DB de stations avec leurs coordonnées
                    # Pour l'instant, on se base sur la présence temporelle
                    # if coordonnees_disponibles_geoloc and 'Station_Lat' in trans_row and 'Station_Lon' in trans_row:
                    #     station_coords = (trans_row['Station_Lat'], trans_row['Station_Lon'])
                    #     if geoloc_entry['Type'] == 'Arrêt' and pd.notna(geoloc_entry['Latitude_depart']): # Supposons que Lat_depart est le lieu de l'arrêt
                    #         dist = haversine(station_coords, (geoloc_entry['Latitude_depart'], geoloc_entry['Longitude_depart']), unit=Unit.KILOMETERS)
                    #         df_trans_avec_immat.at[idx, 'Distance_Station_Km'] = dist
                    #         if dist <= rayon_station_km:
                    #             df_trans_avec_immat.at[idx, 'Methode_Verification'] = f"Géographique ({dist:.2f} km)"
                    #         # else: la présence temporelle reste
                    #     # Pourrait aussi vérifier la proximité pendant un trajet
                    break # Sortir de la boucle geoloc_entry si présence trouvée

        if not presence_detectee_temporelle:
             df_trans_avec_immat.at[idx, 'Methode_Verification'] = "Absence temporelle"


    # Calculer le score de suspicion pour les transactions sans présence détectée
    df_trans_avec_immat['Score_Suspicion'] = np.where(
        ~df_trans_avec_immat['Presence_Vehicule'],
        poids_transaction_sans_presence,
        0
    )

    # Identifier les transactions suspectes
    transactions_suspectes = df_trans_avec_immat[~df_trans_avec_immat['Presence_Vehicule']].copy()
    if not transactions_suspectes.empty:
        transactions_suspectes['Type_Anomalie'] = "Transaction sans présence détectée du véhicule"

        # Ajouter des critères supplémentaires pour les transactions suspectes
        transactions_suspectes['Jour_Semaine'] = transactions_suspectes['DateTime'].dt.dayofweek
        transactions_suspectes['Est_Weekend'] = transactions_suspectes['Jour_Semaine'] >= 5
        transactions_suspectes['Heure'] = transactions_suspectes['DateTime'].dt.hour
        transactions_suspectes['Est_Hors_Heures'] = (
            (transactions_suspectes['Heure'] < st.session_state.get('ss_heure_debut_service', DEFAULT_HEURE_DEBUT_SERVICE)) |
            (transactions_suspectes['Heure'] >= st.session_state.get('ss_heure_fin_service', DEFAULT_HEURE_FIN_SERVICE))
        )

        # Augmenter le score de suspicion pour les critères supplémentaires
        transactions_suspectes['Score_Suspicion'] = transactions_suspectes['Score_Suspicion'] + \
            np.where(transactions_suspectes['Est_Weekend'],
                     st.session_state.get('ss_poids_trajet_weekend', DEFAULT_POIDS_TRAJET_WEEKEND), 0) + \
            np.where(transactions_suspectes['Est_Hors_Heures'],
                     st.session_state.get('ss_poids_trajet_hors_heures', DEFAULT_POIDS_TRAJET_HORS_HEURES), 0)

        # Classifier le niveau de suspicion
        transactions_suspectes['Niveau_Suspicion'] = pd.cut(
            transactions_suspectes['Score_Suspicion'],
            bins=[0, 7, 12, float('inf')], # Ajuster les bins au besoin
            labels=['Faible', 'Modéré', 'Élevé'],
            right=False
        )

    return df_trans_avec_immat, transactions_suspectes


def detecter_detours_suspects(
    df_geoloc: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> pd.DataFrame:
    """
    Détecte les trajets avec des détours potentiellement suspects
    basés sur des critères comme distance/durée anormale, vitesse irrégulière, etc.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un DataFrame des trajets avec détours suspects.
    """
    # Paramètres
    seuil_detour_pct = st.session_state.get('ss_seuil_detour_pct', DEFAULT_SEUIL_DETOUR_PCT)
    poids_detour_suspect = st.session_state.get('ss_poids_detour_suspect', DEFAULT_POIDS_DETOUR_SUSPECT)

    # Filtrer les données pour la période spécifiée
    mask_date = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_filtre = df_geoloc[mask_date].copy()

    # Ne considérer que les trajets
    df_filtre = df_filtre[df_filtre['Type'] == 'Trajet'].copy()

    if df_filtre.empty or 'Durée_minutes' not in df_filtre.columns or df_filtre['Durée_minutes'].isna().all():
        return pd.DataFrame()

    # Calcul des ratios pour identifier les détours
    # Ratio Distance/Durée - si trop faible, peut indiquer un détour ou des arrêts non documentés
    df_filtre['Ratio_Dist_Duree'] = np.where(
        df_filtre['Durée_minutes'] > 0,
        df_filtre['Distance'] / (df_filtre['Durée_minutes'] / 60), # Distance / Heures = Vitesse
        np.nan
    )
    # Ce 'Ratio_Dist_Duree' est en fait la vitesse moyenne, qui est déjà calculée.
    # Utilisons directement 'Vitesse moyenne'
    df_filtre.rename(columns={'Vitesse moyenne': 'Ratio_Dist_Duree_Calc'}, inplace=True, errors='ignore') # Just in case


    # Moyennes par véhicule pour comparer
    moyennes_vehicules = df_filtre.groupby('Véhicule').agg(
        Ratio_Moyen=('Ratio_Dist_Duree_Calc', 'mean'), # Utiliser Vitesse moyenne calculée
        Dist_Moyenne=('Distance', 'mean'),
        Duree_Moyenne=('Durée_minutes', 'mean')
    ).reset_index()


    # Fusionner avec les moyennes par véhicule
    df_filtre = df_filtre.merge(moyennes_vehicules, on='Véhicule', how='left', suffixes=('', '_veh_moyen'))


    # Calcul des écarts par rapport aux moyennes du véhicule
    df_filtre['Ecart_Ratio_Pct'] = np.where(
        (df_filtre['Ratio_Moyen'].notna()) & (df_filtre['Ratio_Moyen'] > 0),
        ((df_filtre['Ratio_Dist_Duree_Calc'] / df_filtre['Ratio_Moyen']) - 1) * 100,
        np.nan
    )

    # Un écart négatif important indique un trajet inefficace (détour potentiel)
    # Un trajet est suspect si sa vitesse moyenne est significativement plus basse que la vitesse moyenne habituelle du véhicule
    # Et si la distance est non négligeable (ex: > 5km) pour éviter les petits trajets urbains lents.
    df_filtre['Est_Detour_Potentiel'] = (df_filtre['Ecart_Ratio_Pct'] < -seuil_detour_pct) & (df_filtre['Distance'] > 5)


    # Calculer le score de suspicion pour les détours potentiels
    df_filtre['Score_Detour'] = np.where(
        df_filtre['Est_Detour_Potentiel'],
        # Plus l'écart est négatif, plus le score est élevé
        poids_detour_suspect * (1 + abs(df_filtre['Ecart_Ratio_Pct']) / 100),
        0
    )

    # Filtrer les trajets suspects de détour
    detours_suspects = df_filtre[df_filtre['Est_Detour_Potentiel']].copy()

    if detours_suspects.empty:
        return pd.DataFrame()

    # Calculer la sévérité du détour
    detours_suspects['Severite_Detour'] = pd.cut(
        detours_suspects['Ecart_Ratio_Pct'].abs(),
        bins=[seuil_detour_pct, seuil_detour_pct + 25, seuil_detour_pct + 50, float('inf')], # Ajuster bins
        labels=['Léger', 'Modéré', 'Important'],
        right=False
    )

    # Ajouter une description du détour
    detours_suspects['Description_Detour'] = detours_suspects.apply(
        lambda row: f"Trajet {row['Severite_Detour']} inefficace ({abs(row['Ecart_Ratio_Pct']):.1f}% sous la moyenne du véhicule). "
                   f"Distance: {row['Distance']:.1f}km, Durée: {row['Durée_minutes']:.0f}min, "
                   f"Vitesse moy: {row['Ratio_Dist_Duree_Calc']:.1f}km/h (vs. {row['Ratio_Moyen']:.1f}km/h pour ce véhicule)",
        axis=1
    )

    # Sélectionner et ordonner les colonnes pertinentes
    cols_detours = [
        'Véhicule', 'Date', 'Début', 'Fin', 'Distance', 'Durée_minutes',
        'Vitesse moyenne', 'Ecart_Ratio_Pct', 'Severite_Detour',
        'Description_Detour', 'Score_Detour'
    ]
    # S'assurer que les colonnes existent avant de les sélectionner
    cols_detours_existantes = [col for col in cols_detours if col in detours_suspects.columns]

    return detours_suspects[cols_detours_existantes].sort_values('Ecart_Ratio_Pct')


def analyser_efficacite_carburant(
    df_geoloc: pd.DataFrame,
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> pd.DataFrame:
    """
    Analyse l'efficacité d'utilisation du carburant en combinant données géoloc et transactions.
    Identifie les trajets avec une consommation anormale par rapport aux moyennes.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        df_transactions: DataFrame des transactions.
        df_vehicules: DataFrame des véhicules.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un DataFrame avec les analyses d'efficacité par véhicule.
    """
    # Filtrer les données pour la période spécifiée
    mask_date_geoloc = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_geoloc_filtre = df_geoloc[mask_date_geoloc].copy()
    df_geoloc_filtre_trajets = df_geoloc_filtre[df_geoloc_filtre['Type'] == 'Trajet']


    mask_date_trans = (df_transactions['Date'].dt.date >= date_debut) & (df_transactions['Date'].dt.date <= date_fin)
    df_trans_filtre = df_transactions[mask_date_trans].copy()

    # Fusionner transactions avec infos véhicules
    df_trans_avec_immat = df_trans_filtre.merge(
        df_vehicules[['N° Carte', 'Nouveau Immat', 'Catégorie']],
        left_on='Card num.',
        right_on='N° Carte',
        how='inner'
    )

    # Agréger les distances par immatriculation (géolocalisation)
    distance_geoloc = df_geoloc_filtre_trajets.groupby('Véhicule').agg(
        Distance_Geoloc_Totale=('Distance', 'sum'),
        Nb_Trajets=('Distance', 'count'),
        Duree_Totale_Minutes=('Durée_minutes', 'sum')
    ).reset_index()

    # Agréger la consommation par immatriculation (transactions)
    conso_vehicule = df_trans_avec_immat.groupby('Nouveau Immat').agg(
        Volume_Total=('Quantity', 'sum'),
        Cout_Total=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count')
    ).reset_index()

    # Fusionner les données
    efficacite = distance_geoloc.merge(
        conso_vehicule,
        left_on='Véhicule',
        right_on='Nouveau Immat',
        how='outer' # Outer join pour garder les véhicules même s'ils manquent dans un des DFs
    )
    efficacite['Véhicule'] = efficacite['Véhicule'].fillna(efficacite['Nouveau Immat'])


    # Calculer les métriques d'efficacité
    efficacite['Consommation_100km'] = np.where(
        efficacite['Distance_Geoloc_Totale'] > 0,
        (efficacite['Volume_Total'] / efficacite['Distance_Geoloc_Totale']) * 100,
        np.nan
    )

    efficacite['Cout_par_km'] = np.where(
        efficacite['Distance_Geoloc_Totale'] > 0,
        efficacite['Cout_Total'] / efficacite['Distance_Geoloc_Totale'],
        np.nan
    )

    efficacite['Cout_par_heure'] = np.where(
        (efficacite['Duree_Totale_Minutes'].notna()) & (efficacite['Duree_Totale_Minutes'] > 0),
        (efficacite['Cout_Total'] / efficacite['Duree_Totale_Minutes']) * 60,
        np.nan
    )


    # Conserver l'immatriculation comme identifiant unique
    efficacite.drop('Nouveau Immat', axis=1, inplace=True, errors='ignore')

    # Ajouter la catégorie du véhicule
    mapping_categorie = df_vehicules.set_index('Nouveau Immat')['Catégorie'].to_dict()
    efficacite['Catégorie'] = efficacite['Véhicule'].map(mapping_categorie)

    # Calculer les moyennes par catégorie pour comparaison
    moyennes_categorie = efficacite.groupby('Catégorie').agg(
        Conso_Moyenne_Cat=('Consommation_100km', 'mean'),
        Cout_km_Moyen_Cat=('Cout_par_km', 'mean')
    ).reset_index() # Pour que Catégorie redevienne une colonne


    # Fusionner avec les moyennes par catégorie
    efficacite = efficacite.merge(moyennes_categorie, on='Catégorie', how='left', suffixes=('', '_cat_ref'))


    # Calculer les écarts par rapport aux moyennes de la catégorie
    efficacite['Ecart_Conso_Pct'] = np.where(
        (efficacite['Conso_Moyenne_Cat'].notna()) & (efficacite['Conso_Moyenne_Cat'] > 0),
        ((efficacite['Consommation_100km'] / efficacite['Conso_Moyenne_Cat']) - 1) * 100,
        np.nan
    )

    efficacite['Ecart_Cout_km_Pct'] = np.where(
        (efficacite['Cout_km_Moyen_Cat'].notna()) & (efficacite['Cout_km_Moyen_Cat'] > 0),
        ((efficacite['Cout_par_km'] / efficacite['Cout_km_Moyen_Cat']) - 1) * 100,
        np.nan
    )


    # Calculer l'efficacité globale (score combiné)
    efficacite['Score_Efficacite'] = np.where(
        pd.notna(efficacite['Ecart_Conso_Pct']) & pd.notna(efficacite['Ecart_Cout_km_Pct']),
        100 - (efficacite['Ecart_Conso_Pct'] + efficacite['Ecart_Cout_km_Pct']) / 2, # Ecart positif = moins efficace
        np.nan
    )
    efficacite['Score_Efficacite'] = efficacite['Score_Efficacite'].fillna(50) # Note neutre si pas assez de données


    # Classifier l'efficacité
    efficacite['Niveau_Efficacite'] = pd.cut(
        efficacite['Score_Efficacite'],
        bins=[-float('inf'), 85, 95, 105, 115, float('inf')],
        labels=['Très faible', 'Faible', 'Normale', 'Bonne', 'Excellente'],
        right=False
    )

    # Arrondir les valeurs numériques pour l'affichage
    cols_arrondi = [
        'Distance_Geoloc_Totale', 'Duree_Totale_Minutes', 'Volume_Total',
        'Consommation_100km', 'Cout_par_km', 'Cout_par_heure',
        'Ecart_Conso_Pct', 'Ecart_Cout_km_Pct', 'Score_Efficacite',
        'Conso_Moyenne_Cat', 'Cout_km_Moyen_Cat'
    ]


    for col in cols_arrondi:
        if col in efficacite.columns:
            efficacite[col] = efficacite[col].round(1)

    return efficacite.sort_values('Score_Efficacite', ascending=False)


def generer_resume_anomalies_geolocalisation(
    df_geoloc: pd.DataFrame,
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> pd.DataFrame:
    """
    Génère un tableau de bord résumant toutes les anomalies détectées
    à partir des données de géolocalisation et leur corrélation avec les transactions.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        df_transactions: DataFrame des transactions.
        df_vehicules: DataFrame des véhicules.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un DataFrame consolidant toutes les anomalies détectées par véhicule avec le score de risque.
    """
    # Collecter les anomalies de différentes analyses
    # Utiliser un seuil de vitesse par défaut pertinent pour l'agrégation, ex: 90km/h
    # Le seuil slider est pour l'exploration interactive dans l'onglet vitesse.
    seuil_vitesse_agregation = 90

    with st.spinner("Analyse des excès de vitesse (agrégation)..."):
        resume_exces, _ = analyser_exces_vitesse(df_geoloc, date_debut, date_fin, seuil_vitesse_agregation)

    with st.spinner("Détection des trajets suspects (agrégation)..."):
        trajets_suspects = detecter_trajets_suspects(df_geoloc, date_debut, date_fin)

    with st.spinner("Analyse des correspondances transactions/géolocalisation (agrégation)..."):
        _, transactions_suspectes = analyser_correspondance_transactions_geoloc(
            df_geoloc, df_transactions, df_vehicules, date_debut, date_fin
        )

    with st.spinner("Détection des détours suspects (agrégation)..."):
        detours_suspects = detecter_detours_suspects(df_geoloc, date_debut, date_fin)

    with st.spinner("Analyse comparative distances géoloc/transactions (agrégation)..."):
        comparaison, anomalies_distance = analyser_geolocalisation_vs_transactions(
            df_geoloc, df_transactions, df_vehicules, date_debut, date_fin
        )

    # Initialiser le DataFrame résumé
    vehicules_geoloc = set(df_geoloc['Véhicule'].unique())

    # Créer un DataFrame avec tous les véhicules géolocalisés
    resume = pd.DataFrame({'Véhicule': list(vehicules_geoloc)})

    # Ajouter les scores de chaque type d'anomalie

    # 1. Excès de vitesse
    if not resume_exces.empty:
        resume_exces_simple = resume_exces[['Véhicule', 'Nb_Trajets_Exces', 'Pourcentage_Trajets_Exces']]
        resume = resume.merge(resume_exces_simple, on='Véhicule', how='left')
        resume['Nb_Trajets_Exces'] = resume['Nb_Trajets_Exces'].fillna(0)
        resume['Pourcentage_Trajets_Exces'] = resume['Pourcentage_Trajets_Exces'].fillna(0)
        resume['Score_Exces_Vitesse'] = resume['Pourcentage_Trajets_Exces'] * st.session_state.get(
            'ss_poids_vitesse_excessive', DEFAULT_POIDS_VITESSE_EXCESSIVE
        ) / 10 # Normaliser le pourcentage
    else:
        resume['Nb_Trajets_Exces'] = 0
        resume['Pourcentage_Trajets_Exces'] = 0
        resume['Score_Exces_Vitesse'] = 0

    # 2. Trajets suspects (hors heures, weekend, vitesse lente)
    if not trajets_suspects.empty:
        trajets_suspects_agg = trajets_suspects.groupby('Véhicule').agg(
            Nb_Trajets_Suspects=('Score_Suspicion_Total', 'count'),
            Score_Trajets_Suspects=('Score_Suspicion_Total', 'sum') # Somme des scores individuels de chaque trajet
        ).reset_index()
        resume = resume.merge(trajets_suspects_agg, on='Véhicule', how='left')
        resume['Nb_Trajets_Suspects'] = resume['Nb_Trajets_Suspects'].fillna(0)
        resume['Score_Trajets_Suspects'] = resume['Score_Trajets_Suspects'].fillna(0)
    else:
        resume['Nb_Trajets_Suspects'] = 0
        resume['Score_Trajets_Suspects'] = 0

    # 3. Transactions suspectes (sans présence)
    if not transactions_suspectes.empty and 'Nouveau Immat' in transactions_suspectes.columns:
        transactions_suspectes_agg = transactions_suspectes.groupby('Nouveau Immat').agg(
            Nb_Transactions_Suspectes=('Score_Suspicion', 'count'),
            Score_Transactions_Suspectes=('Score_Suspicion', 'sum')
        ).reset_index()
        transactions_suspectes_agg.rename(columns={'Nouveau Immat': 'Véhicule'}, inplace=True)
        resume = resume.merge(transactions_suspectes_agg, on='Véhicule', how='left')
        resume['Nb_Transactions_Suspectes'] = resume['Nb_Transactions_Suspectes'].fillna(0)
        resume['Score_Transactions_Suspectes'] = resume['Score_Transactions_Suspectes'].fillna(0)
    else:
        resume['Nb_Transactions_Suspectes'] = 0
        resume['Score_Transactions_Suspectes'] = 0

    # 4. Détours suspects
    if not detours_suspects.empty:
        detours_suspects_agg = detours_suspects.groupby('Véhicule').agg(
            Nb_Detours_Suspects=('Score_Detour', 'count'),
            Score_Detours=('Score_Detour', 'sum')
        ).reset_index()
        resume = resume.merge(detours_suspects_agg, on='Véhicule', how='left')
        resume['Nb_Detours_Suspects'] = resume['Nb_Detours_Suspects'].fillna(0)
        resume['Score_Detours'] = resume['Score_Detours'].fillna(0)
    else:
        resume['Nb_Detours_Suspects'] = 0
        resume['Score_Detours'] = 0

    # 5. Anomalies de distance (comparaison géoloc vs transactions)
    if not anomalies_distance.empty and 'Immatriculation' in anomalies_distance.columns:
        anomalies_distance_agg = anomalies_distance.groupby('Immatriculation').agg(
            Ecart_Distance_Km=('Ecart_Distance', 'sum'), # Somme des écarts
            Pourcentage_Ecart_Moyen=('Pourcentage_Ecart', 'mean') # Écart moyen en %
        ).reset_index()
        anomalies_distance_agg.rename(columns={'Immatriculation': 'Véhicule', 'Pourcentage_Ecart_Moyen': 'Pourcentage_Ecart'}, inplace=True)
        resume = resume.merge(anomalies_distance_agg, on='Véhicule', how='left')
        resume['Ecart_Distance_Km'] = resume['Ecart_Distance_Km'].fillna(0)
        resume['Pourcentage_Ecart'] = resume['Pourcentage_Ecart'].fillna(0)

        # Calculer un score basé sur l'écart de distance (positif ou négatif)
        # Le poids pourrait être un paramètre
        resume['Score_Ecart_Distance'] = resume['Pourcentage_Ecart'].abs() * (st.session_state.get('ss_poids_detour_suspect', DEFAULT_POIDS_DETOUR_SUSPECT) / 20) # Exemple de pondération
    else:
        resume['Ecart_Distance_Km'] = 0
        resume['Pourcentage_Ecart'] = 0
        resume['Score_Ecart_Distance'] = 0

    # Calculer le score total de risque
    resume['Score_Risque_Total'] = (
        resume['Score_Exces_Vitesse'] +
        resume['Score_Trajets_Suspects'] +
        resume['Score_Transactions_Suspectes'] +
        resume['Score_Detours'] +
        resume['Score_Ecart_Distance']
    ).fillna(0)


    # Classifier le niveau de risque
    resume['Niveau_Risque'] = pd.cut(
        resume['Score_Risque_Total'],
        bins=[-1, 10, 20, 40, float('inf')], # -1 pour inclure 0
        labels=['Faible', 'Modéré', 'Élevé', 'Critique']
    )

    # Générer un résumé textuel des anomalies
    def generer_resume_textuel(row):
        anomalies_txt_list = [] # Renommé pour éviter conflit

        if row['Nb_Trajets_Exces'] > 0:
            anomalies_txt_list.append(f"Excès vitesse: {int(row['Nb_Trajets_Exces'])} ({row['Pourcentage_Trajets_Exces']:.0f}%)")

        if row['Nb_Trajets_Suspects'] > 0:
            anomalies_txt_list.append(f"Trajets susp: {int(row['Nb_Trajets_Suspects'])}")

        if row['Nb_Transactions_Suspectes'] > 0:
            anomalies_txt_list.append(f"Transac. susp: {int(row['Nb_Transactions_Suspectes'])}")

        if row['Nb_Detours_Suspects'] > 0:
            anomalies_txt_list.append(f"Détours susp: {int(row['Nb_Detours_Suspects'])}")

        if abs(row['Pourcentage_Ecart']) > 5: # Seuil pour afficher l'écart de distance
            type_ecart = "Sur-décl." if row['Ecart_Distance_Km'] > 0 else "Sous-décl."
            anomalies_txt_list.append(f"{type_ecart} km: {row['Pourcentage_Ecart']:.0f}%")

        if not anomalies_txt_list:
            return "Aucune anomalie géoloc majeure"

        return " | ".join(anomalies_txt_list)

    resume['Résumé_Anomalies'] = resume.apply(generer_resume_textuel, axis=1)

    # Arrondir les valeurs numériques
    cols_arrondi_resume = ['Pourcentage_Trajets_Exces', 'Score_Exces_Vitesse', 'Score_Risque_Total',
                           'Ecart_Distance_Km', 'Pourcentage_Ecart', 'Score_Ecart_Distance',
                           'Score_Trajets_Suspects', 'Score_Transactions_Suspectes', 'Score_Detours']


    for col in cols_arrondi_resume:
        if col in resume.columns:
            resume[col] = resume[col].round(1)

    # Trier par score de risque total décroissant
    return resume.sort_values('Score_Risque_Total', ascending=False)


# Fonction pour visualiser les trajets sur une carte
def visualiser_trajets_sur_carte(
    df_geoloc: pd.DataFrame,
    vehicule_selectionne: str = None,
    date_debut: Optional[datetime.date] = None,
    date_fin: Optional[datetime.date] = None,
    highlight_anomalies: bool = False
) -> None:
    """
    Affiche les trajets sur une carte interactive.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        vehicule_selectionne: Immatriculation du véhicule à afficher (optionnel).
        date_debut: Date de début pour filtrer les trajets (optionnel).
        date_fin: Date de fin pour filtrer les trajets (optionnel).
        highlight_anomalies: Indique si les anomalies doivent être mises en évidence.
    """
    # Vérifier que les coordonnées GPS sont disponibles
    if 'Latitude_depart' not in df_geoloc.columns or 'Longitude_depart' not in df_geoloc.columns \
       or 'Latitude_arrivee' not in df_geoloc.columns or 'Longitude_arrivee' not in df_geoloc.columns:
        st.warning("Les coordonnées GPS (départ ET arrivée) ne sont pas disponibles dans les données. Impossible d'afficher la carte.")
        return

    # Filtrer les données
    df_filtered = df_geoloc.copy()
    df_filtered = df_filtered[df_filtered['Type'] == 'Trajet'] # Uniquement les trajets


    if date_debut is not None and date_fin is not None:
        mask_date = (df_filtered['Date'].dt.date >= date_debut) & (df_filtered['Date'].dt.date <= date_fin)
        df_filtered = df_filtered[mask_date]

    if vehicule_selectionne is not None and vehicule_selectionne != "Tous les véhicules":
        df_filtered = df_filtered[df_filtered['Véhicule'] == vehicule_selectionne]

    # S'assurer qu'il y a des données à afficher avec coordonnées valides
    df_filtered = df_filtered.dropna(subset=['Latitude_depart', 'Longitude_depart', 'Latitude_arrivee', 'Longitude_arrivee'])

    if df_filtered.empty:
        st.warning("Aucune donnée géolocalisée (trajets avec coordonnées valides) disponible pour les critères sélectionnés.")
        return

    # Créer une carte centrée sur le point moyen des coordonnées
    mean_lat = df_filtered['Latitude_depart'].mean()
    mean_lon = df_filtered['Longitude_depart'].mean()

    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=10) # Zoom un peu plus large

    # Pour highlight_anomalies, il faudrait que df_filtered contienne déjà les colonnes d'anomalies
    # On peut pré-calculer certaines si elles ne sont pas là (ex: Exces_Vitesse)
    if highlight_anomalies and 'Exces_Vitesse' not in df_filtered.columns:
        # Recalculer rapidement pour la visualisation
        seuil_vitesse_visu = st.session_state.get('ss_vitesse_excessive_seuil', 90) # Utiliser un seuil, peut-être paramétrable
        df_filtered['Exces_Vitesse'] = df_filtered['Vitesse moyenne'] > seuil_vitesse_visu


    # Ajouter les trajets à la carte
    for idx, row in df_filtered.iterrows():
        # Déterminer la couleur en fonction de la vitesse ou anomalies
        color = 'blue'  # Couleur par défaut
        popup_text = f"Véhicule: {row['Véhicule']}<br>" \
                     f"Date: {row['Date'].strftime('%d/%m/%Y')}<br>" \
                     f"Début: {row['Début']}<br>" \
                     f"Fin: {row['Fin']}<br>" \
                     f"Distance: {row.get('Distance', 'N/A'):.1f} km<br>" \
                     f"Vitesse moy.: {row.get('Vitesse moyenne', 'N/A'):.1f} km/h"


        if highlight_anomalies:
            anomalie_type = []
            if row.get('Est_weekend', False):
                color = 'orange'
                anomalie_type.append("Weekend")
            if row.get('Exces_Vitesse', False): # Exces_Vitesse doit être précalculé
                color = 'red'
                anomalie_type.append("Excès Vitesse")
            # Si d'autres colonnes d'anomalies (ex: Est_Detour_Potentiel) sont ajoutées à df_geoloc
            # if row.get('Est_Detour_Potentiel', False):
            #     color = 'purple' # Example
            #     anomalie_type.append("Détour")
            if anomalie_type:
                popup_text += f"<br><b>Anomalie: {', '.join(anomalie_type)}</b>"

        else: # Couleur basée sur la vitesse moyenne
            v_moy = row.get('Vitesse moyenne', 0)
            if pd.notna(v_moy):
                if v_moy < 30: color = 'green'
                elif v_moy < 60: color = 'blue'
                elif v_moy < 90: color = 'orange'
                else: color = 'red'


        # Créer le tracé du trajet
        folium.PolyLine(
            locations=[
                [row['Latitude_depart'], row['Longitude_depart']],
                [row['Latitude_arrivee'], row['Longitude_arrivee']]
            ],
            color=color,
            weight=3, # Un peu plus épais
            opacity=0.8,
            tooltip=popup_text # Utiliser tooltip pour hover
        ).add_to(m)

        # Ajouter les marqueurs de début et fin
        folium.Marker(
            [row['Latitude_depart'], row['Longitude_depart']],
            popup=f"Départ: {row['Véhicule']} - {row['Date'].strftime('%d/%m/%Y')} {row['Début']}",
            icon=folium.Icon(color='green', icon='play', prefix='fa')
        ).add_to(m)

        folium.Marker(
            [row['Latitude_arrivee'], row['Longitude_arrivee']],
            popup=f"Arrivée: {row['Véhicule']} - {row['Date'].strftime('%d/%m/%Y')} {row['Fin']}",
            icon=folium.Icon(color='darkred', icon='stop', prefix='fa') # 'darkred' pour fin
        ).add_to(m)

    # Afficher la carte
    folium_static(m, width=None, height=600) # Ajuster la hauteur


# Fonction pour afficher la page d'analyse de géolocalisation
def afficher_page_analyse_geolocalisation(
    df_geoloc: pd.DataFrame,
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
):
    """Affiche la page d'analyse des données de géolocalisation."""
    st.header(f"📍 Analyse des Données de Géolocalisation ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_geoloc is None or df_geoloc.empty:
        st.warning("Aucune donnée de géolocalisation à analyser. Veuillez charger un fichier de géolocalisation.")
        return

    # Filtrer les données pour la période sélectionnée
    mask_date = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_geoloc_filtered_orig = df_geoloc[mask_date] # Garder original pour analyses mixtes Type

    if df_geoloc_filtered_orig.empty:
        st.warning(f"Aucune donnée de géolocalisation pour la période du {date_debut} au {date_fin}.")
        return

    # Filtrer spécifiquement les TRAJETS pour de nombreuses analyses
    df_geoloc_filtered_trajets = df_geoloc_filtered_orig[df_geoloc_filtered_orig['Type'] == 'Trajet'].copy()


    # Onglets pour différentes analyses
    tab_synthese, tab_comparaison, tab_vitesse, tab_utilisation, tab_trajets_suspects, tab_carte, tab_integration = st.tabs([
        "📊 Synthèse", "🔍 Comparaison Carburant", "🚨 Excès de Vitesse", "⚙️ Utilisation",
        "⚠️ Trajets Suspects", "🗺️ Carte", "🔄 Intégration"
    ])

    with tab_synthese:
        st.subheader("Synthèse des Données de Géolocalisation (Trajets)")
        if df_geoloc_filtered_trajets.empty:
            st.info("Aucun trajet enregistré dans la période sélectionnée.")
        else:
            nb_vehicules = df_geoloc_filtered_trajets['Véhicule'].nunique()
            nb_trajets = len(df_geoloc_filtered_trajets)
            distance_totale = df_geoloc_filtered_trajets['Distance'].sum()
            duree_totale_min = df_geoloc_filtered_trajets['Durée_minutes'].sum(skipna=True)
            duree_totale_heures = duree_totale_min / 60 if duree_totale_min > 0 else 0
            vitesse_moy = df_geoloc_filtered_trajets['Vitesse moyenne'].mean(skipna=True)
            # Pour Vitesse Max, on prend le max des vitesses moyennes des segments (car 'Vitesse max' originale n'est plus là)
            vitesse_max_segments = df_geoloc_filtered_trajets['Vitesse moyenne'].max(skipna=True)


            # Afficher les KPIs
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Véhicules avec Trajets", f"{nb_vehicules}")
            col2.metric("Trajets Enregistrés", f"{nb_trajets:,}")
            col3.metric("Distance Totale (Trajets)", f"{distance_totale:,.1f} km")
            col4.metric("Durée Totale (Trajets)", f"{duree_totale_heures:,.1f} h")

            col5, col6 = st.columns(2)
            col5.metric("Vitesse Moyenne (Trajets)", f"{vitesse_moy:.1f} km/h" if pd.notna(vitesse_moy) else "N/A")
            col6.metric("Vitesse Max des Segments", f"{vitesse_max_segments:.1f} km/h" if pd.notna(vitesse_max_segments) else "N/A")


            # Graphique de répartition des distances par véhicule
            distance_par_vehicule = df_geoloc_filtered_trajets.groupby('Véhicule')['Distance'].sum().reset_index()
            fig_distance = px.bar(
                distance_par_vehicule.sort_values('Distance', ascending=False),
                x='Véhicule',
                y='Distance',
                title="Distance Totale Parcourue par Véhicule (Trajets)",
                labels={'Distance': 'Distance (km)'}
            )
            st.plotly_chart(fig_distance, use_container_width=True)

            # Graphique d'évolution quotidienne des distances
            distance_quotidienne = df_geoloc_filtered_trajets.groupby(pd.Grouper(key='Date', freq='D'))['Distance'].sum().reset_index()
            fig_evol = px.line(
                distance_quotidienne,
                x='Date',
                y='Distance',
                title="Évolution Quotidienne de la Distance Parcourue (Trajets)",
                labels={'Distance': 'Distance (km)'},
                markers=True
            )
            st.plotly_chart(fig_evol, use_container_width=True)

            # Tableau récapitulatif
            st.subheader("Données par Véhicule (Trajets)")
            recap_vehicule = df_geoloc_filtered_trajets.groupby('Véhicule').agg(
                Nb_Trajets=('Distance', 'count'),
                Distance_Totale=('Distance', 'sum'),
                Distance_Moyenne=('Distance', 'mean'),
                Vitesse_Moyenne_Segments=('Vitesse moyenne', 'mean'),
                Vitesse_Max_Segment=('Vitesse moyenne', 'max'),
                Duree_Totale_Minutes=('Durée_minutes', 'sum')
            ).reset_index()

            recap_vehicule['Duree_Totale_Heures'] = (recap_vehicule['Duree_Totale_Minutes'] / 60).round(1)
            recap_vehicule = recap_vehicule.sort_values('Distance_Totale', ascending=False)

            # Arrondir les valeurs
            cols_numeriques = ['Distance_Totale', 'Distance_Moyenne', 'Vitesse_Moyenne_Segments', 'Vitesse_Max_Segment']
            for col in cols_numeriques:
                recap_vehicule[col] = recap_vehicule[col].round(1)

            afficher_dataframe_avec_export(recap_vehicule, "Récapitulatif par Véhicule (Trajets)", key="geoloc_recap_vehicule_trajets")

    with tab_comparaison:
        st.subheader("Comparaison Kilométrage Géolocalisation vs. Transactions Carburant")

        # Analyse comparative
        with st.spinner("Analyse comparative en cours..."):
            # Utiliser df_geoloc_filtered_orig car analyser_geolocalisation_vs_transactions filtre par Type='Trajet' en interne
            comparaison, anomalies = analyser_geolocalisation_vs_transactions(
                df_geoloc_filtered_orig, df_transactions, df_vehicules, date_debut, date_fin
            )

        if comparaison.empty:
            st.info("Données insuffisantes pour effectuer une comparaison.")
        else:
            # Affichage des résultats
            nb_anomalies = len(anomalies)
            nb_sur_decla = len(anomalies[anomalies['Type_Anomalie'] == 'Sur-déclaration kilométrique']) if not anomalies.empty else 0
            nb_sous_decla = len(anomalies[anomalies['Type_Anomalie'] == 'Sous-déclaration kilométrique']) if not anomalies.empty else 0

            st.warning(f"⚠️ Détection de {nb_anomalies} anomalies significatives de kilométrage ({nb_sur_decla} sur-déclarations, {nb_sous_decla} sous-déclarations)")

            # Graphique des écarts
            fig_ecart = px.bar(
                comparaison.sort_values('Ecart_Distance', ascending=False).head(15),
                x='Immatriculation',
                y='Ecart_Distance',
                title="Top 15 des Écarts de Kilométrage (Déclaré - Géolocalisé)",
                labels={'Ecart_Distance': 'Écart (km)'},
                color='Ecart_Distance',
                color_continuous_scale=px.colors.diverging.RdBu,
                color_continuous_midpoint=0
            )
            st.plotly_chart(fig_ecart, use_container_width=True)

            # Tableau des anomalies
            if not anomalies.empty:
                st.subheader("Anomalies Détectées")
                cols_anomalies = [
                    'Immatriculation', 'Type_Anomalie', 'Gravite',
                    'Distance_Geoloc_Totale', 'Distance_Declaree_Totale',
                    'Ecart_Distance', 'Pourcentage_Ecart'
                ]
                afficher_dataframe_avec_export(
                    anomalies[cols_anomalies],
                    "Anomalies de Kilométrage",
                    key="geoloc_anomalies_km"
                )

            # Tableau complet
            st.subheader("Comparaison Complète par Véhicule")
            cols_comparaison = [
                'Immatriculation', 'Distance_Geoloc_Totale', 'Distance_Declaree_Totale',
                'Ecart_Distance', 'Pourcentage_Ecart', 'Consommation_100km_Reelle',
                'Volume_Carburant_Total', 'Nb_Trajets', 'Nb_Transactions'
            ]
            afficher_dataframe_avec_export(
                comparaison[[c for c in cols_comparaison if c in comparaison.columns]],
                "Comparaison Kilométrage",
                key="geoloc_comparaison_km"
            )

            # Graphique de comparaison consommation
            if 'Consommation_100km_Reelle' in comparaison.columns:
                comparaison_conso = comparaison.dropna(subset=['Consommation_100km_Reelle']).sort_values('Consommation_100km_Reelle', ascending=False)
                if not comparaison_conso.empty:
                    fig_conso = px.bar(
                        comparaison_conso,
                        x='Immatriculation',
                        y='Consommation_100km_Reelle',
                        title="Consommation Réelle (L/100km) basée sur la distance géolocalisée",
                        labels={'Consommation_100km_Reelle': 'Consommation (L/100km)'}
                    )
                    st.plotly_chart(fig_conso, use_container_width=True)

            # Recommandations basées sur les anomalies
            if not anomalies.empty:
                st.subheader("Recommandations")
                st.markdown("""
                Basé sur l'analyse des écarts entre kilométrage déclaré et géolocalisé:

                1. **Pour les sur-déclarations importantes**: Vérifier si les véhicules concernés ont effectué des trajets non enregistrés par le système de géolocalisation, ou s'il y a des déclarations potentiellement incorrectes.

                2. **Pour les sous-déclarations importantes**: Vérifier si les transactions de carburant sont correctement associées au véhicule, ou si le kilométrage n'est pas systématiquement saisi lors des prises de carburant.

                3. **Pour les écarts persistants**: Envisager un audit spécifique des véhicules présentant des anomalies répétées.
                """)

    with tab_vitesse:
        st.subheader("Analyse des Excès de Vitesse")

        # Paramètre de seuil de vitesse
        seuil_vitesse = st.slider(
            "Seuil de vitesse considéré comme excès (km/h)",
            min_value=50,
            max_value=130,
            value=90, # Default
            step=5,
            key="slider_seuil_vitesse_geoloc"
        )

        # Analyse des excès de vitesse
        with st.spinner("Analyse des excès de vitesse en cours..."):
             # Utiliser df_geoloc_filtered_orig car analyser_exces_vitesse filtre par Type='Trajet' en interne
            resume_exces, detail_exces = analyser_exces_vitesse(
                df_geoloc_filtered_orig, date_debut, date_fin, seuil_vitesse
            )

        if resume_exces.empty:
            st.info("Données insuffisantes pour analyser les excès de vitesse.")
        else:
            nb_total_exces = resume_exces['Nb_Trajets_Exces'].sum()
            nb_vehicules_exces = len(resume_exces[resume_exces['Nb_Trajets_Exces'] > 0])

            col_v1, col_v2, col_v3 = st.columns(3)
            col_v1.metric("Nombre Total d'Excès", f"{nb_total_exces:,}")
            col_v2.metric("Véhicules Concernés", f"{nb_vehicules_exces}")
            # Vitesse_Max_Observee est maintenant le max des vitesses moyennes des segments
            col_v3.metric("Vitesse Max (Segment)", f"{resume_exces['Vitesse_Max_Observee'].max():.1f} km/h" if not resume_exces['Vitesse_Max_Observee'].empty else "N/A")


            # Graphique des taux d'excès par véhicule
            fig_exces = px.bar(
                resume_exces[resume_exces['Nb_Trajets_Exces'] > 0].sort_values('Pourcentage_Trajets_Exces', ascending=False),
                x='Véhicule',
                y='Pourcentage_Trajets_Exces',
                title=f"Pourcentage de Trajets en Excès de Vitesse (>{seuil_vitesse} km/h) par Véhicule",
                labels={'Pourcentage_Trajets_Exces': '% de Trajets en Excès'},
                color='Niveau_Risque',
                color_discrete_map={
                    'Faible': 'green',
                    'Modéré': 'orange',
                    'Élevé': 'red',
                    'Très élevé': 'darkred'
                },
                hover_data=['Nb_Trajets_Exces', 'Nb_Total_Trajets', 'Vitesse_Max_Observee']
            )
            st.plotly_chart(fig_exces, use_container_width=True)

            # Histogramme des vitesses moyennes des segments en excès
            if not detail_exces.empty:
                fig_histogramme = px.histogram(
                    detail_exces,
                    x='Vitesse moyenne', # C'est la vitesse moyenne du segment
                    title="Distribution des Vitesses Moyennes des Trajets en Excès",
                    labels={'Vitesse moyenne': 'Vitesse Moyenne du Trajet (km/h)'},
                    color='Niveau_Exces',
                    color_discrete_map={
                        'Léger (< 10 km/h)': 'green',
                        'Modéré (10-20 km/h)': 'orange',
                        'Important (20-30 km/h)': 'red',
                        'Grave (> 30 km/h)': 'darkred'
                    },
                    nbins=30
                )
                st.plotly_chart(fig_histogramme, use_container_width=True)

            # Tableau récapitulatif
            st.subheader("Résumé des Excès par Véhicule")
            cols_resume_exces = [
                'Véhicule', 'Nb_Trajets_Exces', 'Nb_Total_Trajets', 'Pourcentage_Trajets_Exces',
                'Vitesse_Max_Observee', 'Vitesse_Moyenne_Trajets', 'Depassement_Moyen', 'Niveau_Risque'
            ]
            afficher_dataframe_avec_export(
                resume_exces[[c for c in cols_resume_exces if c in resume_exces.columns]],
                "Résumé des Excès",
                key="geoloc_resume_exces"
            )

            # Détail des excès
            if not detail_exces.empty:
                st.subheader("Détail des Trajets en Excès de Vitesse")
                cols_detail_exces = [
                    'Véhicule', 'Date', 'Début', 'Fin', 'Distance', 'Vitesse moyenne', # Utiliser Vitesse moyenne
                    'Depassement_km/h', 'Niveau_Exces'
                ]
                afficher_dataframe_avec_export(
                    detail_exces[[c for c in cols_detail_exces if c in detail_exces.columns]],
                    "Détail des Excès",
                    key="geoloc_detail_exces"
                )

    with tab_utilisation:
        st.subheader("Analyse de l'Utilisation des Véhicules")

        # Analyse de l'utilisation
        with st.spinner("Analyse de l'utilisation en cours..."):
             # Utiliser df_geoloc_filtered_orig car analyser_utilisation_vehicules filtre par Type='Trajet' en interne
            utilisation_vehicules, utilisation_quotidienne = analyser_utilisation_vehicules(
                df_geoloc_filtered_orig, date_debut, date_fin
            )

        if utilisation_vehicules.empty:
            st.info("Données insuffisantes pour analyser l'utilisation des véhicules.")
        else:
            # KPIs généraux
            duree_totale_heures = utilisation_vehicules['Duree_Totale_Heures'].sum()
            distance_totale = utilisation_vehicules['Distance_Totale'].sum()
            nb_trajets_weekend = utilisation_vehicules['Nb_Trajets_Weekend'].sum()
            nb_trajets_total = utilisation_vehicules['Nb_Trajets'].sum()
            pct_weekend = (nb_trajets_weekend / nb_trajets_total * 100) if nb_trajets_total > 0 else 0

            col_u1, col_u2, col_u3 = st.columns(3)
            col_u1.metric("Durée Totale d'Utilisation (Trajets)", f"{duree_totale_heures:,.1f} h")
            col_u2.metric("Distance Totale (Trajets)", f"{distance_totale:,.1f} km")
            col_u3.metric("Utilisation Weekend (Trajets)", f"{pct_weekend:.1f}%")

            # Graphique d'utilisation par véhicule
            fig_utilisation = px.bar(
                utilisation_vehicules.sort_values('Duree_Totale_Heures', ascending=False),
                x='Véhicule',
                y=['Duree_Totale_Heures', 'Distance_Totale'],
                title="Durée d'Utilisation et Distance par Véhicule (Trajets)",
                labels={'value': 'Valeur', 'variable': 'Métrique'},
                barmode='group'
            )
            st.plotly_chart(fig_utilisation, use_container_width=True)

            # Graphique des périodes d'utilisation
            periodes_jours = ['Matin (6h-9h)', 'Matinée (9h-12h)', 'Midi (12h-14h)',
                             'Après-midi (14h-17h)', 'Soir (17h-20h)', 'Nuit (20h-6h)']

            periodes_presentes = [p for p in periodes_jours if p in utilisation_vehicules.columns]
            if periodes_presentes:
                fig_periodes = px.bar(
                    utilisation_vehicules.sort_values('Nb_Trajets', ascending=False),
                    x='Véhicule',
                    y=periodes_presentes,
                    title="Répartition des Trajets par Période de la Journée",
                    labels={'value': 'Nombre de Trajets', 'variable': 'Période'},
                    barmode='stack'
                )
                st.plotly_chart(fig_periodes, use_container_width=True)

            # Pourcentage d'utilisation le weekend
            fig_weekend = px.bar(
                utilisation_vehicules.sort_values('Pourcentage_Trajets_Weekend', ascending=False),
                x='Véhicule',
                y='Pourcentage_Trajets_Weekend',
                title="Pourcentage d'Utilisation le Weekend par Véhicule (Trajets)",
                labels={'Pourcentage_Trajets_Weekend': '% Trajets Weekend'}
            )
            st.plotly_chart(fig_weekend, use_container_width=True)

            # Évolution quotidienne pour un véhicule spécifique
            if not utilisation_quotidienne.empty:
                vehicules_disponibles = sorted(utilisation_quotidienne['Véhicule'].unique())
                if vehicules_disponibles:
                    vehicule_selectionne = st.selectbox(
                        "Sélectionner un véhicule pour l'évolution quotidienne",
                        options=vehicules_disponibles,
                        key="veh_evol_quot_geoloc"
                    )

                    data_vehicule = utilisation_quotidienne[utilisation_quotidienne['Véhicule'] == vehicule_selectionne]

                    fig_evol_veh = px.line(
                        data_vehicule,
                        x='Date',
                        y=['Distance_Jour', 'Duree_Jour_Heures'],
                        title=f"Évolution Quotidienne (Trajets) - {vehicule_selectionne}",
                        labels={'value': 'Valeur', 'variable': 'Métrique'},
                        markers=True
                    )
                    st.plotly_chart(fig_evol_veh, use_container_width=True)

            # Tableau récapitulatif
            st.subheader("Utilisation Détaillée par Véhicule (Trajets)")
            cols_utilisation = [
                'Véhicule', 'Nb_Trajets', 'Distance_Totale', 'Duree_Totale_Heures',
                'Distance_Moyenne_Trajet', 'Duree_Moyenne_Trajet', 'Vitesse_Moyenne',
                'Nb_Trajets_Weekend', 'Pourcentage_Trajets_Weekend'
            ]

            for periode in periodes_jours:
                if periode in utilisation_vehicules.columns:
                    cols_utilisation.append(periode)

            afficher_dataframe_avec_export(
                utilisation_vehicules[[c for c in cols_utilisation if c in utilisation_vehicules.columns]],
                "Utilisation Détaillée (Trajets)",
                key="geoloc_utilisation_detail_trajets"
            )

    with tab_trajets_suspects:
        st.subheader("Analyse des Trajets Suspects")

        # Détection des trajets suspects
        with st.spinner("Détection des trajets suspects en cours..."):
            # Utiliser df_geoloc_filtered_orig car les fonctions internes filtrent par Type au besoin
            trajets_suspects = detecter_trajets_suspects(df_geoloc_filtered_orig, date_debut, date_fin)

            _, transactions_sans_presence = analyser_correspondance_transactions_geoloc(
                df_geoloc_filtered_orig, df_transactions, df_vehicules, date_debut, date_fin
            )

            detours_suspects = detecter_detours_suspects(df_geoloc_filtered_orig, date_debut, date_fin)

            resume_anomalies = generer_resume_anomalies_geolocalisation(
                df_geoloc_filtered_orig, df_transactions, df_vehicules, date_debut, date_fin
            )

        # Affichage du tableau de bord de risque
        if not resume_anomalies.empty:
            st.warning(f"⚠️ Détection de véhicules présentant des anomalies de géolocalisation")

            vehicules_risque_eleve = resume_anomalies[resume_anomalies['Niveau_Risque'].isin(['Élevé', 'Critique'])]

            if not vehicules_risque_eleve.empty:
                nb_vehicules_risque = len(vehicules_risque_eleve)
                st.error(f"🚨 {nb_vehicules_risque} véhicule(s) présentent un niveau de risque élevé ou critique")

                cols_risque = ['Véhicule', 'Score_Risque_Total', 'Niveau_Risque', 'Résumé_Anomalies']
                afficher_dataframe_avec_export(
                    vehicules_risque_eleve[[c for c in cols_risque if c in vehicules_risque_eleve.columns]],
                    "Véhicules à Risque Élevé/Critique",
                    key="vehicules_risque_eleve_geoloc"
                )

            st.subheader("Tableau de Bord des Risques par Véhicule (Géolocalisation)")
            cols_resume = [
                'Véhicule', 'Niveau_Risque', 'Score_Risque_Total', 'Résumé_Anomalies',
                'Nb_Trajets_Exces', 'Nb_Trajets_Suspects', 'Nb_Transactions_Suspectes',
                'Nb_Detours_Suspects', 'Ecart_Distance_Km', 'Pourcentage_Ecart'
            ]
            afficher_dataframe_avec_export(
                resume_anomalies[[c for c in cols_resume if c in resume_anomalies.columns]],
                "Tableau de Bord des Risques (Géoloc)",
                key="tableau_bord_risques_geoloc"
            )

            fig_risque = px.bar(
                resume_anomalies.sort_values('Score_Risque_Total', ascending=False),
                x='Véhicule',
                y='Score_Risque_Total',
                title="Score de Risque Total par Véhicule (Géolocalisation)",
                labels={'Score_Risque_Total': 'Score de Risque'},
                color='Niveau_Risque',
                color_discrete_map={
                    'Faible': 'green',
                    'Modéré': 'yellow',
                    'Élevé': 'orange',
                    'Critique': 'red'
                }
            )
            st.plotly_chart(fig_risque, use_container_width=True)
        else:
            st.success("✅ Aucune anomalie significative détectée dans les données de géolocalisation.")

        if not trajets_suspects.empty:
            st.subheader("Trajets Suspects Détectés (Hors heures, Weekend, Vitesse anormale)")
            nb_trajets_suspects_val = len(trajets_suspects) # Renommé
            st.warning(f"⚠️ {nb_trajets_suspects_val} trajets suspects détectés.")
            cols_ts = [
                'Véhicule', 'Date_Heure_Debut', 'Distance', 'Durée_minutes',
                'Vitesse moyenne', 'Description_Suspicion', 'Niveau_Suspicion'
            ]
            afficher_dataframe_avec_export(
                trajets_suspects[[c for c in cols_ts if c in trajets_suspects.columns]],
                "Détail des Trajets Suspects",
                key="detail_trajets_suspects_geoloc"
            )
            trajets_par_vehicule = trajets_suspects.groupby('Véhicule').size().reset_index(name='Nombre_Trajets_Suspects')
            fig_ts_vehicule = px.bar(
                trajets_par_vehicule.sort_values('Nombre_Trajets_Suspects', ascending=False),
                x='Véhicule', y='Nombre_Trajets_Suspects',
                title="Nombre de Trajets Suspects par Véhicule",
                labels={'Nombre_Trajets_Suspects': 'Nombre de Trajets Suspects'}
            )
            st.plotly_chart(fig_ts_vehicule, use_container_width=True)

        if not transactions_sans_presence.empty:
            st.subheader("Transactions sans Présence du Véhicule (Basé sur Géolocalisation)")
            nb_transactions_suspectes_val = len(transactions_sans_presence) # Renommé
            st.warning(f"⚠️ {nb_transactions_suspectes_val} transactions sans présence détectée du véhicule.")
            cols_trans = [
                'Nouveau Immat', 'Date', 'Hour', 'Place', 'Quantity', 'Amount',
                'Methode_Verification', 'Est_Weekend', 'Est_Hors_Heures', 'Niveau_Suspicion'
            ]
            afficher_dataframe_avec_export(
                transactions_sans_presence[[c for c in cols_trans if c in transactions_sans_presence.columns]],
                "Transactions sans Présence Détectée",
                key="transactions_sans_presence_geoloc"
            )

        if not detours_suspects.empty:
            st.subheader("Détours Potentiels Détectés")
            nb_detours = len(detours_suspects)
            st.warning(f"⚠️ {nb_detours} trajets avec détours potentiels détectés.")
            cols_detours = [
                'Véhicule', 'Date', 'Début', 'Distance', 'Durée_minutes',
                'Vitesse moyenne', 'Severite_Detour', 'Description_Detour'
            ]
            afficher_dataframe_avec_export(
                detours_suspects[[c for c in cols_detours if c in detours_suspects.columns]],
                "Détail des Détours Suspects",
                key="detail_detours_suspects_geoloc"
            )
            detours_par_severite = detours_suspects['Severite_Detour'].value_counts().reset_index()
            detours_par_severite.columns = ['Sévérité', 'Nombre']
            fig_severite = px.pie(
                detours_par_severite, values='Nombre', names='Sévérité',
                title="Répartition des Détours par Sévérité", color='Sévérité',
                color_discrete_map={'Léger': 'green', 'Modéré': 'orange', 'Important': 'red'}
            )
            st.plotly_chart(fig_severite, use_container_width=True)

    with tab_carte:
        st.subheader("Visualisation des Trajets sur Carte")
        coords_disponibles = (
            'Latitude_depart' in df_geoloc_filtered_orig.columns and
            'Longitude_depart' in df_geoloc_filtered_orig.columns and
            'Latitude_arrivee' in df_geoloc_filtered_orig.columns and
            'Longitude_arrivee' in df_geoloc_filtered_orig.columns and
            not df_geoloc_filtered_orig['Latitude_depart'].isna().all() and
            not df_geoloc_filtered_orig['Latitude_arrivee'].isna().all()
        )

        if not coords_disponibles:
            st.warning("Les coordonnées GPS (départ ET arrivée) ne sont pas disponibles dans les données. La visualisation sur carte n'est pas possible.")
        else:
            st.sidebar.subheader("Filtres pour la Carte")
            vehicules_carte = sorted(df_geoloc_filtered_orig['Véhicule'].unique())
            vehicule_carte = st.sidebar.selectbox(
                "Véhicule à visualiser", options=["Tous les véhicules"] + vehicules_carte,
                key="carte_vehicule_filter_geoloc"
            )
            highlight_anomalies_carte = st.sidebar.checkbox( # Renommé pour unicité de clé
                "Mettre en évidence les anomalies (carte)", value=True, key="highlight_anomalies_carte"
            )
            visualiser_trajets_sur_carte(
                df_geoloc_filtered_orig, # Passer le df filtré par date, la fonction interne filtre par Type Trajet
                vehicule_carte if vehicule_carte != "Tous les véhicules" else None,
                date_debut, date_fin, highlight_anomalies_carte
            )
            st.markdown("""
            ### Légende
            - **Points verts (play)**: Points de départ des trajets
            - **Points rouges (stop)**: Points d'arrivée des trajets
            - **Lignes**:
                - *Mode Normal*: Vert (lent) -> Bleu (moyen) -> Orange (rapide) -> Rouge (très rapide)
                - *Mode Anomalies*: Orange (Weekend), Rouge (Excès Vitesse), Violet (Détour - si implémenté)
            """)
            st.info("📌 Zoomez/Dézoomez, cliquez sur les tracés/marqueurs pour détails.")

    with tab_integration:
        st.subheader("Intégration Géolocalisation - Carburant: Analyse d'Efficacité")
        with st.spinner("Analyse de l'efficacité du carburant en cours..."):
             # Utiliser df_geoloc_filtered_orig car la fonction interne filtre par Type='Trajet'
            efficacite_carburant = analyser_efficacite_carburant(
                df_geoloc_filtered_orig, df_transactions, df_vehicules, date_debut, date_fin
            )

        if efficacite_carburant.empty:
            st.info("Données insuffisantes pour analyser l'efficacité du carburant.")
        else:
            conso_moyenne_eff = efficacite_carburant['Consommation_100km'].mean() # Renommé
            cout_km_moyen_eff = efficacite_carburant['Cout_par_km'].mean() # Renommé

            col_e1, col_e2 = st.columns(2)
            col_e1.metric("Consommation Moyenne (basée géoloc)", f"{conso_moyenne_eff:.1f} L/100km" if pd.notna(conso_moyenne_eff) else "N/A")
            col_e2.metric("Coût Moyen par Km (basé géoloc)", f"{cout_km_moyen_eff:.1f} CFA/km" if pd.notna(cout_km_moyen_eff) else "N/A")

            fig_efficacite = px.bar(
                efficacite_carburant.sort_values('Score_Efficacite', ascending=False),
                x='Véhicule', y='Score_Efficacite', title="Score d'Efficacité par Véhicule",
                labels={'Score_Efficacite': "Score d'Efficacité (100 = moyenne)"},
                color='Niveau_Efficacite',
                color_discrete_map={
                    'Très faible': 'red', 'Faible': 'orange', 'Normale': 'yellow',
                    'Bonne': 'lightgreen', 'Excellente': 'darkgreen'}
            )
            st.plotly_chart(fig_efficacite, use_container_width=True)

            st.subheader("Efficacité Détaillée par Véhicule")
            cols_efficacite = [
                'Véhicule', 'Catégorie', 'Distance_Geoloc_Totale', 'Volume_Total',
                'Consommation_100km', 'Conso_Moyenne_Cat', 'Ecart_Conso_Pct',
                'Cout_par_km', 'Score_Efficacite', 'Niveau_Efficacite'
            ]
            afficher_dataframe_avec_export(
                efficacite_carburant[[c for c in cols_efficacite if c in efficacite_carburant.columns]],
                "Efficacité Carburant", key="efficacite_carburant_geoloc"
            )

            efficacite_par_cat = efficacite_carburant.dropna(subset=['Consommation_100km', 'Conso_Moyenne_Cat'])
            if not efficacite_par_cat.empty:
                fig_comp_cat = make_subplots(specs=[[{"secondary_y": True}]])
                fig_comp_cat.add_trace(go.Bar(x=efficacite_par_cat['Véhicule'], y=efficacite_par_cat['Consommation_100km'], name="Consommation Réelle", marker_color='blue'), secondary_y=False)
                fig_comp_cat.add_trace(go.Scatter(x=efficacite_par_cat['Véhicule'], y=efficacite_par_cat['Conso_Moyenne_Cat'], name="Moyenne Catégorie", marker_color='red', mode='lines'), secondary_y=False)
                fig_comp_cat.add_trace(go.Bar(x=efficacite_par_cat['Véhicule'], y=efficacite_par_cat['Ecart_Conso_Pct'], name="Écart (%)", marker_color='green'), secondary_y=True)
                fig_comp_cat.update_layout(title="Comparaison Consommation Réelle vs Moyenne Catégorie", xaxis_title="Véhicule", yaxis_title="Consommation (L/100km)", yaxis2_title="Écart (%)")
                st.plotly_chart(fig_comp_cat, use_container_width=True)

            st.subheader("Recommandations d'Optimisation")
            efficacite_faible = efficacite_carburant[efficacite_carburant['Niveau_Efficacite'].isin(['Très faible', 'Faible'])]
            if not efficacite_faible.empty:
                st.warning(f"⚠️ {len(efficacite_faible)} véhicule(s) présentent une efficacité carburant faible ou très faible")
                recommandations_txt = """### Recommandations pour améliorer l'efficacité:
                1. **Véhicules à efficacité très faible**: Envisager un diagnostic mécanique.
                2. **Trajets avec détours fréquents**: Optimiser les itinéraires.
                3. **Excès de vitesse réguliers**: Sensibiliser les conducteurs.
                4. **Écarts importants kilométrage déclaré/géolocalisé**: Contrôle systématique des déclarations.
                5. **Transactions sans présence détectée**: Renforcer les contrôles sur l'utilisation des cartes."""
                st.markdown(recommandations_txt)
                st.subheader("Véhicules Prioritaires pour Optimisation")
                cols_opti = ['Véhicule', 'Catégorie', 'Consommation_100km', 'Ecart_Conso_Pct', 'Score_Efficacite', 'Niveau_Efficacite']
                afficher_dataframe_avec_export(
                    efficacite_faible[[c for c in cols_opti if c in efficacite_faible.columns]],
                    "Véhicules à Optimiser", key="vehicules_a_optimiser_geoloc"
                )


# --- Nouvelle fonction pour la détection avancée d'anomalies basée sur géolocalisation ---
def detecter_anomalies_geolocalisation(
    df_geoloc: pd.DataFrame,
    df_transactions: pd.DataFrame,
    df_vehicules: pd.DataFrame,
    date_debut: datetime.date,
    date_fin: datetime.date
) -> pd.DataFrame:
    """
    Agrégation de toutes les anomalies détectées via géolocalisation.
    Cette fonction est utilisée notamment pour l'intégration dans la page principale d'anomalies.

    Args:
        df_geoloc: DataFrame des données de géolocalisation.
        df_transactions: DataFrame des transactions.
        df_vehicules: DataFrame des véhicules.
        date_debut: Date de début de la période d'analyse.
        date_fin: Date de fin de la période d'analyse.

    Returns:
        Un DataFrame des anomalies avec leur description et score de risque.
    """
    anomalies = []

    # Filtrer les données pour la période
    mask_date_geoloc = (df_geoloc['Date'].dt.date >= date_debut) & (df_geoloc['Date'].dt.date <= date_fin)
    df_geoloc_filtered = df_geoloc[mask_date_geoloc]


    if df_geoloc_filtered.empty:
        return pd.DataFrame()

    # 1. Détecter les excès de vitesse
    seuil_vitesse_anomalie = 90 # Seuil fixe pour l'agrégation d'anomalies
    _, detail_exces = analyser_exces_vitesse(df_geoloc_filtered, date_debut, date_fin, seuil_vitesse_anomalie)

    if not detail_exces.empty:
        for idx, row in detail_exces.iterrows():
            anomalie = {
                'Véhicule': row['Véhicule'],
                'Date': row['Date'],
                'Heure': row.get('Début_str', row.get('Début')), # Utiliser Début_str si disponible, sinon Début
                'type_anomalie': 'Excès de vitesse (géoloc)',
                'detail_anomalie': f"Vitesse: {row['Vitesse moyenne']:.1f} km/h, Dépassement: {row.get('Depassement_km/h', 'N/A'):.1f} km/h",
                'Niveau_Anomalie': row.get('Niveau_Exces', 'N/A'),
                'poids_anomalie': st.session_state.get('ss_poids_vitesse_excessive', DEFAULT_POIDS_VITESSE_EXCESSIVE)
            }
            anomalies.append(anomalie)

    # 2. Détecter les trajets suspects (hors heures, weekend)
    trajets_suspects = detecter_trajets_suspects(df_geoloc_filtered, date_debut, date_fin)

    if not trajets_suspects.empty:
        for idx, row in trajets_suspects.iterrows():
            # Extraire date et heure de Date_Heure_Debut
            date_heure_str = row.get('Date_Heure_Debut', "N/A N/A")
            try:
                date_part_str, heure_part_str = date_heure_str.split(" ", 1)
                date_val = datetime.strptime(date_part_str, '%d/%m/%Y').date() if date_part_str != "N/A" else None
                heure_val = heure_part_str if heure_part_str != "N/A" else None
            except ValueError:
                date_val = None
                heure_val = None

            anomalie = {
                'Véhicule': row['Véhicule'],
                'Date': date_val,
                'Heure': heure_val,
                'type_anomalie': 'Trajet suspect (géoloc)',
                'detail_anomalie': row['Description_Suspicion'],
                'Niveau_Anomalie': row.get('Niveau_Suspicion', 'N/A'),
                'poids_anomalie': row['Score_Suspicion_Total']
            }
            anomalies.append(anomalie)

    # 3. Détecter les détours suspects
    detours_suspects = detecter_detours_suspects(df_geoloc_filtered, date_debut, date_fin)

    if not detours_suspects.empty:
        for idx, row in detours_suspects.iterrows():
            anomalie = {
                'Véhicule': row['Véhicule'],
                'Date': row['Date'],
                'Heure': row.get('Début_str', row.get('Début')),
                'type_anomalie': 'Détour suspect (géoloc)',
                'detail_anomalie': row['Description_Detour'],
                'Niveau_Anomalie': row.get('Severite_Detour', 'N/A'),
                'poids_anomalie': row['Score_Detour']
            }
            anomalies.append(anomalie)

    # 4. Détecter les transactions sans présence
    _, transactions_sans_presence = analyser_correspondance_transactions_geoloc(
        df_geoloc_filtered, df_transactions, df_vehicules, date_debut, date_fin
    )

    if not transactions_sans_presence.empty:
        for idx, row in transactions_sans_presence.iterrows():
            anomalie = {
                'Véhicule': row['Nouveau Immat'],
                'Date': row['Date'],
                'Heure': row['Hour'].strftime('%H:%M:%S') if pd.notna(row['Hour']) else None,
                'type_anomalie': 'Transaction sans présence (géoloc)',
                'detail_anomalie': f"Station: {row['Place']}, Quantité: {row['Quantity']:.1f}L, Montant: {row['Amount']:.0f} CFA, Vérif: {row['Methode_Verification']}",
                'Niveau_Anomalie': row.get('Niveau_Suspicion', 'N/A'),
                'poids_anomalie': row['Score_Suspicion']
            }
            anomalies.append(anomalie)

    # 5. Détecter les anomalies de distance (comparaison géoloc vs transactions)
    _, anomalies_distance = analyser_geolocalisation_vs_transactions(
        df_geoloc_filtered, df_transactions, df_vehicules, date_debut, date_fin
    )

    if not anomalies_distance.empty:
        for idx, row in anomalies_distance.iterrows():
            # Cette anomalie est globale pour la période, pas liée à une date/heure spécifique
            anomalie = {
                'Véhicule': row['Immatriculation'],
                'Date': date_fin, # Ou None, ou date de fin de période
                'Heure': None,
                'type_anomalie': row['Type_Anomalie'] + " (géoloc)", # Pour distinguer
                'detail_anomalie': f"Géoloc: {row['Distance_Geoloc_Totale']:.1f}km, Déclaré: {row['Distance_Declaree_Totale']:.1f}km, Écart: {row['Ecart_Distance']:.1f}km ({row['Pourcentage_Ecart']:.1f}%)",
                'Niveau_Anomalie': row.get('Gravite', 'N/A'),
                'poids_anomalie': abs(row['Pourcentage_Ecart']) * (st.session_state.get('ss_poids_detour_suspect', DEFAULT_POIDS_DETOUR_SUSPECT) / 20) # Ajuster pondération
            }
            anomalies.append(anomalie)


    if not anomalies:
        return pd.DataFrame()

    df_anomalies = pd.DataFrame(anomalies)

    # Ajouter Card num. et Catégorie pour la fusion avec les autres anomalies
    mapping_carte = df_vehicules.set_index('Nouveau Immat')['N° Carte'].to_dict()
    mapping_categorie = df_vehicules.set_index('Nouveau Immat')['Catégorie'].to_dict()

    df_anomalies['Nouveau Immat'] = df_anomalies['Véhicule'] # Assurer que 'Nouveau Immat' est là
    df_anomalies['Card num.'] = df_anomalies['Véhicule'].map(mapping_carte)
    df_anomalies['Catégorie'] = df_anomalies['Véhicule'].map(mapping_categorie)


    df_anomalies = df_anomalies.sort_values(['Véhicule', 'poids_anomalie'], ascending=[True, False])

    return df_anomalies

# ---------------------------------------------------------------------
# Fonctions d'Affichage des Pages (Mises à jour avec intégration géolocalisation)
# ---------------------------------------------------------------------

def afficher_page_dashboard(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, df_ge: pd.DataFrame, df_autres: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date, df_geoloc: Optional[pd.DataFrame] = None):
    """Affiche le tableau de bord principal."""
    st.header(f"📊 Tableau de Bord Principal ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    total_volume = df_transactions['Quantity'].sum()
    total_cout = df_transactions['Amount'].sum()
    nb_transactions_total = len(df_transactions) # Renommé
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

    nb_anomalies_geoloc_dash = 0 # Initialisation
    anomalies_geoloc_dash_df = pd.DataFrame()
    if df_geoloc is not None and not df_geoloc.empty:
        with st.spinner("Analyse des anomalies de géolocalisation (dashboard)..."):
            anomalies_geoloc_dash_df = detecter_anomalies_geolocalisation(
                df_geoloc, df_transactions, df_vehicules, date_debut, date_fin
            )
            nb_anomalies_geoloc_dash = len(anomalies_geoloc_dash_df)


    st.subheader("🚀 Indicateurs Clés")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Volume Total", f"{total_volume:,.0f} L")
    col2.metric("Coût Total", f"{total_cout:,.0f} CFA")
    col3.metric("Transactions", f"{nb_transactions_total:,}")
    col4.metric("Véhicules Actifs", f"{cartes_veh_actives:,}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Conso. Moyenne Globale", f"{conso_moyenne_globale:.1f} L/100km" if conso_moyenne_globale else "N/A")
    col6.metric("Coût Moyen / Km Global", f"{cout_km_global:.1f} CFA/km" if cout_km_global else "N/A")
    col7.metric("Prix Moyen / Litre", f"{prix_moyen_litre_global:,.0f} CFA/L" if prix_moyen_litre_global else "N/A")

    if df_geoloc is not None and not df_geoloc.empty:
        col8.metric("Anomalies Géoloc", f"{nb_anomalies_geoloc_dash:,}", delta_color="inverse")
    else:
        col8.metric("Anomalies Géoloc", "N/A")


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
             st.info("Aucune anomalie de transaction détectée pour le classement par risque.")

    if not cartes_inconnues_dash.empty:
        with st.expander("🚨 Cartes Inconnues Détectées", expanded=False):
            afficher_dataframe_avec_export(cartes_inconnues_dash, "Détail des Cartes Inconnues", key="dash_cartes_inconnues")

    # Aperçu des anomalies de géolocalisation
    if df_geoloc is not None and not df_geoloc.empty and not anomalies_geoloc_dash_df.empty:
        with st.expander("🚨 Aperçu des Anomalies de Géolocalisation", expanded=True):
            summary_geoloc_dash = anomalies_geoloc_dash_df.groupby('type_anomalie').agg(
                Nombre=('type_anomalie', 'size'),
                Score_Moyen=('poids_anomalie', 'mean') # poids_anomalie est le nom utilisé
            ).reset_index().sort_values('Nombre', ascending=False)

            afficher_dataframe_avec_export(summary_geoloc_dash, "Résumé Anomalies Géolocalisation (Dashboard)", key="dash_anomalies_geoloc")

            fig_anomalies_geoloc_dash = px.bar(
                summary_geoloc_dash,
                x='type_anomalie', y='Nombre', title="Anomalies de Géolocalisation par Type (Dashboard)",
                color='Score_Moyen', labels={'Nombre': "Nombre d'occurrences"}
            )
            st.plotly_chart(fig_anomalies_geoloc_dash, use_container_width=True)
            st.markdown("""👉 *Pour une analyse détaillée des anomalies de géolocalisation, utilisez la page "Géolocalisation"*""")


def afficher_page_anomalies(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date, df_geoloc: Optional[pd.DataFrame] = None):
    """Affiche la page de synthèse des anomalies."""
    st.header(f"🚨 Détection des Anomalies ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    df_anomalies_all = pd.DataFrame() # Pour stocker toutes les anomalies
    df_scores_all = pd.DataFrame() # Pour stocker tous les scores

    with st.spinner("Détection des anomalies de transaction en cours..."):
         df_anomalies_transac = detecter_anomalies(df_transactions, df_vehicules)


    if df_geoloc is not None and not df_geoloc.empty:
        with st.spinner("Détection des anomalies de géolocalisation en cours..."):
            df_anomalies_geoloc = detecter_anomalies_geolocalisation(df_geoloc, df_transactions, df_vehicules, date_debut, date_fin)

        # Fusionner les anomalies
        if not df_anomalies_transac.empty and not df_anomalies_geoloc.empty:
            # Assurer la cohérence des colonnes pour la concaténation
            # Garder les colonnes communes pertinentes pour le calcul de score et l'affichage
            cols_communes = ['Nouveau Immat', 'Card num.', 'Catégorie', 'Date', 'type_anomalie', 'detail_anomalie', 'poids_anomalie']
            # S'assurer que toutes ces colonnes existent dans les deux DFs, sinon les créer avec NaT/NaN/None
            for col in cols_communes:
                if col not in df_anomalies_transac.columns: df_anomalies_transac[col] = pd.NA
                if col not in df_anomalies_geoloc.columns: df_anomalies_geoloc[col] = pd.NA

            df_anomalies_all = pd.concat([
                df_anomalies_transac[cols_communes],
                df_anomalies_geoloc[cols_communes]
            ], ignore_index=True)

        elif not df_anomalies_transac.empty:
            df_anomalies_all = df_anomalies_transac
        elif not df_anomalies_geoloc.empty:
            df_anomalies_all = df_anomalies_geoloc
    else: # Pas de données de géoloc
        df_anomalies_all = df_anomalies_transac

    # Convert Date column to datetime objects for pyarrow compatibility in geoloc anomalies
    if df_geoloc is not None and not df_geoloc.empty and 'df_anomalies_geoloc' in locals() and not df_anomalies_geoloc.empty:
         df_anomalies_geoloc['Date'] = pd.to_datetime(df_anomalies_geoloc['Date'])

    if not df_anomalies_all.empty:
        df_anomalies_all['Date'] = pd.to_datetime(df_anomalies_all['Date'])
        df_scores_all = calculer_score_risque(df_anomalies_all)
    else:
        df_scores_all = pd.DataFrame(columns=['Nouveau Immat', 'Card num.', 'Catégorie', 'nombre_total_anomalies', 'score_risque'])


    # Afficher les résultats
    tab_resume, tab_transactions_detail, tab_geoloc_detail = st.tabs([
        "📊 Résumé Global", "💳 Détail Anomalies Transactions", "📍 Détail Anomalies Géolocalisation"
    ])

    with tab_resume:
        if df_anomalies_all.empty:
            st.success("✅ Aucune anomalie (transaction ou géoloc) détectée sur la période sélectionnée !")
            return

        nb_total_anomalies_all = len(df_anomalies_all)
        nb_vehicules_avec_anomalies_all = df_anomalies_all['Card num.'].nunique()
        st.warning(f"Détecté : **{nb_total_anomalies_all:,}** anomalies (tous types confondus) concernant **{nb_vehicules_avec_anomalies_all:,}** véhicules.")

        st.subheader(f"🎯 Véhicules Suspects (Score de Risque Global ≥ {st.session_state.ss_seuil_anomalies_suspectes_score})")
        vehicules_suspects_all = df_scores_all[df_scores_all['score_risque'] >= st.session_state.ss_seuil_anomalies_suspectes_score]

        if not vehicules_suspects_all.empty:
            # Préparer le pivot pour les détails par type d'anomalie
            pivot_details_all = df_anomalies_all.groupby(['Nouveau Immat', 'Card num.', 'Catégorie', 'type_anomalie']).size().unstack(fill_value=0)
            vehicules_suspects_details_all = vehicules_suspects_all.merge(pivot_details_all, on=['Nouveau Immat', 'Card num.', 'Catégorie'], how='left').fillna(0)
            afficher_dataframe_avec_export(vehicules_suspects_details_all, f"Liste des {len(vehicules_suspects_all)} Véhicules Suspects (Score Global)", key="anom_suspects_score_all")

            with st.expander("Voir les transactions/événements détaillés des véhicules suspects (score global)"):
                details_suspects_all_transac = df_anomalies_all[df_anomalies_all['Card num.'].isin(vehicules_suspects_all['Card num.'])]
                cols_display_detail_all = ['Date', 'Nouveau Immat', 'Catégorie', 'type_anomalie', 'detail_anomalie', 'poids_anomalie']
                # Ajouter Quantity, Amount, Place si elles existent (pour anomalies de transaction)
                if 'Quantity' in details_suspects_all_transac.columns: cols_display_detail_all.append('Quantity')
                if 'Amount' in details_suspects_all_transac.columns: cols_display_detail_all.append('Amount')
                if 'Place' in details_suspects_all_transac.columns: cols_display_detail_all.append('Place')

                cols_final_detail_all = [col for col in cols_display_detail_all if col in details_suspects_all_transac.columns]
                afficher_dataframe_avec_export(details_suspects_all_transac[cols_final_detail_all], "Détail Événements des Suspects (Score Global)", key="anom_suspects_details_transac_all")
        else:
            st.info("Aucun véhicule n'atteint le seuil de score de risque suspect (global).")

        st.subheader("📊 Synthèse par Type d'Anomalie (Global)")
        summary_type_all = df_anomalies_all.groupby('type_anomalie').agg(
            Nombre=('type_anomalie', 'size'),
            Score_Total=('poids_anomalie', 'sum'),
            Nb_Vehicules_Touches=('Card num.', 'nunique')
        ).reset_index().sort_values('Score_Total', ascending=False)
        afficher_dataframe_avec_export(summary_type_all, "Nombre et Score par Type d'Anomalie (Global)", key="anom_summary_type_all")

        fig_summary_type_all = px.bar(summary_type_all, x='type_anomalie', y='Nombre', title="Nombre d'Anomalies par Type (Global)", color='Score_Total', labels={'Nombre':"Nombre d'occurrences", 'type_anomalie':'Type d\'Anomalie'})
        st.plotly_chart(fig_summary_type_all, use_container_width=True)

    with tab_transactions_detail:
        if df_anomalies_transac.empty:
            st.success("✅ Aucune anomalie de transaction détectée sur la période sélectionnée !")
        else:
            nb_total_anomalies_trans = len(df_anomalies_transac)
            nb_vehicules_anomalies_trans = df_anomalies_transac['Card num.'].nunique()
            st.warning(f"Détecté : **{nb_total_anomalies_trans:,}** anomalies de transaction concernant **{nb_vehicules_anomalies_trans:,}** véhicules.")

            summary_type_trans = df_anomalies_transac.groupby('type_anomalie').agg(
                Nombre=('type_anomalie', 'size'),
                Score_Total=('poids_anomalie', 'sum'),
                Nb_Vehicules_Touches=('Card num.', 'nunique')
            ).reset_index().sort_values('Score_Total', ascending=False)

            afficher_dataframe_avec_export(summary_type_trans, "Résumé Anomalies Transactions", key="anom_summary_transactions_page")

            fig_trans_page = px.bar(summary_type_trans, x='type_anomalie', y='Nombre', title="Nombre d'Anomalies de Transaction par Type", color='Score_Total')
            st.plotly_chart(fig_trans_page, use_container_width=True)

            with st.expander("Voir toutes les anomalies de transaction"):
                cols_display_trans_page = ['Date', 'Hour', 'Nouveau Immat', 'Catégorie', 'type_anomalie', 'detail_anomalie', 'Quantity', 'Amount', 'Place', 'poids_anomalie']
                cols_final_trans_page = [col for col in cols_display_trans_page if col in df_anomalies_transac.columns]
                afficher_dataframe_avec_export(df_anomalies_transac[cols_final_trans_page], "Détail Anomalies Transactions (Page)", key="anom_all_transactions_page")

    with tab_geoloc_detail:
        if df_geoloc is None or df_geoloc.empty:
            st.info("Aucune donnée de géolocalisation disponible. Veuillez charger un fichier de géolocalisation.")
        elif 'df_anomalies_geoloc' not in locals() or df_anomalies_geoloc.empty : # Vérifier si df_anomalies_geoloc a été créé et n'est pas vide
            st.success("✅ Aucune anomalie de géolocalisation détectée sur la période sélectionnée !")
        else: # df_anomalies_geoloc existe et n'est pas vide
            nb_total_anomalies_geo = len(df_anomalies_geoloc)
            nb_vehicules_anomalies_geo = df_anomalies_geoloc['Card num.'].nunique()
            st.warning(f"Détecté : **{nb_total_anomalies_geo:,}** anomalies de géolocalisation concernant **{nb_vehicules_anomalies_geo:,}** véhicules.")

            summary_type_geo = df_anomalies_geoloc.groupby('type_anomalie').agg(
                Nombre=('type_anomalie', 'size'),
                Score_Total=('poids_anomalie', 'sum'),
                Nb_Vehicules_Touches=('Card num.', 'nunique')
            ).reset_index().sort_values('Score_Total', ascending=False)

            afficher_dataframe_avec_export(summary_type_geo, "Résumé Anomalies Géolocalisation (Page)", key="anom_summary_geoloc_page")

            fig_geo_page = px.bar(summary_type_geo, x='type_anomalie', y='Nombre', title="Nombre d'Anomalies de Géolocalisation par Type", color='Score_Total')
            st.plotly_chart(fig_geo_page, use_container_width=True)

            top_vehicules_geo_page = df_anomalies_geoloc.groupby('Nouveau Immat').agg(
                Nb_Anomalies=('type_anomalie', 'size'),
                Score_Total=('poids_anomalie', 'sum')
            ).reset_index().sort_values('Score_Total', ascending=False).head(10)

            st.subheader("Top 10 Véhicules avec Anomalies de Géolocalisation")
            afficher_dataframe_avec_export(top_vehicules_geo_page, "Top Véhicules Anomalies Géoloc (Page)", key="top_vehicules_geoloc_page")

            with st.expander("Voir toutes les anomalies de géolocalisation"):
                cols_display_geo_page = ['Date', 'Nouveau Immat', 'Catégorie', 'type_anomalie', 'detail_anomalie', 'poids_anomalie']
                cols_final_geo_page = [col for col in cols_display_geo_page if col in df_anomalies_geoloc.columns]
                afficher_dataframe_avec_export(df_anomalies_geoloc[cols_final_geo_page], "Détail Anomalies Géolocalisation (Page)", key="anom_all_geoloc_page")

        seuil_vitesse_anomalie = 90 # Seuil fixe pour l'affichage des paramètres
        st.subheader("Paramètres de Détection des Anomalies de Géolocalisation")
        with st.expander("Rappel des paramètres de détection (géoloc)", expanded=False):
            st.info(f"""
            - **Excès de vitesse**: > {seuil_vitesse_anomalie} km/h (seuil fixe pour agrégation)
            - **Trajets hors heures**: En dehors de {st.session_state.get('ss_heure_debut_service', DEFAULT_HEURE_DEBUT_SERVICE)}h-{st.session_state.get('ss_heure_fin_service', DEFAULT_HEURE_FIN_SERVICE)}h
            - **Détours suspects**: Écart de vitesse > {st.session_state.get('ss_seuil_detour_pct', DEFAULT_SEUIL_DETOUR_PCT)}% vs moyenne véhicule
            - **Transactions sans présence**: Basé sur rayon de {st.session_state.get('ss_rayon_station_km', DEFAULT_RAYON_STATION_KM)} km (si coordonnées station dispo) ou analyse temporelle.
            """)
            st.markdown("""Pour modifier ces paramètres, rendez-vous dans la page "Paramètres".""")


def afficher_page_parametres(df_vehicules: Optional[pd.DataFrame] = None):
    """Affiche la page des paramètres modifiables."""
    st.header("⚙️ Paramètres de l'Application")
    st.warning("Modifier ces paramètres affectera les analyses et la détection d'anomalies.")

    # Créer des onglets pour organiser les paramètres
    tab_generaux, tab_carburant, tab_geoloc = st.tabs([
        "⚙️ Paramètres Généraux", "⛽ Paramètres Carburant", "📍 Paramètres Géolocalisation"
    ])

    with tab_generaux:
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

        with st.expander("Heures Non Ouvrées (Transactions Carburant)"):
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

    with tab_carburant:
        with st.expander("Seuils de Consommation par Catégorie (L/100km)", expanded=True):
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

        with st.expander("Poids des Anomalies de Transaction pour Score de Risque"):
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
                st.session_state.ss_poids_hors_horaire = st.slider("Poids: Hors Horaires/WE (Transaction)", 1, 15, st.session_state.get('ss_poids_hors_horaire', DEFAULT_POIDS_HORS_HORAIRE), key='poids_hor')
                st.session_state.ss_poids_hors_service = st.slider("Poids: Véhicule Hors Service", 1, 15, st.session_state.get('ss_poids_hors_service', DEFAULT_POIDS_HORS_SERVICE), key='poids_hsv')
                st.session_state.ss_poids_fact_double = st.slider("Poids: Facturation Double", 1, 15, st.session_state.get('ss_poids_fact_double', DEFAULT_POIDS_FACT_DOUBLE), key='poids_dbl')

    with tab_geoloc:
        with st.expander("Paramètres Généraux de Géolocalisation", expanded=True):
            st.session_state.ss_rayon_station_km = st.number_input(
                "Rayon autour station (km) - pour correspondance transaction/présence", min_value=0.1, max_value=1.0,
                value=float(st.session_state.get('ss_rayon_station_km', DEFAULT_RAYON_STATION_KM)),
                step=0.1, format="%.1f", key='param_rayon_station'
            )
            st.session_state.ss_seuil_arret_minutes = st.number_input( # Actuellement non utilisé car Type=Arrêt vient du fichier
                "Durée minimale d'un arrêt (minutes) - si calculé par l'app", min_value=1, max_value=30,
                value=st.session_state.get('ss_seuil_arret_minutes', DEFAULT_SEUIL_ARRET_MINUTES),
                step=1, key='param_seuil_arret',
                disabled=True # Le Type=Arrêt vient du fichier donc ce seuil n'est pas utilisé pour le moment
            )
            st.session_state.ss_seuil_detour_pct = st.slider(
                "Seuil écart vitesse pour détour suspect (%)", min_value=5, max_value=50,
                value=st.session_state.get('ss_seuil_detour_pct', DEFAULT_SEUIL_DETOUR_PCT),
                step=5, key='param_seuil_detour',
                help="Un trajet est un détour suspect si sa vitesse moyenne est X% inférieure à la vitesse moyenne habituelle du véhicule pour des trajets significatifs."
            )
            st.session_state.ss_nb_arrets_suspect = st.slider( # Actuellement non utilisé
                "Nombre d'arrêts Type='Arrêt' suspect pour un trajet court", min_value=2, max_value=10,
                value=st.session_state.get('ss_nb_arrets_suspect', DEFAULT_NB_ARRETS_SUSPECT),
                step=1, key='param_nb_arrets_suspect',
                disabled=True # Logique non implémentée
            )

        with st.expander("Heures de Service (Géolocalisation)"):
            st.session_state.ss_heure_debut_service = st.slider(
                "Heure Début Service Normal (Géoloc)", min_value=5, max_value=9,
                value=st.session_state.get('ss_heure_debut_service', DEFAULT_HEURE_DEBUT_SERVICE),
                step=1, key='param_heure_debut_service_geoloc'
            )
            st.session_state.ss_heure_fin_service = st.slider(
                "Heure Fin Service Normal (Géoloc)", min_value=16, max_value=22,
                value=st.session_state.get('ss_heure_fin_service', DEFAULT_HEURE_FIN_SERVICE),
                step=1, key='param_heure_fin_service_geoloc'
            )
            st.caption(f"Plage de service normal (géoloc) actuelle: de {st.session_state.ss_heure_debut_service}h à {st.session_state.ss_heure_fin_service}h (hors weekend).")

        with st.expander("Poids des Anomalies de Géolocalisation"):
            st.caption("Ajustez l'importance de chaque type d'anomalie de géolocalisation dans le calcul du score de risque.")
            c1_geo, c2_geo = st.columns(2)
            with c1_geo:
                st.session_state.ss_poids_trajet_hors_heures = st.slider(
                    "Poids: Trajet Hors Heures (Géoloc)", 1, 15,
                    st.session_state.get('ss_poids_trajet_hors_heures', DEFAULT_POIDS_TRAJET_HORS_HEURES),
                    key='poids_trajet_hors_heures_geoloc'
                )
                st.session_state.ss_poids_trajet_weekend = st.slider(
                    "Poids: Trajet Weekend (Géoloc)", 1, 15,
                    st.session_state.get('ss_poids_trajet_weekend', DEFAULT_POIDS_TRAJET_WEEKEND),
                    key='poids_trajet_weekend_geoloc'
                )
                st.session_state.ss_poids_arrets_frequents = st.slider(
                    "Poids: Vitesse Lente (Arrêts Fréquents non déclarés)", 1, 15,
                    st.session_state.get('ss_poids_arrets_frequents', DEFAULT_POIDS_ARRETS_FREQUENTS),
                    key='poids_arrets_frequents_geoloc'
                )
            with c2_geo:
                st.session_state.ss_poids_detour_suspect = st.slider(
                    "Poids: Détour Suspect (Géoloc)", 1, 15,
                    st.session_state.get('ss_poids_detour_suspect', DEFAULT_POIDS_DETOUR_SUSPECT),
                    key='poids_detour_suspect_geoloc'
                )
                st.session_state.ss_poids_transaction_sans_presence = st.slider(
                    "Poids: Transaction Sans Présence (Géoloc)", 1, 15,
                    st.session_state.get('ss_poids_transaction_sans_presence', DEFAULT_POIDS_TRANSACTION_SANS_PRESENCE),
                    key='poids_transaction_sans_presence_geoloc'
                )
                st.session_state.ss_poids_vitesse_excessive = st.slider(
                    "Poids: Vitesse Excessive (Géoloc)", 1, 15,
                    st.session_state.get('ss_poids_vitesse_excessive', DEFAULT_POIDS_VITESSE_EXCESSIVE),
                    key='poids_vitesse_excessive_geoloc'
                )

    st.markdown("---")
    st.info("Les paramètres sont sauvegardés automatiquement pendant la session.")

def afficher_page_analyse_vehicules(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date, kpi_cat: pd.DataFrame):
    """Affiche la page d'analyse détaillée des véhicules."""
    st.header(f"🚗 Analyse Détaillée des Véhicules ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return

    # Filtre par véhicule
    all_vehicles = sorted(df_vehicules['Nouveau Immat'].dropna().unique())
    selected_vehicle = st.selectbox("Sélectionner un véhicule", ["Tous"] + all_vehicles, key="select_veh_analyse")


    if selected_vehicle != "Tous":
        # Obtenir les informations du véhicule
        vehicle_info_series = df_vehicules[df_vehicules['Nouveau Immat'] == selected_vehicle]
        if vehicle_info_series.empty:
            st.error(f"Information non trouvée pour le véhicule {selected_vehicle}")
            return
        vehicle_info = vehicle_info_series.iloc[0]

        vehicle_card = vehicle_info['N° Carte']

        # Obtenir les transactions du véhicule
        mask_card = df_transactions['Card num.'] == vehicle_card
        vehicle_transactions = df_transactions[mask_card]

        st.subheader(f"Détails du Véhicule: {selected_vehicle}")

        # Informations de base
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Immatriculation", selected_vehicle)
            st.metric("Catégorie", vehicle_info.get('Catégorie', 'Non définie'))
        with col2:
            st.metric("Numéro de Carte", vehicle_card)
            st.metric("Type", vehicle_info.get('Type', 'Non défini'))
        with col3:
            st.metric("Capacité Réservoir", f"{vehicle_info.get('Cap-rèservoir', 0)} L")
            if 'Dotation' in vehicle_info:
                st.metric("Dotation Mensuelle", f"{vehicle_info.get('Dotation', 0)} L")

        # Statistiques des transactions
        if not vehicle_transactions.empty:
            total_volume_veh = vehicle_transactions['Quantity'].sum() # Renommé
            total_amount_veh = vehicle_transactions['Amount'].sum() # Renommé
            nb_transactions_veh = len(vehicle_transactions) # Renommé

            st.subheader("Statistiques de Consommation")
            col4, col5, col6 = st.columns(3)
            col4.metric("Volume Total", f"{total_volume_veh:.1f} L")
            col5.metric("Montant Total", f"{total_amount_veh:,.0f} CFA")
            col6.metric("Nombre de Transactions", f"{nb_transactions_veh}")

            # Afficher les transactions
            st.subheader("Transactions du Véhicule")
            afficher_dataframe_avec_export(
                vehicle_transactions,
                "Transactions du Véhicule",
                key=f"vehicle_transactions_{selected_vehicle.replace(' ', '_')}"
            )

            # Graphique d'évolution de la consommation
            st.subheader("Évolution de la Consommation")
            # Utiliser 'Date' qui est déjà datetime, pas besoin de créer 'date'
            daily_consumption = vehicle_transactions.groupby(vehicle_transactions['Date'].dt.date)['Quantity'].sum().reset_index()


            fig = px.line(
                daily_consumption,
                x='Date', # 'Date' est déjà la colonne de date
                y='Quantity',
                title="Évolution de la Consommation Quotidienne",
                labels={'Date': 'Date', 'Quantity': 'Volume (L)'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"Aucune transaction trouvée pour le véhicule {selected_vehicle} durant la période sélectionnée.")
    else:
        # Vue globale de tous les véhicules
        st.subheader("Vue d'Ensemble des Véhicules")

        # Afficher les KPIs par catégorie si disponibles
        if not kpi_cat.empty:
            st.write("Consommation moyenne par catégorie de véhicule:")
            afficher_dataframe_avec_export(kpi_cat, "KPIs par Catégorie (Vue Globale)", key="kpi_cat_overview_page")


            # Graphique des consommations par catégorie
            fig_cat_kpi = px.bar( # Renommé
                kpi_cat,
                x='Catégorie',
                y=['consommation_moyenne', 'consommation_globale'],
                title="Consommation par Catégorie de Véhicule",
                barmode='group',
                labels={
                    'value': 'Consommation (L/100km)',
                    'variable': 'Type de Consommation'
                }
            )
            st.plotly_chart(fig_cat_kpi, use_container_width=True)

        # Liste de tous les véhicules et leurs statistiques
        merged_df_all_veh = df_transactions.merge( # Renommé
            df_vehicules[['N° Carte', 'Nouveau Immat', 'Catégorie']],
            left_on='Card num.',
            right_on='N° Carte',
            how='inner'
        )

        vehicle_stats_all_veh = merged_df_all_veh.groupby(['Nouveau Immat', 'Catégorie']).agg( # Renommé
            Volume_Total=('Quantity', 'sum'),
            Montant_Total=('Amount', 'sum'),
            Nb_Transactions=('Quantity', 'count')
        ).reset_index()

        st.subheader("Statistiques par Véhicule (Toutes Transactions)")
        afficher_dataframe_avec_export(
            vehicle_stats_all_veh.sort_values('Volume_Total', ascending=False),
            "Statistiques par Véhicule (Global)",
            key="vehicle_stats_all_page"
        )

def afficher_page_analyse_couts(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page d'analyse des coûts."""
    st.header(f"💰 Analyse des Coûts ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")

    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return
    # Statistiques globales
    total_volume = df_transactions['Quantity'].sum()
    total_amount = df_transactions['Amount'].sum()
    avg_price_per_liter = total_amount / total_volume if total_volume > 0 else 0
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Volume Total", f"{total_volume:.1f} L")
    col2.metric("Coût Total", f"{total_amount:,.0f} CFA")
    col3.metric("Prix Moyen / Litre", f"{avg_price_per_liter:.0f} CFA/L")
    
    # Fusion avec les données des véhicules
    merged_df = df_transactions.merge(
        df_vehicules[['N° Carte', 'Nouveau Immat', 'Catégorie']],
        left_on='Card num.',
        right_on='N° Carte',
        how='inner'
    )
    
    # Analyse par mois
    st.subheader("Évolution Mensuelle des Coûts")
    monthly_costs = merged_df.groupby(pd.Grouper(key='DateTime', freq='M')).agg(
        Volume=('Quantity', 'sum'),
        Montant=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count')
    ).reset_index()
    
    monthly_costs['Mois'] = monthly_costs['DateTime'].dt.strftime('%Y-%m')
    monthly_costs['Prix_Moyen_Litre'] = monthly_costs['Montant'] / monthly_costs['Volume']
    
    fig = px.line(
        monthly_costs, 
        x='Mois', 
        y=['Volume', 'Montant'],
        title="Évolution Mensuelle du Volume et du Coût",
        labels={'value': 'Valeur', 'variable': 'Métrique'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    fig_price = px.line(
        monthly_costs, 
        x='Mois', 
        y='Prix_Moyen_Litre',
        title="Évolution du Prix Moyen par Litre",
        labels={'Prix_Moyen_Litre': 'Prix (CFA/L)'}
    )
    st.plotly_chart(fig_price, use_container_width=True)
    
    # Analyse par station
    st.subheader("Analyse par Station")
    station_costs = merged_df.groupby('Place').agg(
        Volume_Total=('Quantity', 'sum'),
        Montant_Total=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count'),
        Prix_Moyen=('Amount', lambda x: (x.sum() / merged_df.loc[x.index, 'Quantity'].sum()) if merged_df.loc[x.index, 'Quantity'].sum() > 0 else 0)
    ).reset_index()
    
    station_costs = station_costs.sort_values('Montant_Total', ascending=False)
    
    # Afficher le tableau des coûts par station
    afficher_dataframe_avec_export(
        station_costs, 
        "Coûts par Station", 
        key="station_costs"
    )
    
    # Graphique des coûts par station (top 10)
    top_stations = station_costs.head(10)
    fig_stations = px.bar(
        top_stations,
        x='Place',
        y='Montant_Total',
        title="Top 10 des Stations par Coût Total",
        labels={'Montant_Total': 'Montant Total (CFA)'}
    )
    st.plotly_chart(fig_stations, use_container_width=True)
    
    # Analyse par catégorie de véhicule
    st.subheader("Analyse par Catégorie de Véhicule")
    category_costs = merged_df.groupby('Catégorie').agg(
        Volume_Total=('Quantity', 'sum'),
        Montant_Total=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count'),
        Nb_Vehicules=('Nouveau Immat', 'nunique')
    ).reset_index()
    
    category_costs['Prix_Moyen_Litre'] = category_costs['Montant_Total'] / category_costs['Volume_Total']
    category_costs['Cout_Moyen_Transaction'] = category_costs['Montant_Total'] / category_costs['Nb_Transactions']
    
    # Afficher le tableau des coûts par catégorie
    afficher_dataframe_avec_export(
        category_costs, 
        "Coûts par Catégorie", 
        key="category_costs"
    )
    
    # Graphique des coûts par catégorie
    fig_category = px.pie(
        category_costs,
        values='Montant_Total',
        names='Catégorie',
        title="Répartition des Coûts par Catégorie"
    )
    st.plotly_chart(fig_category, use_container_width=True)

def afficher_page_kpi(df_transactions: pd.DataFrame, df_vehicules: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page des indicateurs clés de performance (KPIs)."""
    st.header(f"📊 Indicateurs Clés de Performance ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")
    
    if df_transactions.empty:
        st.warning("Aucune transaction à analyser pour la période sélectionnée.")
        return
    
    # Calculer les KPIs globaux
    total_volume = df_transactions['Quantity'].sum()
    total_amount = df_transactions['Amount'].sum()
    nb_transactions = len(df_transactions)
    nb_active_cards = df_transactions['Card num.'].nunique()
    
    # Fusion avec les données des véhicules
    merged_df = df_transactions.merge(
        df_vehicules[['N° Carte', 'Nouveau Immat', 'Catégorie']],
        left_on='Card num.',
        right_on='N° Carte',
        how='inner'
    )
    
    nb_active_vehicles = merged_df['Nouveau Immat'].nunique()
    avg_price_per_liter = total_amount / total_volume if total_volume > 0 else 0
    
    # Calcul des KPIs de consommation
    merged_df['distance_parcourue'] = merged_df['Current mileage'] - merged_df['Past mileage']
    valid_distance = merged_df.loc[merged_df['distance_parcourue'] > 0, 'distance_parcourue'].sum()
    avg_consumption_100km = (total_volume / valid_distance) * 100 if valid_distance > 0 else 0
    avg_cost_per_km = total_amount / valid_distance if valid_distance > 0 else 0
    
    # Affichage des KPIs principaux
    st.subheader("KPIs Globaux")
    col1, col2, col3 = st.columns(3)
    col1.metric("Volume Total", f"{total_volume:.1f} L")
    col1.metric("Coût Total", f"{total_amount:,.0f} CFA")
    
    col2.metric("Nombre de Transactions", f"{nb_transactions}")
    col2.metric("Véhicules Actifs", f"{nb_active_vehicles}")
    
    col3.metric("Prix Moyen / Litre", f"{avg_price_per_liter:.0f} CFA/L")
    col3.metric("Coût Moyen / Km", f"{avg_cost_per_km:.1f} CFA/km")
    
    # KPIs par catégorie de véhicule
    st.subheader("KPIs par Catégorie de Véhicule")
    category_kpis = merged_df.groupby('Catégorie').agg(
        Volume_Total=('Quantity', 'sum'),
        Montant_Total=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count'),
        Nb_Vehicules=('Nouveau Immat', 'nunique'),
        Distance_Totale=('distance_parcourue', lambda x: x[x > 0].sum())
    ).reset_index()
    
    category_kpis['Consommation_100km'] = category_kpis.apply(
        lambda row: (row['Volume_Total'] / row['Distance_Totale']) * 100 if row['Distance_Totale'] > 0 else 0, 
        axis=1
    )
    
    category_kpis['Cout_km'] = category_kpis.apply(
        lambda row: row['Montant_Total'] / row['Distance_Totale'] if row['Distance_Totale'] > 0 else 0, 
        axis=1
    )
    
    # Afficher le tableau des KPIs par catégorie
    afficher_dataframe_avec_export(
        category_kpis, 
        "KPIs par Catégorie", 
        key="category_kpis"
    )
    
    # Graphiques des KPIs
    col4, col5 = st.columns(2)
    
    with col4:
        fig_consumption = px.bar(
            category_kpis,
            x='Catégorie',
            y='Consommation_100km',
            title="Consommation par Catégorie (L/100km)",
            labels={'Consommation_100km': 'L/100km'}
        )
        st.plotly_chart(fig_consumption, use_container_width=True)
    
    with col5:
        fig_cost = px.bar(
            category_kpis,
            x='Catégorie',
            y='Cout_km',
            title="Coût par Km par Catégorie",
            labels={'Cout_km': 'CFA/km'}
        )
        st.plotly_chart(fig_cost, use_container_width=True)
    
    # KPIs mensuels
    st.subheader("Évolution Mensuelle des KPIs")
    monthly_kpis = merged_df.groupby(pd.Grouper(key='DateTime', freq='M')).agg(
        Volume=('Quantity', 'sum'),
        Montant=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count'),
        Distance=('distance_parcourue', lambda x: x[x > 0].sum())
    ).reset_index()
    
    monthly_kpis['Mois'] = monthly_kpis['DateTime'].dt.strftime('%Y-%m')
    monthly_kpis['Consommation_100km'] = monthly_kpis.apply(
        lambda row: (row['Volume'] / row['Distance']) * 100 if row['Distance'] > 0 else 0, 
        axis=1
    )
    
    fig_monthly = px.line(
        monthly_kpis, 
        x='Mois', 
        y='Consommation_100km',
        title="Évolution Mensuelle de la Consommation (L/100km)",
        labels={'Consommation_100km': 'L/100km'}
    )
    st.plotly_chart(fig_monthly, use_container_width=True)

def afficher_page_autres_cartes(df_transactions: pd.DataFrame, df_autres: pd.DataFrame, date_debut: datetime.date, date_fin: datetime.date):
    """Affiche la page d'analyse des autres types de cartes (non-véhicules)."""
    st.header(f"💳 Analyse des Autres Cartes ({date_debut.strftime('%d/%m/%Y')} - {date_fin.strftime('%d/%m/%Y')})")
    
    if df_transactions.empty or df_autres.empty:
        st.warning("Aucune transaction ou données de cartes à analyser pour la période sélectionnée.")
        return
    
    # Filtrer les transactions des autres cartes
    autres_cartes_nums = set(df_autres['N° Carte'])
    autres_transactions = df_transactions[df_transactions['Card num.'].isin(autres_cartes_nums)]
    
    if autres_transactions.empty:
        st.info("Aucune transaction pour les autres types de cartes durant la période sélectionnée.")
        return
    
    # Statistiques globales
    total_volume = autres_transactions['Quantity'].sum()
    total_amount = autres_transactions['Amount'].sum()
    nb_transactions = len(autres_transactions)
    nb_active_cards = autres_transactions['Card num.'].nunique()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Volume Total", f"{total_volume:.1f} L")
    col2.metric("Coût Total", f"{total_amount:,.0f} CFA")
    col3.metric("Cartes Actives", f"{nb_active_cards}/{len(df_autres)}")
    
    # Fusion avec les données des autres cartes
    merged_df = autres_transactions.merge(
        df_autres[['N° Carte']],
        left_on='Card num.',
        right_on='N° Carte',
        how='inner'
    )
    
    # Analyse par carte
    st.subheader("Consommation par Carte")
    
    carte_stats = merged_df.groupby('Card num.').agg(
        Volume_Total=('Quantity', 'sum'),
        Montant_Total=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count')
    ).reset_index()
    
    if not carte_stats.empty:
        carte_stats = carte_stats.sort_values('Volume_Total', ascending=False)
        
        # Afficher le tableau des statistiques par carte
        afficher_dataframe_avec_export(
            carte_stats, 
            "Statistiques par Carte", 
            key="autres_cartes_stats"
        )
        
        # Graphique des volumes par carte (top 10)
        top_cartes = carte_stats.head(10)
        fig_cartes = px.bar(
            top_cartes,
            x='Card num.',
            y='Volume_Total',
            title="Top 10 des Autres Cartes par Volume",
            labels={'Volume_Total': 'Volume Total (L)'}
        )
        st.plotly_chart(fig_cartes, use_container_width=True)
    
    # Analyse par mois
    st.subheader("Évolution Mensuelle")
    monthly_stats = merged_df.groupby(pd.Grouper(key='DateTime', freq='M')).agg(
        Volume=('Quantity', 'sum'),
        Montant=('Amount', 'sum'),
        Nb_Transactions=('Quantity', 'count')
    ).reset_index()
    
    monthly_stats['Mois'] = monthly_stats['DateTime'].dt.strftime('%Y-%m')
    
    fig_monthly = px.line(
        monthly_stats, 
        x='Mois', 
        y=['Volume', 'Montant'],
        title="Évolution Mensuelle du Volume et du Coût (Autres Cartes)",
        labels={'value': 'Valeur', 'variable': 'Métrique'}
    )
    st.plotly_chart(fig_monthly, use_container_width=True)
    
    # Transactions individuelles
    st.subheader("Détail des Transactions")
    afficher_dataframe_avec_export(
        autres_transactions, 
        "Transactions des Autres Cartes", 
        key="autres_cartes_transactions"
    )
# ---------------------------------------------------------------------
# Point d'entrée avec navigation mise à jour
# ---------------------------------------------------------------------
def main():
    st.title("📊 Gestion & Analyse Cartes Carburant")

    st.sidebar.header("1. Chargement des Données")
    fichier_transactions = st.sidebar.file_uploader("Fichier Transactions (CSV)", type=['csv'])
    fichier_cartes = st.sidebar.file_uploader("Fichier Cartes (Excel)", type=['xlsx', 'xls'])
    
    # Ajout du chargement optionnel du fichier de géolocalisation
    fichier_geoloc = st.sidebar.file_uploader("Fichier Géolocalisation (Excel, optionnel)", type=['xlsx', 'xls'])

    if not fichier_transactions or not fichier_cartes:
        st.info("👋 Bienvenue ! Veuillez charger le fichier des transactions (CSV) et le fichier des cartes (Excel) via la barre latérale pour commencer.")
        initialize_session_state() 
        if st.sidebar.radio("Navigation", ["Paramètres"], index=0, key="nav_no_data") == "Paramètres":
            afficher_page_parametres()
        return

    df_transactions, df_vehicules, df_ge, df_autres = charger_donnees(fichier_transactions, fichier_cartes)
    
    # Chargement des données de géolocalisation (optionnel)
    df_geoloc = None
    if fichier_geoloc is not None:
        with st.spinner("Chargement des données de géolocalisation..."):
            df_geoloc = charger_donnees_geolocalisation(fichier_geoloc)
            if df_geoloc is not None:
                st.sidebar.success("✅ Données de géolocalisation chargées avec succès !")
                st.sidebar.markdown(f"**Trajets géolocalisés :** {len(df_geoloc):,}")
                if 'Date' in df_geoloc.columns:
                    min_date_geo = df_geoloc['Date'].min()
                    max_date_geo = df_geoloc['Date'].max()
                    st.sidebar.markdown(f"**Période géoloc :** {min_date_geo.strftime('%d/%m/%Y')} - {max_date_geo.strftime('%d/%m/%Y')}")

    if df_transactions is None or df_vehicules is None or df_ge is None or df_autres is None:
        st.error("❌ Erreur lors du chargement ou de la validation des fichiers principaux. Veuillez vérifier les fichiers et les colonnes requises.")
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
        "Analyse par Période", "Suivi des Dotations", "Anomalies", "KPIs", "Autres Cartes"
    ]
    
    # Ajouter la page de géolocalisation si le fichier est chargé
    if df_geoloc is not None:
        pages.append("Géolocalisation")
        
    pages.append("Paramètres")  # Toujours en dernier
    
    # Laisser toutes les pages accessibles même si df_transac_filtered est vide, les pages géreront l'affichage.
    page = st.sidebar.radio("Choisir une page :", pages, key="navigation_main")

    if page == "Tableau de Bord":
        kpi_cat_dashboard, df_vehicle_kpi_dashboard = calculer_kpis_globaux(
            df_transac_filtered, df_vehicules, global_date_debut, global_date_fin,
            list(st.session_state.ss_conso_seuils_par_categorie.keys()) 
        )
        afficher_page_dashboard(df_transac_filtered, df_vehicules, df_ge, df_autres, global_date_debut, global_date_fin, df_geoloc)
        ameliorer_dashboard(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin, 
                        kpi_cat_dashboard, df_vehicle_kpi_dashboard)
    elif page == "Analyse Véhicules":
         kpi_cat_veh_page, _ = calculer_kpis_globaux(
             df_transac_filtered, df_vehicules, global_date_debut, global_date_fin,
             list(st.session_state.ss_conso_seuils_par_categorie.keys()) 
         )
         afficher_page_analyse_vehicules(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin, kpi_cat_veh_page)
    elif page == "Analyse des Coûts":
         afficher_page_analyse_couts(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Analyse par Période":
         afficher_page_analyse_periodes(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Suivi des Dotations":
         afficher_page_suivi_dotations(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Anomalies":
        afficher_page_anomalies(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin, df_geoloc)
    elif page == "KPIs":
        afficher_page_kpi(df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Autres Cartes":
        afficher_page_autres_cartes(df_transac_filtered, df_autres, global_date_debut, global_date_fin)
    elif page == "Géolocalisation" and df_geoloc is not None:
        # Nouvelle page d'analyse de géolocalisation
        afficher_page_analyse_geolocalisation(df_geoloc, df_transac_filtered, df_vehicules, global_date_debut, global_date_fin)
    elif page == "Paramètres":
        afficher_page_parametres(df_vehicules)

if __name__ == "__main__":
    main()
