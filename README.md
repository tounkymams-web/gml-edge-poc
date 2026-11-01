# POC Edge Computing Télématique - GreenMove Logistics (GML)

Ce dépôt contient la Preuve de Concept (POC) du moteur autonome embarqué sur tablette durcie **Zebra ET40** pour la flotte de 50 camions de **GreenMove Logistics**.

## Architecture et Fonctionnalités
1. **Module 1 - Moteur ZFE (`Geo`)** : Détection d'entrée en Zone à Faible Émission 100 % hors-ligne. Combine un pré-filtrage par **Bounding Box** en O(1) et l'algorithme du **Ray Casting** en O(N).
2. **Module 2 - Boîte Noire Télématique (`Safety`)** : Lecture en flux continu (*Streaming* en O(1) RAM) des données de l'accéléromètre à 100 Hz, filtrage du bruit de la route et détection des freinages violents (`acc_y < -2.5 m/s²`). Génère le rapport léger `daily_score.json` pour l'assureur *AssurezMoi*.

## Prérequis
* **Python 3.8+** (Aucune bibliothèque externe lourde type Pandas/NumPy n'est requise afin de respecter la limite stricte des **150 Mo de RAM** de la tablette Zebra ET40).

## Procédure de test en local
1. Cloner ce dépôt ou extraire l'archive :
   ```bash
   git clone https://github.com/tounkymams-web/gml-edge-poc.git
   cd gml-edge-poc
   ```
2. Exécuter le moteur autonome :
   ```bash
   python3 main.py
   ```
3. Vérifier les logs `[ALERT ZFE]` et `[ALERT SAFETY]` dans le terminal ainsi que le fichier `daily_score.json` généré automatiquement.
