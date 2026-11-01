#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
POC GreenMove Logistics (GML) - Moteur Autonome Edge Computing
Module 1 : Conformité ZFE (Bounding Box O(1) + Ray Casting O(N))
Module 2 : Boîte Noire Safety (Streaming CSV O(1) RAM + Filtrage Seuil < -2.5 m/s²)
Contraintes cibles : Zebra ET40 (Snapdragon 660, < 150 Mo RAM, complexité O(N) max)
"""

import json
import csv
import os
from datetime import datetime, timezone


def compute_bounding_box(polygon):
    """Pré-calcule la boîte englobante (Bounding Box) du polygone en O(N)."""
    if not polygon:
        raise ValueError("Le polygone ZFE est vide.")
    lats = [pt["lat"] for pt in polygon]
    lons = [pt["lon"] for pt in polygon]
    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons)
    }


def is_in_bounding_box(lat, lon, bbox):
    """Vérifie en temps constant O(1) si le camion est dans la boîte englobante."""
    return (bbox["min_lat"] <= lat <= bbox["max_lat"] and
            bbox["min_lon"] <= lon <= bbox["max_lon"])


def distance_point_to_segment(px, py, x1, y1, x2, y2):
    """Calcule la distance euclidienne entre un point et un segment (pour tolérance bordure GPS)."""
    dx, dy = x2 - x1, y2 - y1
    if dx == 0 and dy == 0:
        return ((px - x1) ** 2 + (py - y1) ** 2) ** 0.5
    t = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
    proj_x, proj_y = x1 + t * dx, y1 + t * dy
    return ((px - proj_x) ** 2 + (py - proj_y) ** 2) ** 0.5


def point_in_polygon_ray_casting(lat, lon, polygon, bbox, border_tolerance_deg=0.0045):
    """
    Détermine si un point GPS (lat, lon) est dans le polygone ZFE ou sur sa bordure immédiate.
    Étape 1 : Filtre Bounding Box en O(1).
    Étape 2 : Algorithme du Ray Casting en O(N) (sans aucune boucle imbriquée).
    """
    if not is_in_bounding_box(lat, lon, bbox):
        return False, "OUT (Rejet rapide Bounding Box O(1))"

    inside = False
    n = len(polygon)
    near_border = False

    for i in range(n - 1):
        lat1, lon1 = polygon[i]["lat"], polygon[i]["lon"]
        lat2, lon2 = polygon[i + 1]["lat"], polygon[i + 1]["lon"]

        # Vérification de la bordure ZFE (tolérance ~450m pour sécuriser l'approche à 90 km/h)
        if distance_point_to_segment(lon, lat, lon1, lat1, lon2, lat2) <= border_tolerance_deg:
            near_border = True

        if (lat1 > lat) != (lat2 > lat):
            intersect_lon = (lon2 - lon1) * (lat - lat1) / (lat2 - lat1 + 1e-15) + lon1
            if lon <= intersect_lon:
                inside = not inside

    if inside:
        return True, "IN (Intérieur ZFE - Ray Casting O(N))"
    if near_border:
        return True, "IN (Bordure ZFE détectée - Ray Casting O(N))"
    return False, "OUT (Dans BBox mais hors Polygone)"


def run_zfe_module(polygon_file, gps_file):
    """Exécute le Module 1 (ZFE) avec gestion complète des exceptions."""
    print("=" * 78)
    print(" DÉMARRAGE MODULE 1 : MOTEUR GÉOGRAPHIQUE ZFE (EDGE COMPUTING)")
    print("=" * 78)

    try:
        with open(polygon_file, mode="r", encoding="utf-8") as f:
            polygon = json.load(f)
        with open(gps_file, mode="r", encoding="utf-8") as f:
            gps_trace = json.load(f)
    except Exception as e:
        print(f"[ERREUR CRITIQUE] Chargement des fichiers JSON impossible : {e}")
        return []

    bbox = compute_bounding_box(polygon)
    print(f"[INIT] Polygone ZFE Lyon chargé ({len(polygon)} sommets).")
    print(f"[INIT] Bounding Box O(1) : Lat [{bbox['min_lat']} .. {bbox['max_lat']}], "
          f"Lon [{bbox['min_lon']} .. {bbox['max_lon']}]\n")

    alerts = []
    for point in gps_trace:
        try:
            pt_id = point.get("id", "?")
            ts = point.get("timestamp", "N/A")
            lat = float(point["lat"])
            lon = float(point["lon"])
            expected = point.get("expected", "N/A")

            is_in_zfe, detail = point_in_polygon_ray_casting(lat, lon, polygon, bbox)
            if is_in_zfe:
                print(f"[ALERT ZFE] Point #{pt_id} ({ts}) | Pos=({lat:.4f}, {lon:.4f}) "
                      f"-> {detail} | Attendu: {expected}")
                alerts.append({"id": pt_id, "timestamp": ts, "lat": lat, "lon": lon, "detail": detail})
            else:
                print(f"[INFO GPS]  Point #{pt_id} ({ts}) | Pos=({lat:.4f}, {lon:.4f}) "
                      f"-> {detail} | Attendu: {expected}")
        except (KeyError, ValueError, TypeError) as err:
            print(f"[WARN GPS]  Trame GPS corrompue ignorée : {point} ({err})")
            continue

    print(f"\n[BILAN ZFE] {len(alerts)} alerte(s) [ALERT ZFE] déclenchée(s) sur {len(gps_trace)} points.\n")
    return alerts


def run_safety_module(csv_file, output_json_file, threshold=-2.5):
    """Lit accelerometer_data.csv en streaming O(1) RAM et isole les freinages < -2.5 m/s²."""
    print("=" * 78)
    print(" DÉMARRAGE MODULE 2 : BOÎTE NOIRE TÉLÉMATIQUE SAFETY (STREAMING 100 Hz)")
    print("=" * 78)

    total_samples = 0
    corrupted_samples = 0
    harsh_braking_events = []
    min_acc_y = 0.0

    try:
        with open(csv_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_idx, row in enumerate(reader, start=1):
                try:
                    ts = int(row["timestamp"])
                    acc_x = float(row["acc_x"])
                    acc_y = float(row["acc_y"])
                    acc_z = float(row["acc_z"])

                    total_samples += 1
                    if acc_y < min_acc_y:
                        min_acc_y = acc_y

                    if acc_y < threshold:
                        event = {
                            "line_number": row_idx,
                            "timestamp": ts,
                            "acc_x": acc_x,
                            "acc_y": acc_y,
                            "acc_z": acc_z,
                            "severity": "CRITICAL" if acc_y < -3.0 else "HIGH",
                            "event_type": "HARSH_BRAKING"
                        }
                        harsh_braking_events.append(event)
                        print(f"[ALERT SAFETY] Freinage violent isolé ! Ligne #{row_idx:02d} "
                              f"(TS: {ts}) | acc_y = {acc_y:+.2f} m/s² (< {threshold} m/s²)")
                    else:
                        print(f"[BRUIT FILTRÉ] Ligne #{row_idx:02d} (TS: {ts}) | "
                              f"acc_y = {acc_y:+.2f} m/s² -> Conduite normale")
                except (KeyError, ValueError, TypeError) as row_err:
                    corrupted_samples += 1
                    print(f"[WARN CSV]     Ligne #{row_idx:02d} corrompue ignorée : {row} ({row_err})")
                    continue
    except Exception as e:
        print(f"[ERREUR CRITIQUE CSV] Impossible de lire {csv_file} : {e}")
        return None

    safety_score = max(0, 100 - (len(harsh_braking_events) * 15))
    daily_report = {
        "vehicle_id": "GML-TRUCK-042",
        "device_model": "Zebra-ET40-Edge",
        "date_generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "telemetry_summary": {
            "total_samples_processed": total_samples,
            "corrupted_samples_ignored": corrupted_samples,
            "threshold_applied_ms2": threshold,
            "max_deceleration_recorded_ms2": min_acc_y
        },
        "safety_evaluation": {
            "harsh_braking_count": len(harsh_braking_events),
            "daily_safety_score": safety_score,
            "max_score": 100,
            "grade": "A" if safety_score >= 85 else "B",
            "assurez_moi_bonus_20pct_eligible": safety_score >= 80
        },
        "detected_anomalies": harsh_braking_events
    }

    with open(output_json_file, mode="w", encoding="utf-8") as out_f:
        json.dump(daily_report, out_f, indent=4, ensure_ascii=False)

    print(f"\n[SUCCÈS] Fichier '{output_json_file}' généré (Score : {safety_score}/100).")
    return daily_report


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    run_zfe_module(
        os.path.join(base_dir, "lyon_polygon.json"),
        os.path.join(base_dir, "truck_gps.json")
    )
    run_safety_module(
        os.path.join(base_dir, "accelerometer_data.csv"),
        os.path.join(base_dir, "daily_score.json"),
        threshold=-2.5
    )
