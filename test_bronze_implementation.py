#!/usr/bin/env python3
"""
Script de prueba para verificar la implementación de Bronze layer
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from sqlalchemy import create_engine, text

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bronze_functions():
    """Prueba las funciones de validación y limpieza"""

    # Cargar datos de ejemplo
    csv_path = 'include/raw/modis-fire-two-months.csv'
    df = pd.read_csv(csv_path)

    logger.info(f"Datos cargados: {len(df)} registros")

    # Simular validaciones Bronze
    valid_mask = (
        (df['frp'] > 0) &
        (df['confidence'] > 30) &
        (df['latitude'].notna()) &
        (df['longitude'].notna()) &
        (df['acq_date'].notna())
    )

    df_valid = df[valid_mask].copy()
    df_rejected = df[~valid_mask].copy()

    # Agregar metadata
    df_valid['ingested_at'] = datetime.now()
    df_valid['validation_status'] = 'valid'
    df_valid['source_file'] = 'modis-fire-two-months.csv'
    df_valid['processing_date'] = datetime.now().date()

    # Calcular data quality score
    df_valid['data_quality_score'] = np.where(
        df_valid['confidence'] >= 80, 100,
        np.where(df_valid['confidence'] >= 50, 75, 50)
    )

    logger.info("=== RESULTADOS DE VALIDACIÓN ===")
    logger.info(f"Registros totales: {len(df)}")
    logger.info(f"Registros válidos: {len(df_valid)}")
    logger.info(f"Registros rechazados: {len(df_rejected)}")
    logger.info(f"Tasa de éxito: {len(df_valid)/len(df):.2%}")

    if len(df_rejected) > 0:
        logger.info("Motivos de rechazo:")
        rejection_counts = df_rejected['confidence'].apply(
            lambda x: 'confidence <= 30' if x <= 30 else 'otro'
        ).value_counts()
        try:
            logger.info(f"  - confidence <= 30: {rejection_counts['confidence <= 30']}")
        except KeyError:
            logger.info(f"  - confidence <= 30: 0")
        logger.info(f"  - frp <= 0: {len(df_rejected[df_rejected['frp'] <= 0])}")
        logger.info(f"  - datos nulos: {len(df_rejected[df_rejected['latitude'].isna() | df_rejected['longitude'].isna()])}")

    # Verificar estructura Bronze
    expected_columns = [
        'latitude', 'longitude', 'brightness', 'scan', 'track',
        'acq_date', 'acq_time', 'satellite', 'instrument', 'confidence',
        'version', 'bright_t31', 'frp', 'daynight',
        'ingested_at', 'validation_status', 'source_file', 'processing_date', 'data_quality_score'
    ]

    logger.info("=== ESTRUCTURA BRONZE ===")
    logger.info(f"Columnas esperadas: {len(expected_columns)}")
    logger.info(f"Columnas actuales: {len(df_valid.columns)}")

    missing_columns = set(expected_columns) - set(df_valid.columns)
    extra_columns = set(df_valid.columns) - set(expected_columns)

    if missing_columns:
        logger.warning(f"Columnas faltantes: {missing_columns}")
    if extra_columns:
        logger.warning(f"Columnas extras: {extra_columns}")

    # Estadísticas de calidad
    logger.info("=== CALIDAD DE DATOS ===")
    logger.info(f"Confidence promedio: {df_valid['confidence'].mean():.2f}")
    logger.info(f"FRP promedio: {df_valid['frp'].mean():.2f}")
    logger.info(f"Data Quality Score promedio: {df_valid['data_quality_score'].mean():.2f}")

    # Distribución geográfica
    logger.info("=== DISTRIBUCIÓN GEOGRÁFICA ===")
    logger.info(f"Latitud rango: {df_valid['latitude'].min():.2f} to {df_valid['latitude'].max():.2f}")
    logger.info(f"Longitud rango: {df_valid['longitude'].min():.2f} to {df_valid['longitude'].max():.2f}")

    # Verificar que todos los datos estén en Argentina
    in_argentina = (
        (df_valid['latitude'] >= -55) & (df_valid['latitude'] <= -21) &
        (df_valid['longitude'] >= -73) & (df_valid['longitude'] <= -53)
    ).all()

    logger.info(f"Todos los datos en Argentina: {in_argentina}")

    logger.info("=== PRUEBA COMPLETADA ===")
    return len(df_valid), len(df_rejected)

if __name__ == "__main__":
    logger.info("Iniciando prueba de implementación Bronze...")
    valid_count, rejected_count = test_bronze_functions()
    logger.info(f"✅ Bronze layer listo: {valid_count} registros válidos, {rejected_count} rechazados")
