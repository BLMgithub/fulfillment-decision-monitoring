# =============================================================================
# VALIDATE RAW FULFILLMENT DATA
# =============================================================================
# - Validate raw order and delivery data integrity
# - Enforce schema, timestamp, and key validity
# - Flag issues that would break downstream aggregation


import os
import sys
from typing import Dict, List
import pandas as pd


# ------------------------------------------------------------
# CONFIGURATIONS
# ------------------------------------------------------------

RAW_DATA_BASE_PATH = 'data/raw'
PARTITIONS = ['train', 'test']

TABLE_CONFIG = {
    'df_Orders': {
        'role': 'event_fact',
        'primary_key': ['order_id'],
    },
    'df_OrderItems': {
        'role': 'transaction_detail',
        'primary_key': ['order_id', 'order_item_id'],
    },
    'df_Customers': {
        'role': 'entity_reference',
        'primary_key': ['customer_id'],
    },
    'df_payments': {
        'role': 'transaction_detail',
        'primary_key': ['order_id', 'payment_sequential'],
    },
    'df_products': {
        'role': 'entity_reference',
        'primary_key': ['product_id'],
    },
}


# ------------------------------------------------------------
# VALIDATION REPORT & LOGS
# ------------------------------------------------------------

def init_report() -> Dict[str, List[str]]:
    return {
        'errors': [],
        'warnings': [],
        'info': [],
    }


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f'[INFO] {message}')
    report['info'].append(message)


def log_warning(message: str, report: Dict[str, List[str]]) -> None:
    print(f'[WARNING] {message}')
    report['warnings'].append(message)


def log_error(message: str, report: Dict[str, List[str]]) -> None:
    print(f'[ERROR] {message}')
    report['errors'].append(message)


# ------------------------------------------------------------
# BASE VALIDATIONS
# ------------------------------------------------------------

def run_base_validations(df: pd.DataFrame,
                         table_name: str,
                         primary_key: List[str],
                         report: Dict[str, List[str]]
                         ) -> None:
    """
    Base structural validations applied to ALL tables.
    
    Stops if dataset is empty.
    """
    # Dataset not empty
    if df.empty:
        log_error(f'{table_name}: dataset is empty', report)

        # Stops Checking: If data is empty
        return
    
    # Column names unique
    duplicate_column = df.columns[df.columns.duplicated().tolist()]
    if duplicate_column:
        log_error(f'{table_name}: duplicate column names detected: {duplicate_column}', report)
    
    # Primary key columns exist
    missing_pk_columns = [col for col in primary_key if col not in df.columns]
    if missing_pk_columns:
        log_error(f'{table_name}: missing primary columns(s): {missing_pk_columns}', report)
    
    # Primary key nulls
    pk_nulls_count = df[primary_key].isnull().any(axis=1).sum()
    if pk_nulls_count > 0:
        log_error(f'{table_name}: {pk_nulls_count} row(s)will null primary key values', report)

    # Primary key uniqueness
    duplicate_pk_count = df.duplicated(subset= primary_key).sum()
    if duplicate_pk_count > 0:
        log_error(f'{table_name}: {duplicate_pk_count} duplicated primary key value(s)', report)

    # Dataset info
    log_info(f'{table_name}: base validation completed '
             f'({df.shape[0]} rows, {df.shape[1]} columns)',
             report)
    
    pass


# ------------------------------------------------------------
# ROLE SPECIFIC VALIDATION
# ------------------------------------------------------------

def run_event_fact_validations(df: pd.DataFrame,
                               table_name: str,
                               report: Dict[str, List[str]]
                               ) -> None:
    """
    Event fact validations.

    Stops if missing timestamp(s) detected.
    """

    # Required timestamp fields
    required_timestamps = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_timestamp',
        'order_estimated_delivery_date'
    ]

    missing_ts_columns = [col for col in required_timestamps if col not in df.columns]
    if missing_ts_columns:
        log_error(f'{table_name}: missing required timestamp column(s): {missing_ts_columns}', report)
        
        # Stops Checking: If missing timestamp
        return
    

    # Timestamp parsing
    parsed_timestamps = {}

    for col in required_timestamps:
        parsed = pd.to_datetime(df[col], errors= 'coerce')
        parsed_timestamps[col] = parsed

        invalid_count = parsed.isna().sum()
        if invalid_count > 0:
            log_error(f'{table_name}: {invalid_count} unparsable timestamp value(s) in `{col}`', report)
    
    # Temporal ordering check
    purchase_ts = parsed_timestamps['order_purchase_timestamp']
    approved_ts = parsed_timestamps['order_approved_at']
    delivered_ts = parsed_timestamps['order_delivered_timestamp']

    # Approval before Purchase
    invalid_approval = (approved_ts < purchase_ts).sum()
    if invalid_approval > 0:
        log_error(f'{table_name}: {invalid_approval} record(s) where approval precedes purchase', report)

    # Delivery before Purchase
    invalid_delivery = (delivered_ts < purchase_ts).sum()
    if invalid_delivery > 0:
        log_error(f'{table_name}: {invalid_delivery} record(s) where delivery precedes purchase', report)
    
    # Date ranges info
    log_info(f'{table_name}: purchase date range'
             f'{purchase_ts.min()} - {purchase_ts.max()}',
             report)
    
    log_info(f'{table_name}: delivery date range'
             f'{delivered_ts.min()} - {delivered_ts.max()}',
             report)
    



# ------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------


# =============================================================================
# END OF SCRIPT
# =============================================================================