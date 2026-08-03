"""Adapter from Valve's query contract to the internal ``bigdataquery`` API."""
from __future__ import annotations


def query(params: dict, custom_col: list, user: str):
    # The internal package exists only in the deployment environment, so import
    # it lazily. Authentication and permission errors are handled by LakeAPI.
    from bigdataquery import getData

    from backend.core.lake_api import normalize_query_params

    clean_params = normalize_query_params(params)
    columns = list(dict.fromkeys(
        str(column).strip() for column in (custom_col or []) if str(column).strip()
    ))
    return getData(clean_params, custom_columns=columns, user_name=user)
