"""Valve가 사내 ``bigdataquery.getData``를 호출하는 실 API 어댑터."""
from __future__ import annotations


def query(params: dict, custom_col: list, user: str):
    # 배포 환경에만 있는 사내 패키지는 쿼리 시점에 로드한다. 패키지 누락이나
    # 인증/권한 오류는 LakeAPI의 재시도 및 실행 로그에 원문 그대로 기록된다.
    from bigdataquery import getData

    # Valve 구버전은 테이블을 ``table`` 키로 만들었지만 사내 getData 규약은
    # ``table_name``을 필수로 요구한다. 모든 호출 경로(scanner 포함)를 여기서도
    # 한 번 정규화해 설정이 저장돼 있는데도 "table_name must be included"가
    # 발생하지 않게 한다.
    params = dict(params or {})
    table_name = params.get("table_name") or params.pop("table", None)
    if table_name:
        params["table_name"] = table_name

    # RAW는 projection 없이 전체 컬럼을 받고, EVENT에서 필요한 컬럼을 고른다.
    return getData(params, user_name=user)
