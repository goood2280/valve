def test_history_exposes_severity_and_filters(app_client):
    response = app_client.get("/api/jobs/history?kind=all&limit=100")
    assert response.status_code == 200
    data = response.json()
    assert set(data["severity_counts"]) == {"info", "warning", "critical"}
    assert all(item.get("severity") in {"info", "warning", "critical"}
               for item in data["items"])

    highlighted = app_client.get(
        "/api/jobs/history?kind=all&min_severity=warning&limit=100"
    )
    assert highlighted.status_code == 200
    assert all(item["severity"] in {"warning", "critical"}
               for item in highlighted.json()["items"])


def test_pipeline_retry_status_endpoint(app_client):
    response = app_client.get("/api/pipeline/retries")
    assert response.status_code == 200
    assert {"pending", "due", "oldest_age_sec", "next_retry_at", "max_attempts", "items"} \
        <= set(response.json())
