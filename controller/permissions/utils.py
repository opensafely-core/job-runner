from controller import config


def build_analysis_scope(analysis_scope, project):
    # In future we expect all analysis_scope to be be passed from the RAP API
    # However, currently permissions for event level data access live in this repo,
    # so we combine info from both sources (sorted for reproducibility and readability in tracing)
    analysis_scope = analysis_scope or {}
    analysis_scope["dataset_permissions"] = sorted(
        analysis_scope.get("dataset_permissions", [])
    )
    if project in config.PROJECTS_WITH_EHRQL_EVENT_LEVEL_ACCESS:
        component_access = set(analysis_scope.get("component_access", [])) | {
            "event_level_data"
        }
        analysis_scope["component_access"] = sorted(component_access)
    return analysis_scope
