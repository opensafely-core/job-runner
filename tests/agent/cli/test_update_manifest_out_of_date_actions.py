from agent.cli import update_manifest_out_of_date_actions
from agent.executors.local import (
    get_medium_privacy_workspace,
    read_manifest_file,
    write_manifest_file,
)


def test_update_manifest_out_of_date_actions(test_repo):

    # Write manifest data for outputs from previous runs
    level4_dir = get_medium_privacy_workspace("test_workspace")
    write_manifest_file(
        level4_dir,
        {
            "outputs": {
                # previous output from a different action which is no longer in the project.yaml
                "output/old_action.txt": {"action": "old_action"},
                # previous output from a different action which IS in the project.yaml (see fixtures/full_project)
                "output/dataset.csv": {"action": "generate_dataset"},
                # previous output from a different action which was incorrectly marked out of date
                "output/extra/dataset.csv": {
                    "action": "generate_dataset_with_dummy_data",
                    "out_of_date_action": True,
                },
            }
        },
    )

    update_manifest_out_of_date_actions.run(
        [
            "test_workspace",
            test_repo.repo_url,
            test_repo.commit,
        ]
    )

    manifest = read_manifest_file(level4_dir, "test_workspace")
    assert manifest["outputs"] == {
        "output/old_action.txt": {"action": "old_action", "out_of_date_action": True},
        "output/dataset.csv": {
            "action": "generate_dataset",
            "out_of_date_action": False,
        },
        "output/extra/dataset.csv": {
            "action": "generate_dataset_with_dummy_data",
            "out_of_date_action": False,
        },
    }
