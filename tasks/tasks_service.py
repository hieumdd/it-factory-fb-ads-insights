from facebook.pipeline import pipelines
from tasks import cloud_tasks

ACCOUNTS = [
    "",
]


def tasks_service(body: dict[str, str]):
    return {
        "tasks": cloud_tasks.create_tasks(
            [
                {
                    "table": i,
                    "ads_account_id": a,
                    "start": body.get("start"),
                    "end": body.get("end"),
                }
                for i in pipelines.keys()
                for a in i
            ],
            lambda x: x["table"],
        )
    }
