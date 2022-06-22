import json
from datetime import datetime
from pathlib import Path
from os.path import join

from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.who_scored_hook import WhoScoredHook


class WhoScoredOperator(BaseOperator):

    template_fields = [
        'file_path',
    ]

    @apply_defaults
    def __init__(
        self,
        file_path,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_path = file_path

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(
            parents = True,
            exist_ok = True
        )

    def execute(self, context):
        hook = WhoScoredHook(
            execution_date = context['prev_ds']
        )

        self.create_parent_folder()
        games = hook.run()

        with open(self.file_path, 'w') as output_file:
            json.dump(games, output_file, ensure_ascii=False)
            output_file.write("\n")

if __name__ == '__main__':
    with DAG(dag_id='ApiFootballTest', start_date=datetime.now()) as dag:
        to = WhoScoredOperator(
            file_path=join(
                '/home/lucas/pipeline-data/datalake',
                'who_scored',
                'extract_date={{ ds }}',
                'WhoScored_{{ ds_nodash }}.json'
                ),
            task_id='test_run'
        )
        ti = TaskInstance(task=to, execution_date=datetime.now())
        ti.run()