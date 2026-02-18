import codecs
import os

import nbformat
import papermill as pm
from airflow.decorators import task
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from nbconvert import HTMLExporter


@task()
def execute_and_upload_notebook(
    input_nb,
    output_nb,
    tmp_path,
    s3_bucket,
    s3_output_filepath,
    parameters,
    **context,
):
    if not input_nb:
        raise ValueError("Input notebook is not specified")
    if not output_nb:
        raise ValueError("Output notebook is not specified")

    os.makedirs(os.path.dirname(tmp_path + "output/"), exist_ok=True)

    pm.execute_notebook(
        input_nb,
        tmp_path + output_nb,
        parameters=parameters,
        progress_bar=False,
        report_mode=True,
    )

    exporter = HTMLExporter()
    # read_file is '.ipynb', output_report is '.html'
    output_report = os.path.splitext(tmp_path + output_nb)[0] + ".html"
    output_notebook = nbformat.read(tmp_path + output_nb, as_version=4)
    output, resources = exporter.from_notebook_node(output_notebook)
    codecs.open(output_report, "w", encoding="utf-8").write(output)

    s3_client = S3Client(bucket=s3_bucket)

    s3_client.send_file(
        File(
            source_path=tmp_path,
            source_name=output_nb.replace("ipynb", "html"),
            dest_path=s3_output_filepath,
            dest_name=output_report.split("/")[-1],
            content_type="text/html; charset=utf-8",
        ),
        ignore_airflow_env=True,
    )

    for path, subdirs, files in os.walk(tmp_path + "output/"):
        for name in files:
            print(os.path.join(path, name))
            isFile = os.path.isfile(os.path.join(path, name))
            if isFile:
                s3_file_path = s3_output_filepath + os.path.join(path, name).replace(
                    tmp_path, ""
                )
                s3_client.send_file(
                    File(
                        source_path=path,
                        source_name=name,
                        dest_path="/".join(s3_file_path.split("/")[:-1]),
                        dest_name=s3_file_path.split("/")[-1],
                        content_type="text/html; charset=utf-8",
                    ),
                    ignore_airflow_env=True,
                )

    context["ti"].xcom_push(
        key="report_url",
        value=s3_client.get_file_url(s3_output_filepath + output_report.split("/")[-1]),
    )
