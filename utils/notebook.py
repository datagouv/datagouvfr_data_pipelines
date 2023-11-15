import codecs
import os
import nbformat
import papermill as pm
from minio import Minio
from nbconvert import HTMLExporter


def execute_and_upload_notebook(
    ti,
    input_nb,
    output_nb,
    tmp_path,
    minio_url,
    minio_user,
    minio_password,
    minio_bucket,
    minio_output_filepath,
    parameters,
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

    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )

    # check if bucket exists.
    found = client.bucket_exists(minio_bucket)
    if found:
        client.fput_object(
            minio_bucket,
            minio_output_filepath + output_report.split("/")[-1],
            output_report,
            content_type="text/html; charset=utf-8",
            metadata={"Content-Disposition": "inline"},
        )

        for path, subdirs, files in os.walk(tmp_path + "output/"):
            for name in files:
                print(os.path.join(path, name))
                isFile = os.path.isfile(os.path.join(path, name))
                if isFile:
                    client.fput_object(
                        minio_bucket,
                        minio_output_filepath
                        + os.path.join(path, name).replace(tmp_path, ""),
                        os.path.join(path, name),
                    )

    report_url = "https://{}/{}/{}".format(
        minio_url,
        minio_bucket,
        minio_output_filepath + output_report.split("/")[-1],
    )
    ti.xcom_push(key="report_url", value=report_url)
