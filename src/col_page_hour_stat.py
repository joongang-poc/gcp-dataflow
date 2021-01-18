#-*- coding: utf-8 -*-
import apache_beam as beam
import csv
import json
import os
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import re



def preProcessing(fields):

    fields = fields.replace("$","")

    json_data = json.loads(fields)

    if "_id" not in json_data : #
        json_data["_id"] = {}
        if "oid" not in json_data["_id"] :
            json_data["_id"]["oid"] = None

    if "device" not in json_data :
        json_data["device"] = None

    if "referer_detail" not in json_data :
        json_data["referer_detail"] = None

    if "referer_group" not in json_data :
        json_data["referer_group"] = None

    if "regdate" not in json_data:
        json_data["regdate"] = {}
        if "date" not in json_data["regdate"]:
            json_data["regdate"]["date"] = None

    if "referer_super" not in json_data:
        json_data["referer_super"] = None


    if "urlpath" not in json_data:
        json_data["urlpath"] = None

    if "uv" not in json_data:
        json_data["uv"] = None

    if "pv" not in json_data:
        json_data["pv"] = None

    if "ontime" not in json_data:
        json_data["ontime"] = None

    if "read" not in json_data:
        json_data["read"] = None

    if "section_name" not in json_data:
        json_data["section_name"] = None

    if "bs" not in json_data:
        json_data["bs"] = None

    yield json_data

class DataTransformation:
     def __init__(self):
         dir_path = os.path.abspath(os.path.dirname(__file__))
         self.schema_str=""
         schema_file = os.path.join(dir_path, "resource", "[SCHEMA_FILE]")
         with open(schema_file) \
                       as f:
              data = f.read()
              self.schema_str = '{"fields": ' + data + '}'

class PTransform(beam.DoFn) :
    def __init__(self, default=""):
        self.default = default

    def process(self, fields):
        totalid_field_regex = None
        if len(re.findall('\d+', fields["urlpath"])) == 0:
            totalid_field_regex = None

        if fields["urlpath"].find("ilab::") != -1:
            totalid_field_regex = None

        if fields["urlpath"].find("ilab::") == -1 and len(re.findall('\d+', fields["urlpath"])) >= 1:

            totalid_field = re.findall('\d+', fields["urlpath"])
            visit = {}

            for i in range(len(totalid_field)) :
                sub_totalid_field = re.findall('\d+',fields["urlpath"])[i]
                if sub_totalid_field in visit :
                    continue
                isBoolean = False
                for j in re.finditer(sub_totalid_field,fields["urlpath"]) :
                    beforeIdx = j.start()-1
                    if beforeIdx < 0 :
                        continue
                    if fields["urlpath"][beforeIdx] == '/' :
                        totalid_field_regex = sub_totalid_field
                        isBoolean = True
                        break
                if isBoolean:
                    break
                visit[sub_totalid_field] = True


        regdate_variable = None
        if fields["regdate"]["date"] is not None:
            regdate_variable = fields["regdate"]["date"][0:19]
        tablerow = {
                "totalid" : totalid_field_regex,
                "device" : fields["device"],
                "referer_detail": fields["referer_detail"],
                "referer_group": fields["referer_group"],
                "regdate": regdate_variable,
                "referer_super": fields["referer_super"],
                "urlpath": fields["urlpath"],
                "uv": fields["uv"],
                "pv": fields["pv"],
                "ontime": fields["ontime"],
                "read": fields["read"],
                "section_name": fields["section_name"],
                "bs": fields["bs"],
        }
        yield tablerow

def run(project, bucket, dataset) :
        argv = [
            "--project={0}".format(project),
            "--job_name=[JOB_NAME]",
            "--save_main_session",
            "--region=asia-northeast1",
            "--staging_location=gs://{0}/[BUCKET_NAME]/".format(bucket),
            "--temp_location=gs://{0}/[BUCKET_NAME]/".format(bucket),
            "--max_num_workers=8",
            "--autoscaling_algorithm=THROUGHPUT_BASED",
            "--worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd",
            "--runner=DataflowRunner",
            "--worker_region=asia-northeast3"
        ]

        events_output = "{}:{}.[DATASET_NAME]".format(project, dataset)
        filename = "gs://{}/[FILE_NAME]".format(bucket)
        pipeline = beam.Pipeline(argv=argv)
        ptransform = (pipeline
                      | "Read from GCS" >> beam.io.ReadFromText(filename)
                      | "Pre Processing" >> beam.FlatMap(preProcessing)
                      | "PTransform" >> beam.ParDo(PTransform())
                      )


        data_ingestion = DataTransformation()
        schema = parse_table_schema_from_json(data_ingestion.schema_str)

        (ptransform
        | "events:out" >> beam.io.Write(
                    beam.io.BigQuerySink(
                    events_output, schema=schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            ))


        pipeline.run()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run pipeline on the cloud")
    parser.add_argument("--project", dest="project", help="Unique project ID", required=True)
    parser.add_argument("--bucket", dest="bucket", help="Bucket where your data were ingested", required=True)
    parser.add_argument("--dataset", dest="dataset", help="BigQuery dataset")


    args = vars(parser.parse_args())

    print("Correcting timestamps and writing to BigQuery dataset {}".format(args["dataset"]))

    run(project=args["project"], bucket=args["bucket"], dataset=args["dataset"])
