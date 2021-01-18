#-*- coding: utf-8 -*-
import apache_beam as beam
import csv
import json
import os
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


def preProcessing(fields):
    fields = fields.replace("$","")

    json_data = json.loads(fields)

    if "press_date" not in json_data :
        json_data["press_date"] = {}
        if "date" not in json_data["press_date"] :
            json_data["press_date"]["date"] = None
    if "totalid" not in json_data :
        json_data["totalid"] = None
    if "section" not in json_data or json_data["section"] is None :
        json_data["section"] = []
        section_dict = {}
        json_data["section"].append(section_dict)

    if "article_title" not in json_data :
        json_data["article_title"] = None
    if "urlpath" not in json_data :
        json_data["urlpath"] = None
    if "jamid" not in json_data :
        json_data["jamid"] = None
    if "box_article_count" not in json_data:
        json_data["box_article_count"] = None
    if "image_cnt" not in json_data:
        json_data["image_cnt"] = None
    if "sns_cnt" not in json_data:
        json_data["sns_cnt"] = None
    if "vod_cnt" not in json_data:
        json_data["vod_cnt"] = None
    if "article_type" not in json_data:
        json_data["article_type"] = None
    if "keyword" not in json_data:
        json_data["keyword"] = None
    if "origin_article_id" not in json_data:
        json_data["origin_article_id"] = None
    if "release_department" not in json_data:
        json_data["release_department"] = None

    if "source_code" not in json_data:
        json_data["source_code"] = None
    if "source_name" not in json_data:
        json_data["source_name"] = None
    if "bulk_site" not in json_data:
        json_data["bulk_site"] = None
    if "article_flag" not in json_data:
        json_data["article_flag"] = None
    if "text_cnt" not in json_data:
        json_data["text_cnt"] = None
    if "create_date" not in json_data :
        json_data["create_date"] = {}
        if "date" not in json_data["create_date"] :
            json_data["create_date"]["date"] = None

    if "_id" not in json_data :
        json_data["_id"] = {}
        if "oid" not in json_data["_id"] :
            json_data["_id"]["oid"] = None

    if "service_date" not in json_data:
        json_data["service_date"] = {}
        if "date" not in json_data["service_date"]:
            json_data["service_date"]["date"] = None

    if "reporter" not in json_data:
        json_data["reporter"] = {}
        if "reporter_seq" not in json_data["reporter"]:
            json_data["reporter"]["reporter_seq"] = None
        if "reporter_name" not in json_data["reporter"]:
            json_data["reporter"]["reporter_name"] = None
        if "department_name" not in json_data["reporter"]:
            json_data["reporter"]["department_name"] = None

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

        com = []
        com2 = {}
        com3 = ""
        if type(fields["reporter"]) == type(com):
            if len(fields["reporter"])==0 :
                reporter_seq = None
                reporter_name = None
                department_name = None
            else :
                reporter_seq = fields["reporter"][0]["reporter_seq"]
                reporter_name = fields["reporter"][0]["reporter_name"]
                department_name = fields["reporter"][0]["department_name"]
        if type(fields["reporter"]) == type(com2):
            reporter_seq = None
            reporter_name = None
            department_name = None

        section_cat1 = []
        section_cat2 = []
        section_cat3 = []

        if type(fields["section"] == type(com)) :
            if type(fields["section"][0]) == type(com3) :
                group = int(len(fields["section"])/3)
                rest = len(fields["section"])%3
                if rest != 0 :
                    group += 1
                real_for = len(fields["section"])
                for i in range(real_for) :
                    section_cat1.append(fields["section"][i])
            if type(fields["section"][0]) == type(com2) :
                for i in range(len(fields["section"])) :
                    if 'cat1' in fields["section"][i] :
                        section_cat1.append(fields["section"][i]['cat1'])
                    if 'cat2' in fields["section"][i] :
                        section_cat2.append(fields["section"][i]['cat2'])
                    if 'cat3' in fields["section"][i] :
                        section_cat3.append(fields["section"][i]['cat3'])

        if len(section_cat1) == 0 :
            section_cat1 = None
        if len(section_cat2) == 0 :
            section_cat2 = None
        if len(section_cat3) == 0 :
            section_cat3 = None

        create_date_variable = None
        if fields["create_date"]["date"] is not None :
            create_date_variable = fields["create_date"]["date"][0:19]
        service_date_variable = None
        if fields["service_date"]["date"] is not None :
            service_date_variable = fields["service_date"]["date"][0:19]
        press_date_variable = None
        if fields["press_date"]["date"] is not None :
            press_date_variable = fields["press_date"]["date"][0:19]

        tablerow = {
                "reporter" : {
                  "reporter_seq" : reporter_seq,
                  "reporter_name": reporter_name,
                  "department_name": department_name
                },
                "totalid" : fields["totalid"],
                "article_title" : fields["article_title"],
                "urlpath" : fields["urlpath"],
                "jamid": fields["jamid"],
                "bulk_site": fields["bulk_site"],
                "article_flag": fields["article_flag"],
                "text_cnt": fields["text_cnt"],
                "create_date": create_date_variable,
                "box_article_count": fields["box_article_count"],
                "image_cnt": fields["image_cnt"],
                "sns_cnt": fields["sns_cnt"],
                "vod_cnt": fields["vod_cnt"],
                "article_type": fields["article_type"],
                "keyword": fields["keyword"],
                "release_department": fields["release_department"],
                "origin_article_id": fields["origin_article_id"],
                "service_date" : service_date_variable,
                "section" : {
                    "cat1": section_cat1,
                    "cat2" : section_cat2,
                    "cat3" : section_cat3
                },
                "source_code" : fields["source_code"],
                "source_name" : fields["source_name"],
                "press_date" : press_date_variable
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
                   write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
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
