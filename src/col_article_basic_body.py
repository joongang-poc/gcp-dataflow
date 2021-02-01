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
        section_dict["cat1"]=None
        section_dict["cat2"]=None
        section_dict["cat3"]=None
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
    if "article_body" not in json_data :
        json_data["article_body"] = None
    if "first_img_src" not in json_data :
        json_data["first_img_src"] = None
    if "embed_youtube" not in json_data :
        json_data["embed_youtube"] = None
    if "embed_ooyala" not in json_data :
        json_data["embed_ooyala"] = None
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
        recu_sect = []
        if type(fields["section"] == type(com)) :
            if type(fields["section"][0]) == type(com3) :
                sub_recu_cat = {}
                real_for = len(fields["section"])
                for i in range(real_for) :
                    sub_recu_cat["cat1"] = fields["section"][i]
                    sub_recu_cat["cat2"] = None
                    sub_recu_cat["cat3"] = None
                    recu_sect.append(sub_recu_cat)
            if type(fields["section"][0]) == type(com2) :
                sub_recu_cat = {}
                for i in range(len(fields["section"])) :
                    if 'cat1' in fields["section"][i] :
                        sub_recu_cat["cat1"] = fields["section"][i]["cat1"]
                    if 'cat2' in fields["section"][i] :
                        sub_recu_cat["cat2"] = fields["section"][i]["cat2"]
                    if 'cat3' in fields["section"][i] :
                        sub_recu_cat["cat3"] = fields["section"][i]["cat3"]
                    recu_sect.append(sub_recu_cat)
        embed_ovpl = []
        if fields["embed_ooyala"] is not None :
            embed_ovpl = fields["embed_ooyala"].split("@@")
        embed_youtubel = []
        if fields["embed_youtube"] is not None :
            embed_youtubel = fields["embed_youtube"].split("@@")
        create_date_variable = None
        if fields["create_date"]["date"] is not None :
            create_date_variable = fields["create_date"]["date"][0:19]
        service_date_variable = None
        if fields["service_date"]["date"] is not None :
            service_date_variable = fields["service_date"]["date"][0:19]
        press_date_variable = None
        if fields["press_date"]["date"] is not None :
            press_date_variable = fields["press_date"]["date"][0:19]
        tablerow = {}
        sub_reporter_dict = {}
        sub_reporter_dict["reporter_seq"] = reporter_seq
        sub_reporter_dict["reporter_name"] = reporter_name
        sub_reporter_dict["department_name"] = department_name
        tablerow["reporter"] = sub_reporter_dict
        tablerow["totalid"] = fields["totalid"]
        tablerow["article_title"] = fields["article_title"]
        tablerow["urlpath"] = fields["urlpath"]
        tablerow["jamid"] = fields["jamid"]
        tablerow["bulk_site"] = fields["bulk_site"]
        tablerow["article_flag"] = fields["article_flag"]
        tablerow["text_cnt"] = fields["text_cnt"]
        tablerow["create_date"] = create_date_variable
        tablerow["box_article_count"] = fields["box_article_count"]
        tablerow["image_cnt"] = fields["image_cnt"]
        tablerow["sns_cnt"] = fields["sns_cnt"]
        tablerow["vod_cnt"] = fields["vod_cnt"]
        tablerow["article_type"] = fields["article_type"]
        tablerow["keyword"] = fields["keyword"]
        tablerow["release_department"] = fields["release_department"]
        tablerow["origin_article_id"] = fields["origin_article_id"]
        tablerow["service_date"] = service_date_variable
        tablerow["section"] = recu_sect
        tablerow["source_code"] = fields["source_code"]
        tablerow["source_name"] = fields["source_name"]
        tablerow["press_date"] = press_date_variable
        tablerow["article_body"] = fields["article_body"]
        tablerow["first_img_src"] = fields["first_img_src"]
        tablerow["embed_youtube"] = embed_youtubel
        tablerow["embed_ovp"] = embed_ovpl
        yield tablerow
def run(project, bucket, dataset) :
        argv = [
            "--project={0}".format(project),
            "--job_name=col-article-basic-body",
            "--save_main_session",
            "--region=asia-northeast1",
            "--staging_location=gs://{0}/staging/".format(bucket),
            "--temp_location=gs://{0}/temp-location/".format(bucket),
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
                      | "Pre-Processing" >> beam.FlatMap(preProcessing)
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

