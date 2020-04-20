import requests
from pathlib import Path
import boto3
from os import listdir
from os.path import isfile, join
from shutil import copyfile
import xml.etree.ElementTree
import random

s3 = boto3.client('s3', region_name='us-west-2')
bucket = 'wingwarp-annotated-data'
dataset_names = ["04-19-2020_h78sf9"]
def get_indices(ds_name):
	response = s3.list_objects_v2(
	            Bucket=bucket,
	            Prefix ='basketball/%s/voc' % ds_name)
	return [r["Key"].split("/")[-1].split(".xml")[0] for r in response["Contents"]]
urls = ["https://wingwarp-annotated-data.s3-us-west-2.amazonaws.com/basketball/%s/%s/%s.xml",
		"https://wingwarp-annotated-data.s3-us-west-2.amazonaws.com/basketball/%s/%s/%s.png"]

def download_files(dataset_names, urls):
	for idx, ds_name in enumerate(dataset_names):
		Path("./annotations/%s" % ds_name).mkdir(parents=True, exist_ok=True)
		Path("./images/%s" % ds_name).mkdir(parents=True, exist_ok=True)
		indices = get_indices(ds_name)
		for x in indices:
			url = urls[0] % (ds_name, "voc", x)
			res = requests.get(url)
			if res.status_code == 200:
				fname = url.split("/")[-1]
				xml_path = "annotations/%s/%s"  % (ds_name, fname)
				with open(xml_path, "w") as f:
					f.write(res.text)
				et = xml.etree.ElementTree.parse(xml_path)
				root = et.getroot()
				for obj_el in root.findall('object'):
					print("dsafadsf", obj_el.find("name"))
					name_el = obj_el.find("name")
					if name_el.text == "basketball":
						name_el.text = "ball"
						et.write(xml_path)

				print("annot %s done" % x)

			url = urls[1] % (ds_name, "pngs", x)
			res = requests.get(url)
			if res.status_code == 200:
				fname = url.split("/")[-1]
				with open("images/%s/%s" % (ds_name, fname), "wb") as f:
					f.write(res.content)
				
				print("img %s done" % x)

def organize_training_data(dataset_names):
	final_images_path = "./images/final"
	final_annot_path = "./annotations/final"
	Path(final_annot_path).mkdir(parents=True, exist_ok=True)
	Path(final_images_path).mkdir(parents=True, exist_ok=True)
	all_names = set()
	for ds_name in dataset_names:
		adjusted_names = set()
		ds_annotations_path = "./annotations/%s" % ds_name
		annotation_file_names = set([f for f in listdir(ds_annotations_path) if isfile(join(ds_annotations_path, f))])
		print(annotation_file_names)
		already_exist = all_names.intersection(set(annotation_file_names))
		new_names = annotation_file_names - already_exist
		for xml_name in new_names:
			png_name = xml_name.replace("xml", "png")
			xml_path = join(ds_annotations_path, xml_name)
			png_path = join("./images/%s" % ds_name, png_name)
			png_final_path = join(final_images_path, png_name)
			xml_final_path = join(final_annot_path, xml_name)
			copyfile(png_path, png_final_path)
			copyfile(xml_path, xml_final_path)

		for xml_name in already_exist:
			print(xml_name)
			xml_path = join(ds_annotations_path, xml_name)
			png_name = xml_name.replace("xml", "png")
			png_path = join("./images/%s" % ds_name, png_name)
			found = False
			new_xml_name = None
			while not found:
				new_xml_name = "%d.xml" % random.randint(int(10**5), int(10**8))
				if new_xml_name not in annotation_file_names:
					found = True

			adjusted_names.add(new_xml_name)
			new_png_name = new_xml_name.replace("xml", "png")
			et = xml.etree.ElementTree.parse(xml_path)
			root = et.getroot()
			for fn_el in root.findall('filename'):
				fn_el.text = new_png_name
			png_final_path = join(final_images_path, new_png_name)
			xml_final_path = join(final_annot_path, new_xml_name)
			copyfile(png_path, png_final_path)
			et.write(xml_final_path)

		all_names = all_names.union(new_names)
		all_names = all_names.union(adjusted_names)


if __name__ == "__main__":
	download_files(dataset_names, urls)
	organize_training_data(dataset_names)




